package dalgo2dynamodb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func TestOpenQueryJourneyCallerPriorityWithoutIO(t *testing.T) {
	profile := &JourneyOptions{Authority: ReadAuthority{PrincipalScope: "p", Generation: "g", ResourceScope: "r"}}
	canceled := func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}
	expired := func() (context.Context, context.CancelFunc) {
		return context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	}
	modes := []struct {
		name       string
		closed     bool
		newContext func() (context.Context, context.CancelFunc)
		want       error
		code       string
	}{
		{"canceled", false, canceled, context.Canceled, ""},
		{"deadline", false, expired, context.DeadlineExceeded, ""},
		{"closed", true, func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }, nil, "closed"},
		{"closed-canceled", true, canceled, context.Canceled, ""},
		{"closed-deadline", true, expired, context.DeadlineExceeded, ""},
	}
	profiles := []struct {
		name    string
		amend   func(*Options)
		request QueryRequest
	}{
		{"profile-nil", nil, QueryRequest{}},
		{"legacy-cursor", func(o *Options) { o.Journeys = profile }, QueryRequest{Cursor: &Cursor{}}},
		{"normal-profile", func(o *Options) { o.Journeys = profile }, QueryRequest{}},
	}
	for _, p := range profiles {
		for _, m := range modes {
			t.Run(p.name+"/"+m.name, func(t *testing.T) {
				calls := 0
				c := newTestClient(t, &fakeAPI{
					get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
						calls++
						return nil, nil
					},
					query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
						calls++
						return nil, nil
					},
				}, p.amend)
				if m.closed {
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
				}
				ctx, stop := m.newContext()
				defer stop()
				cursor, err := c.OpenQueryJourney(ctx, p.request)
				if cursor != nil || calls != 0 {
					t.Fatalf("cursor=%v api-calls=%d", cursor, calls)
				}
				if m.want != nil {
					if err != m.want {
						t.Fatalf("error=%v, want exact %v", err, m.want)
					}
				} else {
					requireCode(t, err, m.code)
				}
			})
		}
	}
}

type afterFuncContext struct {
	parent   context.Context
	callback func()
}

func (c *afterFuncContext) Deadline() (time.Time, bool) { return c.parent.Deadline() }
func (c *afterFuncContext) Done() <-chan struct{}       { return c.parent.Done() }
func (c *afterFuncContext) Err() error                  { return c.parent.Err() }
func (c *afterFuncContext) Value(key any) any           { return nil }
func (c *afterFuncContext) AfterFunc(f func()) func() bool {
	stop := context.AfterFunc(c.parent, f)
	var once sync.Once
	return func() bool {
		ran := false
		once.Do(func() {
			ran = true
			c.callback()
		})
		stop()
		return ran
	}
}

func TestOpenQueryJourneyOwnedCleanupBeforeCommit(t *testing.T) {
	profile := &JourneyOptions{Authority: ReadAuthority{PrincipalScope: "p", Generation: "g", ResourceScope: "r"}}
	for _, mode := range []string{"caller", "client"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			c := newTestClient(t, &fakeAPI{
				get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					calls.Add(1)
					return nil, nil
				},
				query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
					calls.Add(1)
					return nil, nil
				},
			}, func(o *Options) { o.Journeys = profile })
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			var entered atomic.Bool
			wrapped := &afterFuncContext{parent: parent, callback: func() {
				entered.Store(true)
				if mode == "caller" {
					cancel()
				} else {
					_ = c.Close()
				}
			}}
			cursor, err := c.OpenQueryJourney(wrapped, QueryRequest{Collection: "items", Partition: s("one"), Limit: 1})
			if !entered.Load() {
				t.Fatal("owned cleanup did not enter caller stop callback")
			}
			if cursor != nil {
				t.Fatal("handle returned after owned cleanup invalidated admission")
			}
			if calls.Load() != 0 {
				t.Fatalf("API called %d times", calls.Load())
			}
			if mode == "caller" {
				if err != context.Canceled {
					t.Fatalf("error=%v, want exact %v", err, context.Canceled)
				}
			} else {
				requireCode(t, err, "closed")
			}
			reg := &c.registry
			reg.mu.Lock()
			open, bytes, nodes := len(reg.open), reg.bytes, reg.nodes
			reg.mu.Unlock()
			if open != 0 || bytes != 0 || nodes != 0 {
				t.Fatalf("leaked journey quota: open=%d bytes=%d nodes=%d", open, bytes, nodes)
			}
		})
	}
}

func TestOpenQueryJourneyFailedOpenFreshClassification(t *testing.T) {
	profile := &JourneyOptions{Authority: ReadAuthority{PrincipalScope: "p", Generation: "g", ResourceScope: "r"}}
	failures := []struct {
		name    string
		amend   func(*Options)
		request QueryRequest
	}{
		{"profile-nil", nil, QueryRequest{Collection: "items", Partition: s("one"), Limit: 1}},
		{"legacy-cursor", func(o *Options) { o.Journeys = profile }, QueryRequest{Collection: "items", Partition: s("one"), Limit: 1, Cursor: &Cursor{}}},
		{"compiler-failure", func(o *Options) { o.Journeys = profile }, QueryRequest{Collection: "items", Limit: 1}},
	}
	for _, f := range failures {
		for _, mode := range []string{"caller", "client"} {
			t.Run(f.name+"/"+mode, func(t *testing.T) {
				var calls atomic.Int32
				c := newTestClient(t, &fakeAPI{
					get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
						calls.Add(1)
						return nil, nil
					},
					query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
						calls.Add(1)
						return nil, nil
					},
				}, f.amend)
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				var entered atomic.Bool
				wrapped := &afterFuncContext{parent: parent, callback: func() {
					entered.Store(true)
					if mode == "caller" {
						cancel()
					} else {
						_ = c.Close()
					}
				}}
				cursor, err := c.OpenQueryJourney(wrapped, f.request)
				if !entered.Load() {
					t.Fatal("owned cleanup did not enter caller stop callback")
				}
				if cursor != nil {
					t.Fatal("handle returned after failed admission")
				}
				if calls.Load() != 0 {
					t.Fatalf("API called %d times", calls.Load())
				}
				if mode == "caller" {
					if err != context.Canceled {
						t.Fatalf("error=%v, want exact %v", err, context.Canceled)
					}
				} else {
					requireCode(t, err, "closed")
				}
				reg := &c.registry
				reg.mu.Lock()
				open, bytes, nodes := len(reg.open), reg.bytes, reg.nodes
				reg.mu.Unlock()
				if open != 0 || bytes != 0 || nodes != 0 {
					t.Fatalf("leaked journey quota: open=%d bytes=%d nodes=%d", open, bytes, nodes)
				}
			})
		}
	}
}

func TestInvokeRechecksAfterRetainError(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "active"} {
		t.Run(mode, func(t *testing.T) {
			var calls, acquired atomic.Int32
			c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
				calls.Add(1)
				return &dynamodb.GetItemOutput{Item: in.Key}, nil
			}}, func(o *Options) { o.MaxConcurrent = 1 })
			var ctx context.Context
			var cancel context.CancelFunc
			if mode == "cancel" {
				ctx, cancel = context.WithCancel(context.Background())
			} else if mode == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			o, err := c.begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer o.close()
			retainedErr := failure("busy")
			o.retain = func() (func(), error) {
				acquired.Add(1)
				switch mode {
				case "cancel":
					cancel()
				case "deadline":
					<-ctx.Done()
				}
				return nil, retainedErr
			}
			value, err := invoke(o, func(context.Context) (string, error) {
				calls.Add(1)
				return "dispatched", nil
			})
			if value != "" {
				t.Fatalf("value=%q returned after retain error", value)
			}
			if calls.Load() != 0 {
				t.Fatalf("SDK dispatched %d times after retain error", calls.Load())
			}
			switch mode {
			case "cancel":
				if err != context.Canceled {
					t.Fatalf("error=%v, want exact %v", err, context.Canceled)
				}
			case "deadline":
				if err != context.DeadlineExceeded {
					t.Fatalf("error=%v, want exact %v", err, context.DeadlineExceeded)
				}
			default:
				if err != retainedErr {
					t.Fatalf("error=%v, want retained %v", err, retainedErr)
				}
			}
			if acquired.Load() != 1 {
				t.Fatalf("acquired=%d", acquired.Load())
			}
			if len(c.semaphore) != 0 {
				t.Fatalf("semaphore slot leaked: %d", len(c.semaphore))
			}
			recoverCtx, recoverCancel := context.WithTimeout(context.Background(), time.Second)
			defer recoverCancel()
			if _, err := c.Get(recoverCtx, "items", pk("two")); err != nil {
				t.Fatalf("next admission did not recover: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("recovery dispatch count=%d", calls.Load())
			}
		})
	}
}

func TestInvokeRechecksAfterRetainBeforeDispatch(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			var calls, acquired, released atomic.Int32
			c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
				calls.Add(1)
				return &dynamodb.GetItemOutput{Item: in.Key}, nil
			}}, func(o *Options) { o.MaxConcurrent = 1 })
			var ctx context.Context
			var cancel context.CancelFunc
			if mode == "cancel" {
				ctx, cancel = context.WithCancel(context.Background())
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()
			o, err := c.begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer o.close()
			o.retain = func() (func(), error) {
				acquired.Add(1)
				if mode == "cancel" {
					cancel()
				} else {
					<-ctx.Done()
				}
				return func() { released.Add(1) }, nil
			}
			value, err := invoke(o, func(context.Context) (string, error) {
				calls.Add(1)
				return "dispatched", nil
			})
			if value != "" {
				t.Fatalf("value=%q returned after retain invalidation", value)
			}
			if calls.Load() != 0 {
				t.Fatalf("SDK dispatched %d times after retain invalidation", calls.Load())
			}
			if mode == "cancel" {
				if err != context.Canceled {
					t.Fatalf("error=%v, want exact %v", err, context.Canceled)
				}
			} else {
				if err != context.DeadlineExceeded {
					t.Fatalf("error=%v, want exact %v", err, context.DeadlineExceeded)
				}
			}
			if acquired.Load() != 1 || released.Load() != 1 {
				t.Fatalf("acquired=%d released=%d", acquired.Load(), released.Load())
			}
			if len(c.semaphore) != 0 {
				t.Fatalf("semaphore slot leaked: %d", len(c.semaphore))
			}
			recoverCtx, recoverCancel := context.WithTimeout(context.Background(), time.Second)
			defer recoverCancel()
			if _, err := c.Get(recoverCtx, "items", pk("two")); err != nil {
				t.Fatalf("next admission did not recover: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("recovery dispatch count=%d", calls.Load())
			}
		})
	}
}
