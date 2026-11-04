package dalgo2dynamodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type journeyLifecycleResult struct {
	page ReadPage
	err  error
}

func TestPublicQueryJourneyPrepareCancellationPriority(t *testing.T) {
	newJourney := func(t *testing.T, calls *atomic.Int32) (*Client, *ReadCursor) {
		t.Helper()
		c := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			calls.Add(1)
			return journeyPublicOutput([]Item{qitem("tenant", "1")}, 1, qitem("tenant", "2"), nil), nil
		}}, func(o *Options) {
			o.PageSize = 1
			o.Timeout = 2 * time.Second
			o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority()}
		})
		cursor, err := c.OpenQueryJourney(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 3})
		if err != nil {
			t.Fatal(err)
		}
		return c, cursor
	}

	for _, secret := range []bool{false, true} {
		name := "nil-error"
		if secret {
			name = "wrapped-secret-error"
		}
		t.Run(name, func(t *testing.T) {
			calls := &atomic.Int32{}
			c, cursor := newJourney(t, calls)
			ctx, cancel := context.WithCancelCause(context.Background())
			entered := false
			page, err := c.NextReadPageWith(ctx, cursor, func(ReadPage) error {
				entered = true
				cancel(errors.New("private cause secret"))
				if secret {
					return fmt.Errorf("wrapped secret: %w", errors.New("secret"))
				}
				return nil
			})
			if !entered {
				t.Fatal("prepare was not entered")
			}
			if !errors.Is(err, context.Canceled) || errors.Is(err, context.Cause(ctx)) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("cancellation was not classified exactly: %v", err)
			}
			if !reflect.DeepEqual(page, ReadPage{}) {
				t.Fatal("cancelled prepare leaked a page")
			}
			retry, err := c.NextReadPage(context.Background(), cursor)
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 || len(retry.Items) != 1 || !equalKeyValue(retry.Items[0]["sequence"], n("1")) {
				t.Fatal("retry did not reuse the retained native page")
			}
		})
	}

	t.Run("caller-cancel-priority-over-close", func(t *testing.T) {
		calls := &atomic.Int32{}
		c, cursor := newJourney(t, calls)
		ctx, cancel := context.WithCancelCause(context.Background())
		page, err := c.NextReadPageWith(ctx, cursor, func(ReadPage) error {
			cancel(errors.New("private cause secret"))
			if err := c.Close(); err != nil {
				t.Error(err)
			}
			return nil
		})
		if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "closed") {
			t.Fatalf("original caller cancellation lost priority: %v", err)
		}
		if !reflect.DeepEqual(page, ReadPage{}) {
			t.Fatal("cancelled prepare leaked a page")
		}
		before := calls.Load()
		if _, err := c.NextReadPage(context.Background(), cursor); err == nil || calls.Load() != before {
			t.Fatal("closed journey dispatched SDK work")
		}
	})
}

func TestPublicQueryJourneyBorrowedSDKIgnoresCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	callerFinished := make(chan struct{})
	var releaseOnce sync.Once
	var callerCancel context.CancelCauseFunc
	var callerStarted bool
	var calls atomic.Int32
	api := &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			close(returned)
			return nil, errors.New("late secret result")
		}
		return journeyPublicOutput([]Item{qitem("tenant", "2")}, 1, nil, nil), nil
	}}
	c := newTestClient(t, api, func(o *Options) {
		o.Timeout = 2 * time.Second
		o.MaxConcurrent = 1
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), MaxOpen: 1}
	})
	var cursor *ReadCursor
	request := QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 3}
	t.Cleanup(func() {
		if callerCancel != nil {
			callerCancel(errors.New("private cleanup cause"))
		}
		releaseOnce.Do(func() { close(release) })
		if callerStarted {
			select {
			case <-callerFinished:
			case <-time.After(time.Second):
				t.Error("caller goroutine did not finish during cleanup")
			}
		}
		select {
		case <-entered:
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Error("held SDK future did not finish during cleanup")
			}
		default:
		}
		if cursor == nil {
			return
		}
		if err := c.CloseReadJourney(cursor); err != nil {
			t.Error(err)
		}
		probeCtx, stopProbe := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopProbe()
		var recovered *ReadCursor
		for recovered == nil {
			if err := probeCtx.Err(); err != nil {
				t.Errorf("journey admission did not recover during cleanup: %v", err)
				break
			}
			probed, openErr := c.OpenQueryJourney(probeCtx, request)
			if openErr == nil {
				recovered = probed
				break
			}
			var typed *Error
			if !errors.As(openErr, &typed) || typed.Code != "limit" {
				t.Errorf("unexpected journey admission error during cleanup: %v", openErr)
				break
			}
			runtime.Gosched()
		}
		if recovered != nil {
			if err := c.CloseReadJourney(recovered); err != nil {
				t.Error(err)
			}
		}
	})
	cursor, err := c.OpenQueryJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	callerCancel = cancel
	done := make(chan journeyLifecycleResult, 1)
	callerStarted = true
	go func() {
		defer close(callerFinished)
		page, err := c.NextReadPage(ctx, cursor)
		done <- journeyLifecycleResult{page: page, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("SDK query did not start")
	}
	cancel(errors.New("private cause secret"))
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) || strings.Contains(result.err.Error(), "secret") || !reflect.DeepEqual(result.page, ReadPage{}) {
			t.Fatalf("caller cancellation was not exact: %v", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not return promptly")
	}
	select {
	case <-returned:
		t.Fatal("uncooperative SDK returned before release")
	default:
	}
	_, err = c.OpenQueryJourney(context.Background(), request)
	requireCode(t, err, "limit")
	if err := c.CloseReadJourney(cursor); err != nil {
		t.Fatal(err)
	}
	_, err = c.OpenQueryJourney(context.Background(), request)
	requireCode(t, err, "limit")
	before := calls.Load()
	_, err = c.NextReadPage(context.Background(), cursor)
	requireCode(t, err, "cursor")
	if calls.Load() != before {
		t.Fatal("closed journey refetched")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("released SDK did not return")
	}
	probe, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	var recovered *ReadCursor
	for recovered == nil {
		if err := probe.Err(); err != nil {
			t.Fatalf("journey admission did not recover: %v", err)
		}
		recovered, err = c.OpenQueryJourney(probe, request)
		if err == nil {
			break
		}
		requireCode(t, err, "limit")
		runtime.Gosched()
	}
	defer func() {
		if err := c.CloseReadJourney(recovered); err != nil {
			t.Error(err)
		}
	}()
	page, err := c.NextReadPage(context.Background(), recovered)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || page.Ordinal != 1 || page.Receipt.StopReason != ReadStopNativeEOF {
		t.Fatal("normal SDK query after release did not succeed")
	}
	if err := c.CloseReadJourney(recovered); err != nil {
		t.Fatal(err)
	}
}

func TestPublicQueryJourneyOriginalExpiryIsNotRenewed(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		switch calls.Add(1) {
		case 1:
			return journeyPublicOutput([]Item{qitem("tenant", "1")}, 1, qitem("tenant", "2"), nil), nil
		case 2:
			return journeyPublicOutput([]Item{qitem("tenant", "3")}, 1, qitem("tenant", "4"), nil), nil
		default:
			return nil, errors.New("unexpected secret query")
		}
	}}, func(o *Options) {
		o.Timeout = 2 * time.Second
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), TTL: 500 * time.Millisecond, MaxOpen: 2}
	})
	request := QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 5}
	cursor, err := c.OpenQueryJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	page1, err := c.NextReadPage(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	expiry := page1.Receipt.OriginalExpiry
	if expiry.IsZero() || page1.Next == nil || page1.Receipt.StopReason != ReadStopNonterminal {
		t.Fatal("first physical page was not acknowledged as nonterminal")
	}
	if !time.Now().Before(expiry) {
		t.Fatal("test did not resume before the recorded expiry")
	}
	page2, err := c.NextReadPage(context.Background(), page1.Next)
	if err != nil {
		t.Fatal(err)
	}
	if !page2.Receipt.OriginalExpiry.Equal(expiry) || page2.Next == nil || page2.Receipt.StopReason != ReadStopNonterminal {
		t.Fatal("resume renewed the original expiry")
	}
	if calls.Load() != 2 {
		t.Fatalf("got %d SDK calls, want 2", calls.Load())
	}

	var foreignCalls atomic.Int32
	foreign := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		foreignCalls.Add(1)
		return nil, errors.New("secret foreign query")
	}}, func(o *Options) {
		o.Timeout = 2 * time.Second
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), TTL: 500 * time.Millisecond, MaxOpen: 2}
	})
	page, err := foreign.NextReadPage(context.Background(), page2.Next)
	requireCode(t, err, "cursor")
	if !reflect.DeepEqual(page, ReadPage{}) || foreignCalls.Load() != 0 {
		t.Fatal("foreign cursor performed I/O")
	}

	remaining := time.Until(expiry)
	if remaining < 0 {
		t.Fatal("recorded expiry passed before the owned timer wait")
	}
	timer := time.NewTimer(remaining)
	outer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	defer outer.Stop()
	select {
	case <-timer.C:
	case <-outer.C:
		t.Fatal("owned expiry timer was not reached")
	}
	before := calls.Load()
	page, err = c.NextReadPage(context.Background(), page2.Next)
	if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(page, ReadPage{}) || calls.Load() != before {
		t.Fatal("expired background resume did not refuse without SDK work")
	}

	fresh, err := c.OpenQueryJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CloseReadJourney(fresh); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseReadJourney(fresh); err != nil {
		t.Fatal(err)
	}
}
