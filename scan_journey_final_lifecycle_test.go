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

func TestPublicScanJourneyCallerDeadlineAfterSDKEntry(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	callerFinished := make(chan struct{})
	var releaseOnce sync.Once
	var callerCancel context.CancelFunc
	var callerStarted bool
	var scans, gets, queries atomic.Int32
	api := newScanCounters(&scans, &gets, &queries)
	api.scan = func(ctx context.Context, _ *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		switch scans.Add(1) {
		case 1:
			close(entered)
			select {
			case <-ctx.Done():
				close(returned)
				return nil, ctx.Err()
			case <-release:
				close(returned)
				return nil, errors.New("fixture release")
			}
		case 2:
			return &dynamodb.ScanOutput{Items: []Item{{"account": s("recovered")}}, Count: 1, ScannedCount: 1}, nil
		default:
			return nil, errors.New("unexpected secret scan")
		}
	}
	c := newTestClient(t, api, func(o *Options) {
		o.MaxConcurrent = 1
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"items"}, MaxOpen: 1}
	})
	request := ScanRequest{Collection: "items", MaxItems: 1, EvaluatedLimit: 1}
	var cursor *ReadCursor
	t.Cleanup(func() {
		if callerCancel != nil {
			callerCancel()
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
		if cursor != nil {
			if err := c.CloseReadJourney(cursor); err != nil {
				t.Error(err)
			}
			cursor = nil
		}
		probeCtx, stopProbe := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopProbe()
		probeRequest := QueryRequest{Collection: "items", Partition: s("tenant"), Limit: 1}
		var probeCursor *ReadCursor
		for probeCursor == nil {
			if err := probeCtx.Err(); err != nil {
				t.Errorf("cleanup: journey admission did not recover after deadline: %v", err)
				break
			}
			probed, openErr := c.OpenQueryJourney(probeCtx, probeRequest)
			if openErr == nil {
				probeCursor = probed
				break
			}
			var typed *Error
			if !errors.As(openErr, &typed) || typed.Code != "limit" {
				t.Errorf("cleanup: unexpected error probing admission recovery: %v", openErr)
				break
			}
			runtime.Gosched()
		}
		if probeCursor != nil {
			if err := c.CloseReadJourney(probeCursor); err != nil {
				t.Error(err)
			}
		}
	})

	var err error
	cursor, err = c.OpenScanJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
	case <-time.After(2 * time.Second):
		t.Fatal("SDK scan did not start")
	}
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("caller deadline was not observed")
	}
	select {
	case result := <-done:
		if !errors.Is(result.err, context.DeadlineExceeded) || strings.Contains(result.err.Error(), "secret") {
			t.Fatalf("caller deadline was not exact: %v", result.err)
		}
		if !reflect.DeepEqual(result.page, ReadPage{}) {
			t.Fatal("deadline returned a candidate page")
		}
	case <-time.After(time.Second):
		t.Fatal("caller deadline did not return promptly")
	}
	if scans.Load() != 1 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("deadline journey used unexpected SDK calls")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("SDK scan did not return after its context deadline")
	}
	if err := c.CloseReadJourney(cursor); err != nil {
		t.Fatal(err)
	}
	cursor = nil

	probeCtx, stopProbe := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopProbe()
	probeRequest := QueryRequest{Collection: "items", Partition: s("tenant"), Limit: 1}
	var recovered *ReadCursor
	for recovered == nil {
		if err := probeCtx.Err(); err != nil {
			t.Fatalf("journey admission did not recover after deadline: %v", err)
		}
		probed, openErr := c.OpenQueryJourney(probeCtx, probeRequest)
		if openErr == nil {
			recovered = probed
			break
		}
		requireCode(t, openErr, "limit")
		runtime.Gosched()
	}
	if err := c.CloseReadJourney(recovered); err != nil {
		t.Error(err)
	}

	fresh, err := c.OpenScanJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseReadJourney(fresh) })
	page, err := c.NextReadPage(context.Background(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	if scans.Load() != 2 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("fresh scan after deadline used unexpected SDK methods or calls")
	}
	if page.Receipt.StopReason != ReadStopNativeEOF || !reflect.DeepEqual(page.Items, []Item{{"account": s("recovered")}}) {
		t.Fatal("fresh scan after deadline did not deliver native EOF")
	}
	if err := c.CloseReadJourney(fresh); err != nil {
		t.Fatal(err)
	}
}

func TestPublicScanJourneyOriginalExpiryIsNotRenewed(t *testing.T) {
	var scans, gets, queries atomic.Int32
	firstKey := Item{"account": s("k1")}
	secondKey := Item{"account": s("k2")}
	api := newScanCounters(&scans, &gets, &queries)
	api.scan = func(_ context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		switch scans.Add(1) {
		case 1:
			if len(input.ExclusiveStartKey) != 0 {
				t.Error("first scan request had a continuation key")
			}
			return &dynamodb.ScanOutput{Items: []Item{{"account": s("row-1")}}, Count: 1, ScannedCount: 1, LastEvaluatedKey: firstKey}, nil
		case 2:
			if !reflect.DeepEqual(input.ExclusiveStartKey, firstKey) {
				t.Error("second scan request did not use the exact first continuation key")
			}
			return &dynamodb.ScanOutput{Items: []Item{{"account": s("row-2")}}, Count: 1, ScannedCount: 1, LastEvaluatedKey: secondKey}, nil
		default:
			return nil, errors.New("unexpected secret scan")
		}
	}
	c := newTestClient(t, api, func(o *Options) {
		o.Timeout = 2 * time.Second
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"items"}, TTL: 500 * time.Millisecond, MaxOpen: 2}
	})
	request := ScanRequest{Collection: "items", MaxItems: 5, EvaluatedLimit: 5}
	var current *ReadCursor
	t.Cleanup(func() {
		if current != nil {
			_ = c.CloseReadJourney(current)
		}
	})
	var err error
	current, err = c.OpenScanJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	page1, err := c.NextReadPage(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	current = page1.Next
	expiry := page1.Receipt.OriginalExpiry
	if expiry.IsZero() || current == nil || page1.Ordinal != 1 || page1.Receipt.StopReason != ReadStopNonterminal {
		t.Fatal("first scan page was not acknowledged as nonterminal")
	}
	if !time.Now().Before(expiry) {
		t.Fatal("test did not resume before the recorded expiry")
	}
	if scans.Load() != 1 || !reflect.DeepEqual(page1.Items, []Item{{"account": s("row-1")}}) {
		t.Fatal("first scan page changed")
	}
	page2, err := c.NextReadPage(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	current = page2.Next
	if !page2.Receipt.OriginalExpiry.Equal(expiry) || page2.Next == nil || page2.Ordinal != 2 || page2.Receipt.StopReason != ReadStopNonterminal {
		t.Fatal("scan resume renewed the original expiry")
	}
	if scans.Load() != 2 || !reflect.DeepEqual(page2.Items, []Item{{"account": s("row-2")}}) {
		t.Fatal("second scan page changed")
	}

	remaining := time.Until(expiry)
	if remaining < 0 {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	outer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	defer outer.Stop()
	select {
	case <-timer.C:
	case <-outer.C:
		t.Fatal("owned scan expiry timer was not reached")
	}
	before := scans.Load()
	page, err := c.NextReadPage(context.Background(), current)
	if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(page, ReadPage{}) || scans.Load() != before {
		t.Fatal("expired background scan resume did not refuse without SDK work")
	}
	if gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("scan journey used Query or Get")
	}
	fresh, err := c.OpenScanJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CloseReadJourney(fresh); err != nil {
		t.Fatal(err)
	}
}

func TestPublicScanJourneyFinalDeliveryCallbacks(t *testing.T) {
	for _, mode := range []string{"caller-cancel-over-close-and-prepare", "close-only", "panic-propagation"} {
		t.Run(mode, func(t *testing.T) {
			var scans, gets, queries atomic.Int32
			api := newScanCounters(&scans, &gets, &queries)
			api.scan = func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
				if scans.Add(1) != 1 {
					return nil, errors.New("unexpected secret scan")
				}
				return &dynamodb.ScanOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, LastEvaluatedKey: qitem("tenant", "2")}, nil
			}
			c := newTestClient(t, api, func(o *Options) {
				o.PageSize = 1
				o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"events"}}
			})
			cursor, err := c.OpenScanJourney(context.Background(), ScanRequest{Collection: "events", MaxItems: 1, EvaluatedLimit: 1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.CloseReadJourney(cursor) })

			entered := false
			var page ReadPage
			switch mode {
			case "caller-cancel-over-close-and-prepare":
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				page, err = c.NextReadPageWith(ctx, cursor, func(ReadPage) error {
					entered = true
					cancel(errors.New("private cause secret"))
					if closeErr := c.Close(); closeErr != nil {
						t.Error(closeErr)
					}
					return fmt.Errorf("private prepare token: %w", &Error{Code: "transport"})
				})
				if !entered {
					t.Fatal("prepare was not entered")
				}
				if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "closed") {
					t.Fatalf("caller cancellation lost priority: %v", err)
				}
				if !reflect.DeepEqual(page, ReadPage{}) {
					t.Fatal("cancelled prepare leaked a page")
				}
			case "close-only":
				page, err = c.NextReadPageWith(context.Background(), cursor, func(ReadPage) error {
					entered = true
					if closeErr := c.Close(); closeErr != nil {
						t.Error(closeErr)
					}
					return nil
				})
				if !entered {
					t.Fatal("prepare was not entered")
				}
				requireCode(t, err, "closed")
				if !reflect.DeepEqual(page, ReadPage{}) {
					t.Fatal("closed prepare leaked a page")
				}
			case "panic-propagation":
				sentinel := &journeyPublicPanicSentinel{marker: "unique-scan-panic"}
				recovered := journeyPublicRecover(func() {
					_, _ = c.NextReadPageWith(context.Background(), cursor, func(ReadPage) error {
						entered = true
						panic(sentinel)
					})
				})
				if !entered || recovered != sentinel {
					t.Fatalf("prepare panic was swallowed: %T", recovered)
				}
				if closeErr := c.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}
			if scans.Load() != 1 || gets.Load() != 0 || queries.Load() != 0 {
				t.Fatal("final delivery callback used unexpected SDK methods or calls")
			}
			before := scans.Load()
			if _, retryErr := c.NextReadPage(context.Background(), cursor); retryErr == nil || scans.Load() != before {
				t.Fatal("closed scan dispatched additional SDK work")
			}
		})
	}
}
