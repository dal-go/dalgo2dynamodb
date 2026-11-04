package dalgo2dynamodb

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func TestPublicScanJourneySharesAdmissionWithQuery(t *testing.T) {
	var scans, gets, queries atomic.Int32
	c := newTestClient(t, newScanCounters(&scans, &gets, &queries), func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"events"}, MaxOpen: 2}
	})
	queryRequest := QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1}
	scanRequest := ScanRequest{Collection: "events", MaxItems: 1, EvaluatedLimit: 1}
	var opened []*ReadCursor
	t.Cleanup(func() {
		for _, cursor := range opened {
			if cursor != nil {
				if err := c.CloseReadJourney(cursor); err != nil {
					t.Error(err)
				}
			}
		}
	})
	queryCursor, err := c.OpenQueryJourney(context.Background(), queryRequest)
	if err != nil {
		t.Fatal(err)
	}
	opened = append(opened, queryCursor)
	scanCursor, err := c.OpenScanJourney(context.Background(), scanRequest)
	if err != nil {
		t.Fatal(err)
	}
	opened = append(opened, scanCursor)
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("admission performed SDK I/O")
	}
	refusedQuery, err := c.OpenQueryJourney(context.Background(), queryRequest)
	if refusedQuery != nil {
		t.Fatal("third shared admission returned a query cursor")
	}
	requireCode(t, err, "limit")
	refusedScan, err := c.OpenScanJourney(context.Background(), scanRequest)
	if refusedScan != nil {
		t.Fatal("third shared admission returned a scan cursor")
	}
	requireCode(t, err, "limit")
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("refused admission performed SDK I/O")
	}
	if err := c.CloseReadJourney(queryCursor); err != nil {
		t.Fatal(err)
	}
	replacement, err := c.OpenScanJourney(context.Background(), scanRequest)
	if err != nil {
		t.Fatal(err)
	}
	opened = append(opened, replacement)
	if err := c.CloseReadJourney(scanCursor); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseReadJourney(replacement); err != nil {
		t.Fatal(err)
	}
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("closed handles performed SDK I/O")
	}
}

func TestPublicScanJourneyCanceledCallerOutlivesRawFuture(t *testing.T) {
	entered, release, returned, callerFinished := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var callerCancel context.CancelCauseFunc
	var callerStarted bool
	var scans, gets, queries atomic.Int32
	api := newScanCounters(&scans, &gets, &queries)
	api.scan = func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		if scans.Add(1) == 1 {
			close(entered)
			<-release
			close(returned)
			return nil, errors.New("late secret result")
		}
		return &dynamodb.ScanOutput{Items: []Item{qitem("tenant", "2")}, Count: 1, ScannedCount: 1}, nil
	}
	scanRequest := ScanRequest{Collection: "events", MaxItems: 1, EvaluatedLimit: 1}
	queryRequest := QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1}
	c := newTestClient(t, api, func(o *Options) {
		o.Timeout = 2 * time.Second
		o.MaxConcurrent = 1
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"events"}, MaxOpen: 1}
	})
	var cursor *ReadCursor
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
			probed, openErr := c.OpenQueryJourney(probeCtx, queryRequest)
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

	cursor, err := c.OpenScanJourney(context.Background(), scanRequest)
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
		t.Fatal("SDK scan did not start")
	}
	cancel(errors.New("private cause secret"))
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.Cause(ctx)) ||
			strings.Contains(result.err.Error(), "secret") || !reflect.DeepEqual(result.page, ReadPage{}) {
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
	refusedScan, err := c.OpenScanJourney(context.Background(), scanRequest)
	if refusedScan != nil {
		t.Fatal("held scan admission returned a cursor")
	}
	requireCode(t, err, "limit")
	if err := c.CloseReadJourney(cursor); err != nil {
		t.Fatal(err)
	}
	refusedQuery, err := c.OpenQueryJourney(context.Background(), queryRequest)
	if refusedQuery != nil {
		t.Fatal("held shared lease admitted a query cursor")
	}
	requireCode(t, err, "limit")
	if scans.Load() != 1 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("held shared lease performed unexpected SDK work")
	}
	before := scans.Load()
	if page, err := c.NextReadPage(context.Background(), cursor); err == nil || !reflect.DeepEqual(page, ReadPage{}) {
		t.Fatal("closed scan cursor returned a page")
	} else {
		requireCode(t, err, "cursor")
	}
	if scans.Load() != before {
		t.Fatal("closed scan cursor refetched")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("released SDK did not return")
	}
	select {
	case <-callerFinished:
	case <-time.After(time.Second):
		t.Fatal("caller goroutine did not finish after release")
	}
	probe, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	var recovered *ReadCursor
	for recovered == nil {
		if err := probe.Err(); err != nil {
			t.Fatalf("journey admission did not recover: %v", err)
		}
		recovered, err = c.OpenQueryJourney(probe, queryRequest)
		if err == nil {
			break
		}
		requireCode(t, err, "limit")
		runtime.Gosched()
	}
	if err := c.CloseReadJourney(recovered); err != nil {
		t.Error(err)
	}
	fresh, err := c.OpenScanJourney(context.Background(), scanRequest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.CloseReadJourney(fresh); err != nil {
			t.Error(err)
		}
	})
	page, err := c.NextReadPage(context.Background(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	if scans.Load() != 2 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("fresh scan used unexpected SDK methods or calls")
	}
	if page.Receipt.StopReason != ReadStopNativeEOF || !reflect.DeepEqual(page.Items, []Item{qitem("tenant", "2")}) {
		t.Fatal("fresh scan did not deliver one native EOF page")
	}
	if err := c.CloseReadJourney(fresh); err != nil {
		t.Fatal(err)
	}
}
