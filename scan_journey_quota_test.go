package dalgo2dynamodb

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func scanQuotaTargetAPI(t *testing.T, targetKey string, scans, gets, queries *atomic.Int32) *scanClosureAPI {
	t.Helper()
	api := newScanCounters(scans, gets, queries)
	api.scan = func(_ context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		scans.Add(1)
		if input.TableName == nil || *input.TableName != "existing-table" || input.Limit == nil || *input.Limit != 1 {
			t.Error("target scan did not preserve the existing table or page size")
		}
		return &dynamodb.ScanOutput{Items: []Item{{"account": s(targetKey)}}, Count: 1, ScannedCount: 1}, nil
	}
	return api
}

func requireScanQuotaTarget(t *testing.T, page ReadPage, err error, scans, gets, queries *atomic.Int32, targetKey string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	successor := page.Next
	if scans.Load() != 1 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatalf("recovered scan target used %d Scan, %d Get, %d Query calls, want 1/0/0", scans.Load(), gets.Load(), queries.Load())
	}
	if page.Ordinal != 1 || page.Receipt.StopReason != ReadStopNativeEOF || page.HasMore || successor != nil ||
		len(page.Items) != 1 || !equalKeyValue(page.Items[0]["account"], s(targetKey)) {
		t.Fatal("recovered scan target did not return the single-row native EOF")
	}
}

func closeScanQuotaFillers(t *testing.T, c *Client, fillers *[]*ReadCursor) {
	t.Helper()
	for _, cursor := range *fillers {
		if err := c.CloseReadJourney(cursor); err != nil {
			t.Fatal(err)
		}
	}
	*fillers = nil
}

func TestPublicScanJourneyByteQuotaRefundRecovery(t *testing.T) {
	const targetKey = "p"
	var scans, gets, queries atomic.Int32
	c := newTestClient(t, scanQuotaTargetAPI(t, targetKey, &scans, &gets, &queries), func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"items"}, MaxOpen: 64, MaxRetainedBytes: 8192}
	})
	ctx := context.Background()
	target, err := c.OpenScanJourney(ctx, ScanRequest{Collection: "items", MaxItems: 1, EvaluatedLimit: 5})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseReadJourney(target) })
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("scan admission dispatched SDK work")
	}

	var fillers []*ReadCursor
	keep := func(cursor *ReadCursor) {
		fillers = append(fillers, cursor)
		t.Cleanup(func() { _ = c.CloseReadJourney(cursor) })
	}
	for {
		cursor, err := c.OpenQueryJourney(ctx, quotaItemRequest(strings.Repeat("f", 2048)))
		if err != nil {
			requireCode(t, err, "limit")
			break
		}
		keep(cursor)
	}
	if len(fillers) == 0 || len(fillers) >= 32 {
		t.Fatalf("filled %d 2048-byte snapshots, want 1..31", len(fillers))
	}
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("journey admission dispatched SDK work")
	}

	findScanByteFiller := func() *ReadCursor {
		low, high, best := 1, 2048, 0
		for low <= high {
			mid := (low + high) / 2
			cursor, err := c.OpenQueryJourney(ctx, quotaItemRequest(strings.Repeat("g", mid)))
			if err != nil {
				requireCode(t, err, "limit")
				high = mid - 1
				continue
			}
			if closeErr := c.CloseReadJourney(cursor); closeErr != nil {
				t.Fatal(closeErr)
			}
			best = mid
			low = mid + 1
		}
		if best == 0 {
			return nil
		}
		cursor, err := c.OpenQueryJourney(ctx, quotaItemRequest(strings.Repeat("g", best)))
		if err != nil {
			t.Fatalf("largest fitting scan byte filler was rejected: %v", err)
		}
		return cursor
	}
	extra := findScanByteFiller()
	if extra == nil && len(fillers) > 0 {
		if err := c.CloseReadJourney(fillers[len(fillers)-1]); err != nil {
			t.Fatal(err)
		}
		fillers = fillers[:len(fillers)-1]
		extra = findScanByteFiller()
	}
	if extra == nil {
		t.Fatal("could not consume the byte quota to the closed-scenario boundary")
	}
	keep(extra)
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("filler admission dispatched SDK work")
	}

	prepareEntered := false
	page, err := c.NextReadPageWith(ctx, target, func(ReadPage) error {
		prepareEntered = true
		return nil
	})
	requireCode(t, err, "quota")
	if prepareEntered || !reflect.DeepEqual(page, ReadPage{}) || scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("byte quota refusal returned a candidate page, entered prepare or dispatched SDK work")
	}

	closeScanQuotaFillers(t, c, &fillers)
	page, err = c.NextReadPage(ctx, target)
	requireScanQuotaTarget(t, page, err, &scans, &gets, &queries, targetKey)
}

func TestPublicScanJourneyNodeQuotaRefundRecovery(t *testing.T) {
	const targetKey = "p"
	var scans, gets, queries atomic.Int32
	c := newTestClient(t, scanQuotaTargetAPI(t, targetKey, &scans, &gets, &queries), func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"items"}, MaxOpen: 64, MaxRetainedNodes: 64, MaxPageNodes: 32, MaxRetainedBytes: 1 << 20}
	})
	ctx := context.Background()
	target, err := c.OpenScanJourney(ctx, ScanRequest{Collection: "items", MaxItems: 1, EvaluatedLimit: 5})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseReadJourney(target) })
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("scan admission dispatched SDK work")
	}

	var fillers []*ReadCursor
	for i := 0; ; i++ {
		cursor, err := c.OpenQueryJourney(ctx, quotaItemRequest(fmt.Sprintf("filler-%d", i)))
		if err != nil {
			requireCode(t, err, "limit")
			break
		}
		fillers = append(fillers, cursor)
		t.Cleanup(func() { _ = c.CloseReadJourney(cursor) })
		if len(fillers)+1 >= 64 {
			t.Fatalf("node limit was not reached before MaxOpen: %d fillers", len(fillers))
		}
	}
	if len(fillers) < 1 || len(fillers)+1 >= 64 {
		t.Fatalf("filled %d node snapshots, want 1..63", len(fillers))
	}
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("journey admission dispatched SDK work")
	}

	prepareEntered := false
	page, err := c.NextReadPageWith(ctx, target, func(ReadPage) error {
		prepareEntered = true
		return nil
	})
	requireCode(t, err, "quota")
	if prepareEntered || !reflect.DeepEqual(page, ReadPage{}) || scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("node quota refusal returned a candidate page, entered prepare or dispatched SDK work")
	}

	closeScanQuotaFillers(t, c, &fillers)
	page, err = c.NextReadPage(ctx, target)
	requireScanQuotaTarget(t, page, err, &scans, &gets, &queries, targetKey)
}
