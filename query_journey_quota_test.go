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

func quotaItemRequest(partition string) QueryRequest {
	return QueryRequest{Collection: "items", Partition: s(partition), Limit: 1}
}

func TestPublicQueryJourneyByteQuotaRefundRecovery(t *testing.T) {
	const targetKey = "p"
	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls.Add(1)
		if input.TableName == nil || *input.TableName != "existing-table" || !journeyPublicHasValue(input, s(targetKey)) {
			t.Error("target query did not preserve the existing table or partition")
		}
		return journeyPublicOutput([]Item{{"account": s(targetKey)}}, 1, nil, nil), nil
	}}, func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), MaxOpen: 64, MaxRetainedBytes: 8192}
	})
	ctx := context.Background()
	target, err := c.OpenQueryJourney(ctx, quotaItemRequest(targetKey))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseReadJourney(target) })

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
	if calls.Load() != 0 {
		t.Fatal("journey admission dispatched SDK work")
	}

	find := func() *ReadCursor {
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
			t.Fatalf("largest fitting filler was rejected: %v", err)
		}
		return cursor
	}
	extra := find()
	if extra == nil && len(fillers) > 0 {
		if err := c.CloseReadJourney(fillers[len(fillers)-1]); err != nil {
			t.Fatal(err)
		}
		fillers = fillers[:len(fillers)-1]
		extra = find()
	}
	if extra == nil {
		t.Fatal("could not consume the byte quota to the closed-scenario boundary")
	}
	keep(extra)
	if calls.Load() != 0 {
		t.Fatal("filler admission dispatched SDK work")
	}

	page, err := c.NextReadPage(ctx, target)
	requireCode(t, err, "quota")
	if !reflect.DeepEqual(page, ReadPage{}) || calls.Load() != 0 {
		t.Fatal("byte quota refusal returned a candidate page or dispatched SDK work")
	}

	for _, cursor := range fillers {
		if err := c.CloseReadJourney(cursor); err != nil {
			t.Fatal(err)
		}
	}
	fillers = nil
	page, err = c.NextReadPage(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("recovered target used %d SDK calls, want 1", calls.Load())
	}
	if page.Receipt.StopReason != ReadStopNativeEOF || page.HasMore || page.Next != nil ||
		len(page.Items) != 1 || !equalKeyValue(page.Items[0]["account"], s(targetKey)) {
		t.Fatal("recovered target did not return the single-row native EOF")
	}
}

func TestPublicQueryJourneyNodeQuotaRefundRecovery(t *testing.T) {
	const targetKey = "p"
	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls.Add(1)
		if input.TableName == nil || *input.TableName != "existing-table" || !journeyPublicHasValue(input, s(targetKey)) {
			t.Error("target query did not preserve the existing table or partition")
		}
		return journeyPublicOutput([]Item{{"account": s(targetKey)}}, 1, nil, nil), nil
	}}, func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), MaxOpen: 64, MaxRetainedNodes: 64, MaxPageNodes: 32, MaxRetainedBytes: 1 << 20}
	})
	ctx := context.Background()
	target, err := c.OpenQueryJourney(ctx, quotaItemRequest(targetKey))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseReadJourney(target) })

	var fillers []*ReadCursor
	for i := 0; ; i++ {
		cursor, err := c.OpenQueryJourney(ctx, quotaItemRequest(fmt.Sprintf("filler-%d", i)))
		if err != nil {
			requireCode(t, err, "limit")
			break
		}
		fillers = append(fillers, cursor)
		t.Cleanup(func() { _ = c.CloseReadJourney(cursor) })
		if len(fillers) >= 64 {
			t.Fatalf("node limit was not reached before MaxOpen: %d fillers", len(fillers))
		}
	}
	if len(fillers) < 1 || len(fillers) >= 64 {
		t.Fatalf("filled %d node snapshots, want 1..63", len(fillers))
	}
	if calls.Load() != 0 {
		t.Fatal("journey admission dispatched SDK work")
	}

	page, err := c.NextReadPage(ctx, target)
	requireCode(t, err, "quota")
	if !reflect.DeepEqual(page, ReadPage{}) || calls.Load() != 0 {
		t.Fatal("node quota refusal returned a candidate page or dispatched SDK work")
	}

	for _, cursor := range fillers {
		if err := c.CloseReadJourney(cursor); err != nil {
			t.Fatal(err)
		}
	}
	fillers = nil
	page, err = c.NextReadPage(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("recovered target used %d SDK calls, want 1", calls.Load())
	}
	if page.Receipt.StopReason != ReadStopNativeEOF || page.HasMore || page.Next != nil ||
		len(page.Items) != 1 || !equalKeyValue(page.Items[0]["account"], s(targetKey)) {
		t.Fatal("recovered target did not return the single-row native EOF")
	}
}
