package dalgo2dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type journeyPublicPanicSentinel struct {
	marker string
}

func journeyPublicAuthority() ReadAuthority {
	return ReadAuthority{PrincipalScope: "principal", Generation: "generation", ResourceScope: "resource"}
}

func journeyPublicFloat(value float64) *float64 {
	return &value
}

func journeyPublicOutput(items []Item, scanned int32, lastEvaluatedKey Item, capacity *types.ConsumedCapacity) *dynamodb.QueryOutput {
	return &dynamodb.QueryOutput{
		Items:            items,
		Count:            int32(len(items)),
		ScannedCount:     scanned,
		LastEvaluatedKey: lastEvaluatedKey,
		ConsumedCapacity: capacity,
	}
}

func journeyPublicHasValue(input *dynamodb.QueryInput, want types.AttributeValue) bool {
	for _, value := range input.ExpressionAttributeValues {
		if equalKeyValue(value, want) {
			return true
		}
	}
	return false
}

func journeyPublicRecover(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return
}

func TestPublicQueryJourneyPendingRetryIsolationAndCapacity(t *testing.T) {
	binary := []byte{0, 255, 128, 7}
	firstRow := qitem("tenant", "1")
	firstRow["wide"] = n("9007199254740993.000")
	firstRow["binary"] = &types.AttributeValueMemberB{Value: binary}
	firstRow["strings"] = &types.AttributeValueMemberSS{Value: []string{"a", "b"}}
	firstRow["numbers"] = &types.AttributeValueMemberNS{Value: []string{"1.0", "9007199254740993"}}
	firstRow["binaries"] = &types.AttributeValueMemberBS{Value: [][]byte{{0, 255}, {1}}}
	firstRow["nested"] = &types.AttributeValueMemberM{Value: Item{"missing": &types.AttributeValueMemberNULL{Value: true}}}

	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		switch calls.Add(1) {
		case 1:
			if input.TableName == nil || *input.TableName != "existing-events" || input.Limit == nil || *input.Limit != 1 {
				t.Error("existing table/page size changed")
			}
			if !journeyPublicHasValue(input, s("tenant")) || journeyPublicHasValue(input, s("evil")) {
				t.Error("caller partition mutation reached the admitted SDK request")
			}
			if !journeyPublicHasValue(input, n("0")) || journeyPublicHasValue(input, n("999999")) {
				t.Error("caller sort mutation reached the admitted SDK request")
			}
			return journeyPublicOutput([]Item{firstRow}, 1, qitem("tenant", "1"), &types.ConsumedCapacity{CapacityUnits: journeyPublicFloat(0)}), nil
		case 2:
			if !reflect.DeepEqual(input.ExclusiveStartKey, qitem("tenant", "1")) {
				t.Error("caller page mutation changed the admitted continuation key")
			}
			return journeyPublicOutput(nil, 1, qitem("tenant", "2"), nil), nil
		case 3:
			return journeyPublicOutput([]Item{qitem("tenant", "3")}, 1, nil, &types.ConsumedCapacity{CapacityUnits: journeyPublicFloat(1.25)}), nil
		default:
			t.Errorf("unexpected SDK call %d", calls.Load())
			return nil, fmt.Errorf("unexpected SDK call %d", calls.Load())
		}
	}}, func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority()}
	})

	request := QueryRequest{
		Collection:             "events",
		Partition:              s("tenant"),
		Sort:                   &SortCondition{Operator: ">=", Values: []types.AttributeValue{n("0")}},
		Limit:                  3,
		ReturnConsumedCapacity: CapacityTotal,
	}
	cursor, err := c.OpenQueryJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("open dispatched SDK work")
	}
	request.Partition.(*types.AttributeValueMemberS).Value = "evil"
	request.Sort.Values[0].(*types.AttributeValueMemberN).Value = "999999"

	prepareEntered := false
	failed, err := c.NextReadPageWith(context.Background(), cursor, func(page ReadPage) error {
		prepareEntered = true
		if len(page.Items) != 1 {
			t.Fatalf("prepare received %d items, want 1", len(page.Items))
		}
		page.Items[0]["sequence"] = n("777")
		page.Items[0]["wide"] = n("1")
		if page.Receipt.Capacity != nil && page.Receipt.Capacity.Current != nil && page.Receipt.Capacity.Current.CapacityUnits != nil {
			page.Receipt.Capacity.Current.CapacityUnits = journeyPublicFloat(9)
		}
		return fmt.Errorf("secret prepare token: %w", &Error{Code: "transport"})
	})
	if !prepareEntered {
		t.Fatal("prepare callback was not entered")
	}
	requireCode(t, err, "prepare")
	if !reflect.DeepEqual(failed, ReadPage{}) || calls.Load() != 1 {
		t.Fatal("failed prepare leaked a page or dispatched again")
	}

	page1, err := c.NextReadPage(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || page1.Ordinal != 1 || page1.Receipt.StopReason != ReadStopNonterminal {
		t.Fatal("retry refetched or lost the retained pending page")
	}
	if !reflect.DeepEqual(page1.Items, []Item{firstRow}) {
		t.Fatal("failed prepare mutated the retained native page")
	}
	receipt1 := page1.Receipt
	if receipt1.FetchedPages != 1 || receipt1.AckedPages != 1 || receipt1.ReceivedRows != 1 || receipt1.AckedRows != 1 ||
		receipt1.ObservedScannedCount != 1 || receipt1.LogicalWork != 1 {
		t.Fatal("retry changed provisional metrics")
	}
	if receipt1.Capacity == nil || receipt1.Capacity.NativePages != 1 || receipt1.Capacity.ReportedPages != 1 ||
		receipt1.Capacity.CapacityUnits.ReportedTotal == nil || *receipt1.Capacity.CapacityUnits.ReportedTotal != 0 ||
		receipt1.Capacity.Current == nil || receipt1.Capacity.Current.CapacityUnits == nil || *receipt1.Capacity.Current.CapacityUnits != 0 {
		t.Fatal("explicit zero capacity was not preserved")
	}
	originalExpiry := receipt1.OriginalExpiry

	encoded, err := json.Marshal(page1)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "\"Next\"") || strings.Contains(string(encoded), "owner") {
		t.Fatal("process-local cursor became portable JSON")
	}

	before := calls.Load()
	_, err = c.NextReadPage(context.Background(), cursor)
	requireCode(t, err, "cursor")
	if calls.Load() != before {
		t.Fatal("consumed cursor re-dispatched SDK work")
	}

	page1.Items[0]["sequence"] = n("777")
	page1.Items[0]["wide"] = n("1")
	page1.Items[0]["binary"].(*types.AttributeValueMemberB).Value[0] = 42
	delete(page1.Items[0]["nested"].(*types.AttributeValueMemberM).Value, "missing")

	page2, err := c.NextReadPage(context.Background(), page1.Next)
	if err != nil {
		t.Fatal(err)
	}
	if page2.Ordinal != 2 || !page2.HasMore || page2.Next == nil || len(page2.Items) != 0 || page2.Receipt.StopReason != ReadStopNonterminal {
		t.Fatal("empty physical page was not delivered as a nonterminal page")
	}
	if page2.Receipt.Capacity == nil || page2.Receipt.Capacity.Current != nil || page2.Receipt.Capacity.NativePages != 2 || page2.Receipt.Capacity.ReportedPages != 1 {
		t.Fatal("missing capacity on the empty page was invented")
	}

	page3, err := c.NextReadPage(context.Background(), page2.Next)
	if err != nil {
		t.Fatal(err)
	}
	if page3.Ordinal != 3 || page3.HasMore || page3.Next != nil || page3.Receipt.StopReason != ReadStopNativeEOF {
		t.Fatal("native EOF page classification changed")
	}
	final := page3.Receipt
	if final.FetchedPages != 3 || final.AckedPages != 3 || final.ReceivedRows != 2 || final.AckedRows != 2 ||
		final.ObservedScannedCount != 3 || final.LogicalWork != 3 {
		t.Fatal("final journey receipt totals changed")
	}
	if !final.OriginalExpiry.Equal(originalExpiry) {
		t.Fatal("journey expiry was not stable across pages")
	}
	if final.Capacity == nil || final.Capacity.NativePages != 3 || final.Capacity.ReportedPages != 2 ||
		final.Capacity.CapacityUnits.ReportedPages != 2 || final.Capacity.CapacityUnits.Complete ||
		final.Capacity.CapacityUnits.ReportedTotal == nil || *final.Capacity.CapacityUnits.ReportedTotal != 1.25 {
		t.Fatal("capacity coverage or exact reported total changed")
	}
	if final.Capacity.Current == nil || final.Capacity.Current.CapacityUnits == nil || *final.Capacity.Current.CapacityUnits != 1.25 {
		t.Fatal("last page capacity was not preserved")
	}
}

func TestPublicQueryJourneyHardCapBoundary(t *testing.T) {
	cases := []struct {
		name         string
		rowLimit     int
		maxPages     int
		maxEvaluated int
		hasLEK       bool
		success      bool
		hasMore      bool
		hasNext      bool
		stop         ReadStopReason
	}{
		{"row-limit-wins", 1, 1, 1, true, true, true, false, ReadStopRequestedRowLimit},
		{"max-pages", 2, 1, 0, true, false, false, false, ""},
		{"max-evaluated", 2, 0, 1, true, false, false, false, ""},
		{"native-eof-even-at-caps", 2, 1, 1, false, true, false, false, ReadStopNativeEOF},
		{"finite-nonterminal", 2, 2, 2, true, true, true, true, ReadStopNonterminal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			c := newTestClient(t, &fakeAPI{query: func(_ context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				calls.Add(1)
				var lek Item
				if tc.hasLEK {
					lek = qitem("tenant", "2")
				}
				return journeyPublicOutput([]Item{qitem("tenant", "1")}, 1, lek, nil), nil
			}}, func(o *Options) {
				o.PageSize = 1
				if tc.maxPages > 0 {
					o.MaxPages = tc.maxPages
				}
				if tc.maxEvaluated > 0 {
					o.MaxEvaluated = tc.maxEvaluated
				}
				o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority()}
			})
			cursor, err := c.OpenQueryJourney(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: tc.rowLimit})
			if err != nil {
				t.Fatal(err)
			}
			prepareEntered := false
			page, err := c.NextReadPageWith(context.Background(), cursor, func(ReadPage) error {
				prepareEntered = true
				return nil
			})
			if calls.Load() != 1 {
				t.Fatalf("got %d SDK calls, want exactly one", calls.Load())
			}
			if !tc.success {
				requireCode(t, err, "limit")
				if !reflect.DeepEqual(page, ReadPage{}) || prepareEntered {
					t.Fatal("hard cap reached the prepare callback or returned a page")
				}
				before := calls.Load()
				if _, retryErr := c.NextReadPage(context.Background(), cursor); retryErr == nil || calls.Load() != before {
					t.Fatal("stale capped handle dispatched SDK work")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !prepareEntered || page.Ordinal != 1 || page.HasMore != tc.hasMore || page.Receipt.StopReason != tc.stop {
				t.Fatal("successful boundary page classification changed")
			}
			if tc.hasNext != (page.Next != nil) {
				t.Fatal("successor cursor presence changed")
			}
			if !tc.hasNext && page.Next != nil {
				t.Fatal("terminal-looking page retained a successor")
			}
		})
	}
}

func TestPublicQueryJourneyDeliveryAttemptLimitIsWholeJourney(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, _ *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls.Add(1)
		return journeyPublicOutput([]Item{qitem("tenant", "1")}, 1, qitem("tenant", "2"), nil), nil
	}}, func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), MaxDeliveryAttempts: 2}
	})
	cursor, err := c.OpenQueryJourney(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := c.NextReadPageWith(context.Background(), cursor, func(ReadPage) error {
		return fmt.Errorf("secret delivery token: %w", &Error{Code: "transport"})
	})
	requireCode(t, err, "prepare")
	if !reflect.DeepEqual(failed, ReadPage{}) || calls.Load() != 1 {
		t.Fatal("prepare failure leaked a page or refetched")
	}
	page, err := c.NextReadPage(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || page.Ordinal != 1 || !page.HasMore || page.Next == nil || page.Receipt.AckedPages != 1 {
		t.Fatal("second attempt did not acknowledge the retained nonterminal page")
	}
	failed, err = c.NextReadPage(context.Background(), page.Next)
	requireCode(t, err, "limit")
	if !reflect.DeepEqual(failed, ReadPage{}) || calls.Load() != 1 {
		t.Fatal("delivery attempt ceiling reset per page or dispatched before failing")
	}
}

func TestPublicQueryJourneyPanicRefundsPreviewCapacity(t *testing.T) {
	payload := strings.Repeat("x", 64*1024)
	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, _ *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls.Add(1)
		row := qitem("tenant", "1")
		row["payload"] = s(payload)
		return journeyPublicOutput([]Item{row}, 1, nil, nil), nil
	}}, func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), MaxRetainedBytes: 256 * 1024}
	})
	request := QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1}
	cursor, err := c.OpenQueryJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := &journeyPublicPanicSentinel{marker: "unique-recovered-panic"}
	for i := 0; i < 3; i++ {
		entered := false
		recovered := journeyPublicRecover(func() {
			_, _ = c.NextReadPageWith(context.Background(), cursor, func(page ReadPage) error {
				entered = true
				if len(page.Items) != 1 || !equalKeyValue(page.Items[0]["payload"], s(payload)) {
					t.Errorf("preview %d lost the retained item", i+1)
				}
				panic(sentinel)
			})
		})
		if recovered != sentinel {
			t.Fatalf("preview %d did not propagate the same panic sentinel: %T", i+1, recovered)
		}
		if !entered || calls.Load() != 1 {
			t.Fatalf("preview %d did not reuse the retained pending page", i+1)
		}
	}
	page, err := c.NextReadPage(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || page.Ordinal != 1 || page.Receipt.AckedPages != 1 || page.Receipt.ReceivedRows != 1 || page.Receipt.AckedRows != 1 {
		t.Fatal("acknowledged preview refetched or lost its receipt")
	}
	if len(page.Items) != 1 || !equalKeyValue(page.Items[0]["payload"], s(payload)) {
		t.Fatal("retained native payload changed")
	}

	reused, err := c.OpenQueryJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	reusedPage, err := c.NextReadPage(context.Background(), reused)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || reusedPage.Ordinal != 1 || len(reusedPage.Items) != 1 ||
		!equalKeyValue(reusedPage.Items[0]["payload"], s(payload)) {
		t.Fatal("journey admission or retained pool was not reusable after refunds")
	}
}
