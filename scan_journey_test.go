package dalgo2dynamodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type scanClosureAPI struct {
	*fakeAPI
	scan func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

func (f *scanClosureAPI) Scan(ctx context.Context, input *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if f.scan == nil {
		return nil, errors.New("unexpected Scan")
	}
	return f.scan(ctx, input)
}

func newScanCounters(s, g, q *atomic.Int32) *scanClosureAPI {
	return &scanClosureAPI{fakeAPI: &fakeAPI{
		get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			g.Add(1)
			return nil, errors.New("unexpected Get")
		},
		query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			q.Add(1)
			return nil, errors.New("unexpected Query")
		},
	}, scan: func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		s.Add(1)
		return nil, errors.New("unexpected Scan")
	}}
}

func TestPublicScanJourneyAdmissionAndCallerPriority(t *testing.T) {
	request := ScanRequest{Collection: "events", MaxItems: 1, EvaluatedLimit: 1, ReturnConsumedCapacity: CapacityIndexes}
	granted := &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"events"}}
	cases := []struct {
		name     string
		profile  *JourneyOptions
		withScan bool
		code     string
	}{
		{"journey-disabled", nil, true, "journey-disabled"},
		{"grant-missing", &JourneyOptions{Authority: journeyPublicAuthority()}, true, "scan-disabled"},
		{"api-without-scan", granted, false, "scan-disabled"},
		{"valid-open", granted, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var scans, gets, queries atomic.Int32
			var api API
			if tc.withScan {
				api = newScanCounters(&scans, &gets, &queries)
			} else {
				api = &fakeAPI{
					get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
						gets.Add(1)
						return nil, errors.New("unexpected Get")
					},
					query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
						queries.Add(1)
						return nil, errors.New("unexpected Query")
					},
				}
			}
			c := newTestClient(t, api, func(o *Options) { o.PageSize = 1; o.Journeys = tc.profile })
			cursor, err := c.OpenScanJourney(context.Background(), request)
			if tc.code != "" {
				requireCode(t, err, tc.code)
				if cursor != nil {
					t.Fatal("refusal returned a cursor")
				}
			} else if err != nil || cursor == nil {
				t.Fatalf("granted open failed: %v", err)
			} else {
				t.Cleanup(func() { c.CloseReadJourney(cursor) })
			}
			if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
				t.Fatal("scan admission performed SDK I/O")
			}
		})
	}

	for _, closed := range []bool{false, true} {
		name := "canceled-context"
		if closed {
			name = "closed-canceled"
		}
		t.Run(name, func(t *testing.T) {
			var scans, gets, queries atomic.Int32
			c := newTestClient(t, newScanCounters(&scans, &gets, &queries), func(o *Options) { o.PageSize = 1; o.Journeys = granted })
			if closed {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(errors.New("secret cause"))
			cursor, err := c.OpenScanJourney(ctx, request)
			if cursor != nil || !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("caller cancellation priority lost: %v", err)
			}
			if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
				t.Fatal("canceled admission performed SDK I/O")
			}
		})
	}
}

func TestPublicScanJourneyNativeDeliveryAndRetry(t *testing.T) {
	native := func() Item {
		row := qitem("tenantA", "1")
		row["wide"] = n("9007199254740993.000")
		row["binary"] = &types.AttributeValueMemberB{Value: []byte{0, 255, 7}}
		row["missing"] = &types.AttributeValueMemberNULL{Value: true}
		return row
	}
	var scans, gets, queries atomic.Int32
	api := newScanCounters(&scans, &gets, &queries)
	api.scan = func(_ context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		call := scans.Add(1)
		if input.TableName == nil || *input.TableName != "existing-events" || input.Limit == nil || *input.Limit != 1 {
			t.Errorf("scan call %d table/limit changed", call)
		}
		if input.ConsistentRead == nil || *input.ConsistentRead || input.ReturnConsumedCapacity != types.ReturnConsumedCapacityIndexes {
			t.Errorf("scan call %d consistency/capacity mapping changed", call)
		}
		if input.FilterExpression != nil || input.IndexName != nil || input.ProjectionExpression != nil || input.Select != "" || input.Segment != nil || input.TotalSegments != nil || input.ExpressionAttributeNames != nil || input.ExpressionAttributeValues != nil {
			t.Errorf("scan call %d carried query/filter/index/projection/segment state", call)
		}
		switch call {
		case 1:
			if len(input.ExclusiveStartKey) != 0 {
				t.Error("first scan request had a continuation key")
			}
			return &dynamodb.ScanOutput{Count: 0, ScannedCount: 0, LastEvaluatedKey: qitem("otherA", "0"), ConsumedCapacity: &types.ConsumedCapacity{CapacityUnits: journeyPublicFloat(0)}}, nil
		case 2:
			if !reflect.DeepEqual(input.ExclusiveStartKey, qitem("otherA", "0")) {
				t.Error("second scan request did not use the exact previous key")
			}
			return &dynamodb.ScanOutput{Items: []Item{native()}, Count: 1, ScannedCount: 1, LastEvaluatedKey: qitem("otherB", "2"), ConsumedCapacity: &types.ConsumedCapacity{CapacityUnits: journeyPublicFloat(0.5)}}, nil
		case 3:
			if !reflect.DeepEqual(input.ExclusiveStartKey, qitem("otherB", "2")) {
				t.Error("third scan request did not use the exact previous key")
			}
			return &dynamodb.ScanOutput{Items: []Item{native()}, Count: 1, ScannedCount: 1}, nil
		default:
			t.Errorf("unexpected scan call %d", call)
			return nil, fmt.Errorf("unexpected scan call %d", call)
		}
	}
	c := newTestClient(t, api, func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"events"}}
	})
	cursor, err := c.OpenScanJourney(context.Background(), ScanRequest{Collection: "events", MaxItems: 3, EvaluatedLimit: 5, ReturnConsumedCapacity: CapacityIndexes})
	if err != nil {
		t.Fatal(err)
	}
	active := cursor
	t.Cleanup(func() {
		if active != nil {
			c.CloseReadJourney(active)
		}
	})
	if scans.Load() != 0 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("open dispatched SDK work")
	}

	entered := false
	failed, err := c.NextReadPageWith(context.Background(), cursor, func(page ReadPage) error {
		entered = true
		if len(page.Items) != 0 {
			t.Errorf("prepare received %d items from empty frame", len(page.Items))
		}
		return fmt.Errorf("secret prepare token: %w", &Error{Code: "transport"})
	})
	requireCode(t, err, "prepare")
	if !entered || !reflect.DeepEqual(failed, ReadPage{}) || scans.Load() != 1 || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("failed prepare leaked a page, skipped the frame or used a non-Scan method")
	}
	page1, err := c.NextReadPage(context.Background(), cursor)
	if err != nil {
		t.Fatal(err)
	}
	active = page1.Next
	receipt1 := page1.Receipt
	if scans.Load() != 1 || page1.Ordinal != 1 || !page1.HasMore || page1.Next == nil || len(page1.Items) != 0 || receipt1.StopReason != ReadStopNonterminal {
		t.Fatal("retry refetched or changed the retained empty frame")
	}
	if receipt1.FetchedPages != 1 || receipt1.AckedPages != 1 || receipt1.ReceivedRows != 0 || receipt1.AckedRows != 0 || receipt1.ObservedScannedCount != 0 || receipt1.LogicalWork != 1 {
		t.Fatal("retry changed provisional empty-frame metrics")
	}
	if receipt1.Capacity == nil || receipt1.Capacity.NativePages != 1 || receipt1.Capacity.ReportedPages != 1 || receipt1.Capacity.CapacityUnits.ReportedTotal == nil || *receipt1.Capacity.CapacityUnits.ReportedTotal != 0 || receipt1.Capacity.Current == nil || receipt1.Capacity.Current.CapacityUnits == nil || *receipt1.Capacity.Current.CapacityUnits != 0 {
		t.Fatal("explicit zero capacity was lost or counted twice")
	}
	originalExpiry := receipt1.OriginalExpiry

	before := scans.Load()
	_, err = c.NextReadPage(context.Background(), cursor)
	requireCode(t, err, "cursor")
	if scans.Load() != before {
		t.Fatal("consumed handle re-dispatched SDK work")
	}
	page2, err := c.NextReadPage(context.Background(), page1.Next)
	if err != nil {
		t.Fatal(err)
	}
	active = page2.Next
	if scans.Load() != 2 || page2.Ordinal != 2 || !page2.HasMore || page2.Next == nil || page2.Receipt.StopReason != ReadStopNonterminal {
		t.Fatal("second native page classification changed")
	}
	if !reflect.DeepEqual(page2.Items, []Item{native()}) {
		t.Fatal("second native page lost exact N/B/NULL values")
	}
	receipt2 := page2.Receipt
	if receipt2.FetchedPages != 2 || receipt2.AckedPages != 2 || receipt2.ReceivedRows != 1 || receipt2.AckedRows != 1 || receipt2.ObservedScannedCount != 1 || receipt2.LogicalWork != 2 {
		t.Fatal("second page metrics changed")
	}
	if receipt2.Capacity == nil || receipt2.Capacity.NativePages != 2 || receipt2.Capacity.ReportedPages != 2 || receipt2.Capacity.CapacityUnits.ReportedTotal == nil || *receipt2.Capacity.CapacityUnits.ReportedTotal != 0.5 || receipt2.Capacity.Current == nil || receipt2.Capacity.Current.CapacityUnits == nil || *receipt2.Capacity.Current.CapacityUnits != 0.5 {
		t.Fatal("second page capacity changed")
	}

	page3, err := c.NextReadPage(context.Background(), page2.Next)
	if err != nil {
		t.Fatal(err)
	}
	active = page3.Next
	if scans.Load() != 3 || page3.Ordinal != 3 || page3.HasMore || page3.Next != nil || page3.Receipt.StopReason != ReadStopNativeEOF {
		t.Fatal("native EOF page classification changed")
	}
	if !reflect.DeepEqual(page3.Items, []Item{native()}) || !reflect.DeepEqual(page2.Items[0], page3.Items[0]) {
		t.Fatal("duplicate native row was deduplicated or reordered")
	}
	final := page3.Receipt
	if final.FetchedPages != 3 || final.AckedPages != 3 || final.ReceivedRows != 2 || final.AckedRows != 2 || final.ObservedScannedCount != 2 || final.LogicalWork != 3 {
		t.Fatal("final scan journey receipt totals changed")
	}
	if !final.OriginalExpiry.Equal(originalExpiry) || !receipt1.OriginalExpiry.Equal(originalExpiry) {
		t.Fatal("journey expiry was not stable across pages")
	}
	if final.Capacity == nil || final.Capacity.NativePages != 3 || final.Capacity.ReportedPages != 2 || final.Capacity.CapacityUnits.ReportedPages != 2 || final.Capacity.CapacityUnits.ReportedTotal == nil || *final.Capacity.CapacityUnits.ReportedTotal != 0.5 || final.Capacity.Current != nil {
		t.Fatal("capacity coverage or last-page observation changed")
	}
	if gets.Load() != 0 || queries.Load() != 0 {
		t.Fatal("Scan journey used Query or Get")
	}
}
