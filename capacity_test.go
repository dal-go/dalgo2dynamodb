package dalgo2dynamodb

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func capacityTestFloat(v float64) *float64 { return &v }
func capacityTestString(v string) *string  { return &v }

func TestQueryCapacityDefaultAndSelector(t *testing.T) {
	for _, mode := range []CapacityMode{"", CapacityNone} {
		calls := 0
		c := newTestClient(t, &fakeAPI{query: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			calls++
			if string(in.ReturnConsumedCapacity) != string(mode) {
				t.Errorf("return-consumed-capacity = %q, want %q", in.ReturnConsumedCapacity, mode)
			}
			return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1}, nil
		}}, nil)
		page, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: mode})
		if err != nil {
			t.Fatal(err)
		}
		if calls != 1 || page.Capacity != nil {
			t.Fatalf("mode %q dispatched %d times or exposed capacity", mode, calls)
		}
		if mode == "" {
			data, err := json.Marshal(page)
			if err != nil {
				t.Fatal(err)
			}
			oldJSON := `{"Items":[{"account":{"Value":"tenant"},"sequence":{"Value":"1"}}],"Next":null,"Pages":1,"Evaluated":1}`
			if string(data) != oldJSON {
				t.Fatalf("legacy JSON changed: %s", data)
			}
		}
	}
	for _, mode := range []CapacityMode{"total", " TOTAL", "UNKNOWN"} {
		calls := 0
		c := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			calls++
			return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1}, nil
		}}, nil)
		_, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: mode})
		requireCode(t, err, "query")
		if calls != 0 {
			t.Fatalf("invalid mode %q dispatched", mode)
		}
	}
}

func TestQueryCapacityEveryNativePageAndCopies(t *testing.T) {
	pageOne := &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(0)}
	tableCapacity := &types.Capacity{CapacityUnits: capacityTestFloat(0.75), ReadCapacityUnits: capacityTestFloat(0.5)}
	localCapacity := types.Capacity{CapacityUnits: capacityTestFloat(0.25)}
	globalCapacity := types.Capacity{CapacityUnits: capacityTestFloat(0.25)}
	pageThree := &types.ConsumedCapacity{
		TableName:              capacityTestString("existing-events"),
		CapacityUnits:          capacityTestFloat(1.25),
		ReadCapacityUnits:      capacityTestFloat(1),
		Table:                  tableCapacity,
		LocalSecondaryIndexes:  map[string]types.Capacity{"lsi-1": localCapacity},
		GlobalSecondaryIndexes: map[string]types.Capacity{"gsi-1": globalCapacity},
	}
	calls := 0
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls++
		switch calls {
		case 1:
			if in.ExclusiveStartKey != nil {
				t.Error("first page started from a cursor")
			}
			return &dynamodb.QueryOutput{ScannedCount: 0, LastEvaluatedKey: qitem("tenant", "0"), ConsumedCapacity: pageOne}, nil
		case 2:
			if !reflect.DeepEqual(in.ExclusiveStartKey, qitem("tenant", "0")) {
				t.Error("second page cursor changed")
			}
			return &dynamodb.QueryOutput{ScannedCount: 2, LastEvaluatedKey: qitem("tenant", "1")}, nil
		default:
			if !reflect.DeepEqual(in.ExclusiveStartKey, qitem("tenant", "1")) {
				t.Error("third page cursor changed")
			}
			return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "2")}, Count: 1, ScannedCount: 1, ConsumedCapacity: pageThree}, nil
		}
	}}, func(o *Options) { o.PageSize = 3 })
	page, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 3, ReturnConsumedCapacity: CapacityTotal})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 || len(page.Items) != 1 || page.Pages != 3 || page.Evaluated != 4 {
		t.Fatalf("page traversal changed: calls=%d items=%d pages=%d evaluated=%d", calls, len(page.Items), page.Pages, page.Evaluated)
	}
	receipt := page.Capacity
	if receipt == nil || receipt.Mode != CapacityTotal || receipt.NativePages != 3 || receipt.ReportedPages != 2 || receipt.ObservedScannedCount != 3 {
		t.Fatalf("receipt summary: %+v", receipt)
	}
	if len(receipt.Pages) != 3 {
		t.Fatalf("native page count: %d", len(receipt.Pages))
	}
	if receipt.Pages[0].Ordinal != 1 || receipt.Pages[0].ScannedCount != 0 || receipt.Pages[0].ConsumedCapacity == nil || receipt.Pages[0].ConsumedCapacity.CapacityUnits == nil || *receipt.Pages[0].ConsumedCapacity.CapacityUnits != 0 {
		t.Fatalf("first page zero capacity changed: %+v", receipt.Pages[0])
	}
	if receipt.Pages[1].Ordinal != 2 || receipt.Pages[1].ScannedCount != 2 || receipt.Pages[1].ConsumedCapacity != nil {
		t.Fatalf("second page absent capacity changed: %+v", receipt.Pages[1])
	}
	if receipt.Pages[2].Ordinal != 3 || receipt.Pages[2].ScannedCount != 1 || receipt.Pages[2].ConsumedCapacity == nil {
		t.Fatalf("third page capacity changed: %+v", receipt.Pages[2])
	}
	if receipt.CapacityUnits.ReportedTotal == nil || *receipt.CapacityUnits.ReportedTotal != 1.25 || receipt.CapacityUnits.ReportedPages != 2 || receipt.CapacityUnits.Complete {
		t.Fatalf("capacity sum changed: %+v", receipt.CapacityUnits)
	}
	breakdown := receipt.Pages[2].ConsumedCapacity
	if breakdown.Table == nil || breakdown.Table.CapacityUnits == nil || *breakdown.Table.CapacityUnits != 0.75 || breakdown.Table.ReadCapacityUnits == nil || *breakdown.Table.ReadCapacityUnits != 0.5 {
		t.Fatalf("table breakdown changed: %+v", breakdown.Table)
	}
	if *breakdown.LocalSecondaryIndexes["lsi-1"].CapacityUnits != 0.25 || *breakdown.GlobalSecondaryIndexes["gsi-1"].CapacityUnits != 0.25 {
		t.Fatal("index breakdown changed")
	}
	if *receipt.CapacityUnits.ReportedTotal != 1.25 {
		t.Fatal("breakdown was double counted into CapacityUnits")
	}

	*pageOne.CapacityUnits = 99
	*pageThree.CapacityUnits = 99
	*pageThree.ReadCapacityUnits = 99
	*tableCapacity.CapacityUnits = 99
	*tableCapacity.ReadCapacityUnits = 99
	pageThree.LocalSecondaryIndexes["lsi-1"] = types.Capacity{CapacityUnits: capacityTestFloat(99)}
	delete(pageThree.GlobalSecondaryIndexes, "gsi-1")
	if *receipt.CapacityUnits.ReportedTotal != 1.25 || *breakdown.CapacityUnits != 1.25 || *receipt.Pages[0].ConsumedCapacity.CapacityUnits != 0 {
		t.Fatal("returned report borrowed SDK pointers")
	}
	if *breakdown.Table.CapacityUnits != 0.75 || *breakdown.Table.ReadCapacityUnits != 0.5 || *breakdown.LocalSecondaryIndexes["lsi-1"].CapacityUnits != 0.25 || *breakdown.GlobalSecondaryIndexes["gsi-1"].CapacityUnits != 0.25 {
		t.Fatal("returned report borrowed SDK maps")
	}

	allCalls := 0
	all := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		allCalls++
		output := &dynamodb.QueryOutput{Items: []Item{qitem("tenant", []string{"", "1", "2"}[allCalls])}, Count: 1, ScannedCount: 1, ConsumedCapacity: &types.ConsumedCapacity{CapacityUnits: capacityTestFloat(0.25)}}
		if allCalls == 1 {
			output.LastEvaluatedKey = qitem("tenant", "1")
		}
		return output, nil
	}}, func(o *Options) { o.PageSize = 1 })
	allPage, err := all.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 2, ReturnConsumedCapacity: CapacityTotal})
	if err != nil {
		t.Fatal(err)
	}
	if allCalls != 2 || len(allPage.Items) != 2 || allPage.Capacity == nil || allPage.Capacity.NativePages != 2 || !allPage.Capacity.CapacityUnits.Complete || allPage.Capacity.CapacityUnits.ReportedPages != 2 || allPage.Capacity.CapacityUnits.ReportedTotal == nil || *allPage.Capacity.CapacityUnits.ReportedTotal != 0.5 {
		t.Fatalf("all-present summary changed: %+v", allPage.Capacity)
	}
}

func TestQueryCapacityModeCursorBinding(t *testing.T) {
	capacity := &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(0.5)}
	calls := 0
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls++
		switch calls {
		case 1:
			if string(in.ReturnConsumedCapacity) != string(CapacityTotal) || in.ExclusiveStartKey != nil {
				t.Error("first TOTAL request changed")
			}
			return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, LastEvaluatedKey: qitem("tenant", "1"), ConsumedCapacity: capacity}, nil
		default:
			if string(in.ReturnConsumedCapacity) != string(CapacityTotal) || !reflect.DeepEqual(in.ExclusiveStartKey, qitem("tenant", "1")) {
				t.Error("TOTAL continuation changed")
			}
			return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "2")}, Count: 1, ScannedCount: 1, ConsumedCapacity: capacity}, nil
		}
	}}, nil)
	request := QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: CapacityTotal}
	first, err := c.QueryPartition(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 || !equalKeyValue(first.Items[0]["sequence"], n("1")) || first.Next == nil || first.Capacity == nil || first.Capacity.Mode != CapacityTotal {
		t.Fatal("TOTAL page did not return one row, cursor and capacity")
	}
	before := calls
	for _, mode := range []CapacityMode{"", CapacityNone, CapacityIndexes} {
		attempt := request
		attempt.Cursor = first.Next
		attempt.ReturnConsumedCapacity = mode
		_, err := c.QueryPartition(context.Background(), attempt)
		requireCode(t, err, "cursor")
		if calls != before {
			t.Fatalf("mode %q dispatched with a cursor bound to TOTAL", mode)
		}
	}
	resume := request
	resume.Cursor = first.Next
	next, err := c.QueryPartition(context.Background(), resume)
	if err != nil {
		t.Fatal(err)
	}
	if calls != before+1 || len(next.Items) != 1 || !equalKeyValue(next.Items[0]["sequence"], n("2")) || next.Next != nil || next.Capacity == nil || next.Capacity.Mode != CapacityTotal {
		t.Fatal("TOTAL continuation did not resume the next row at EOF")
	}
}
