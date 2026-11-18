package dalgo2dynamodb

import (
	"context"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestQueryEmptyPagesNativeKeysAndContinuation(t *testing.T) {
	calls := 0
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls++
		if *in.TableName != "existing-events" || *in.KeyConditionExpression != "#p = :p" || *in.Limit != 1 || in.IndexName != nil || in.FilterExpression != nil {
			t.Error("query authority/limit changed")
		}
		switch calls {
		case 1:
			return &dynamodb.QueryOutput{LastEvaluatedKey: qitem("tenant", "0"), ScannedCount: 1}, nil
		case 2:
			if !reflect.DeepEqual(in.ExclusiveStartKey, qitem("tenant", "0")) {
				t.Error("empty page cursor lost")
			}
			return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, LastEvaluatedKey: qitem("tenant", "1")}, nil
		default:
			return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "2")}, Count: 1, ScannedCount: 1}, nil
		}
	}}, func(o *Options) { o.PageSize = 1 })
	request := QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1}
	page, err := c.QueryPartition(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Pages != 2 || page.Evaluated != 2 || page.Next == nil {
		t.Fatal("empty cursor page ended traversal")
	}
	before := calls
	wrong := request
	wrong.Partition = s("other-secret")
	wrong.Cursor = page.Next
	_, err = c.QueryPartition(context.Background(), wrong)
	requireCode(t, err, "cursor")
	if calls != before {
		t.Fatal("cursor scope mismatch dispatched")
	}
	other := newTestClient(t, &fakeAPI{}, nil)
	wrong = request
	wrong.Cursor = page.Next
	_, err = other.QueryPartition(context.Background(), wrong)
	requireCode(t, err, "cursor")
	request.Cursor = page.Next
	next, err := c.QueryPartition(context.Background(), request)
	if err != nil || len(next.Items) != 1 || next.Next != nil || !equalKeyValue(next.Items[0]["sequence"], n("2")) {
		t.Fatal("continuation failed")
	}
}

func TestQueryMalformedRepliesAndWorkBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		output *dynamodb.QueryOutput
		code   string
		amend  func(*Options)
	}{
		{"over-return", &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "1"), qitem("tenant", "2")}, Count: 2, ScannedCount: 2}, "invalid-response", nil},
		{"foreign-partition", &dynamodb.QueryOutput{Items: []Item{qitem("secret", "1")}, Count: 1, ScannedCount: 1}, "invalid-response", nil},
		{"count", &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "1")}, ScannedCount: 1}, "invalid-response", nil},
		{"foreign-cursor", &dynamodb.QueryOutput{LastEvaluatedKey: qitem("secret", "1"), ScannedCount: 1}, "invalid-response", nil},
		{"repeated-cursor", &dynamodb.QueryOutput{LastEvaluatedKey: qitem("tenant", "1"), ScannedCount: 1}, "cursor", nil},
		{"pages", &dynamodb.QueryOutput{LastEvaluatedKey: qitem("tenant", "1"), ScannedCount: 1}, "limit", func(o *Options) { o.MaxPages = 1 }},
		{"evaluated", &dynamodb.QueryOutput{LastEvaluatedKey: qitem("tenant", "1"), ScannedCount: 1}, "limit", func(o *Options) { o.MaxEvaluated = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) { return tc.output, nil }}, tc.amend)
			_, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1})
			requireCode(t, err, tc.code)
		})
	}
}

func TestQuerySortUsesBoundExpressionsAndRefusesForeignRows(t *testing.T) {
	c := newTestClient(t, &fakeAPI{query: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if *in.KeyConditionExpression != "#p = :p AND #s BETWEEN :s0 AND :s1" || !equalKeyValue(in.ExpressionAttributeValues[":s0"], n("1")) {
			t.Error("sort parameters not bound")
		}
		return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "3")}, Count: 1, ScannedCount: 1}, nil
	}}, nil)
	_, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Sort: &SortCondition{Operator: "between", Values: []types.AttributeValue{n("1"), n("2")}}, Limit: 1})
	requireCode(t, err, "invalid-response")
}

func TestQueryChecksOrderWithoutLosingWideNumbers(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(map[bool]string{false: "ascending", true: "descending"}[descending], func(t *testing.T) {
			items := []Item{qitem("tenant", "9007199254740993"), qitem("tenant", "9007199254740994")}
			if descending {
				items[0], items[1] = items[1], items[0]
			}
			c := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				return &dynamodb.QueryOutput{Items: items, Count: 2, ScannedCount: 2}, nil
			}}, nil)
			request := QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 2, Descending: descending}
			if page, err := c.QueryPartition(context.Background(), request); err != nil || len(page.Items) != 2 {
				t.Fatal("exact native order failed")
			}
			items[0], items[1] = items[1], items[0]
			_, err := c.QueryPartition(context.Background(), request)
			requireCode(t, err, "invalid-response")
		})
	}
}
