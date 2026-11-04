package dalgo2dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestQueryCapacityMalformedReportFailClosed(t *testing.T) {
	tooMany := make(map[string]types.Capacity, 33)
	for i := 0; i < 33; i++ {
		tooMany[fmt.Sprintf("gsi-%02d", i)] = types.Capacity{CapacityUnits: capacityTestFloat(0.25)}
	}
	var vectorCapacity types.VectorCapacity
	for _, tc := range []struct {
		name     string
		capacity *types.ConsumedCapacity
	}{
		{"negative", &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(-1)}},
		{"NaN", &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(math.NaN())}},
		{"Inf", &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(math.Inf(1))}},
		{"table-name", &types.ConsumedCapacity{TableName: capacityTestString("other-events"), CapacityUnits: capacityTestFloat(0.5)}},
		{"index-label", &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(0.5), GlobalSecondaryIndexes: map[string]types.Capacity{"bad label!": {CapacityUnits: capacityTestFloat(0.25)}}}},
		{"index-count", &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(0.5), GlobalSecondaryIndexes: tooMany}},
		{"vector-indexes", &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(0.5), VectorIndexes: map[string]types.VectorCapacity{"vector-1": vectorCapacity}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				calls++
				return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, ConsumedCapacity: tc.capacity}, nil
			}}, nil)
			page, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: CapacityTotal})
			requireCode(t, err, "invalid-response")
			if calls != 1 {
				t.Fatalf("malformed report dispatched %d times", calls)
			}
			if !reflect.DeepEqual(page, QueryPage{}) {
				t.Fatalf("malformed report leaked a partial page: %+v", page)
			}
		})
	}

	for _, mode := range []CapacityMode{"", CapacityNone} {
		name := "omitted"
		if mode != "" {
			name = string(mode)
		}
		t.Run("unsolicited-"+name, func(t *testing.T) {
			calls := 0
			c := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				calls++
				return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, ConsumedCapacity: &types.ConsumedCapacity{TableName: capacityTestString("wrong-secret"), CapacityUnits: capacityTestFloat(-1)}}, nil
			}}, nil)
			page, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: mode})
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || page.Capacity != nil || len(page.Items) != 1 {
				t.Fatalf("omitted/NONE stopped ignoring an unsolicited report: %+v", page)
			}
		})
	}

	t.Run("cumulative-overflow", func(t *testing.T) {
		calls := 0
		capacity := &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(math.MaxFloat64)}
		c := newTestClient(t, &fakeAPI{query: func(_ context.Context, _ *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			calls++
			sort := "0"
			if calls > 1 {
				sort = "1"
			}
			return &dynamodb.QueryOutput{ScannedCount: 0, LastEvaluatedKey: qitem("tenant", sort), ConsumedCapacity: capacity}, nil
		}}, func(o *Options) { o.PageSize = 1 })
		page, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: CapacityTotal})
		requireCode(t, err, "invalid-response")
		if calls != 2 {
			t.Fatalf("overflow dispatched %d times, want 2", calls)
		}
		if !reflect.DeepEqual(page, QueryPage{}) {
			t.Fatalf("overflow leaked a partial page: %+v", page)
		}
	})
}

func TestQueryCapacityAggregateParsedBudget(t *testing.T) {
	indexes := make(map[string]types.Capacity, 32)
	for i := 0; i < 32; i++ {
		indexes[fmt.Sprintf("gsi-%02d-%s", i, strings.Repeat("x", 190))] = types.Capacity{CapacityUnits: capacityTestFloat(float64(i) / 100)}
	}
	output := func() *dynamodb.QueryOutput {
		return &dynamodb.QueryOutput{
			Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1,
			ConsumedCapacity: &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(1), GlobalSecondaryIndexes: indexes},
		}
	}

	validCalls := 0
	valid := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		validCalls++
		return output(), nil
	}}, nil)
	page, err := valid.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: CapacityTotal})
	if err != nil {
		t.Fatal(err)
	}
	if validCalls != 1 || page.Capacity == nil || len(page.Capacity.Pages) != 1 || page.Capacity.Pages[0].ConsumedCapacity == nil || len(page.Capacity.Pages[0].ConsumedCapacity.GlobalSecondaryIndexes) != 32 {
		t.Fatalf("default-budget fixture is not valid: %+v", page.Capacity)
	}

	calls := 0
	bounded := newTestClient(t, &fakeAPI{query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		calls++
		return output(), nil
	}}, func(o *Options) { o.MaxParsedBytes = 256 })
	page, err = bounded.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: CapacityTotal})
	requireCode(t, err, "limit")
	if calls != 1 {
		t.Fatalf("budget failure dispatched %d times, want 1", calls)
	}
	if !reflect.DeepEqual(page, QueryPage{}) {
		t.Fatalf("budget failure leaked a partial page: %+v", page)
	}
}

func TestQueryCapacityCancellationAfterEarlierPage(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "caller"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			finished := make(chan struct{})
			var released sync.Once
			t.Cleanup(func() { released.Do(func() { close(release) }) })
			var calls atomic.Int32
			c := newTestClient(t, &fakeAPI{query: func(ctx context.Context, _ *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				switch calls.Add(1) {
				case 1:
					return &dynamodb.QueryOutput{ScannedCount: 0, LastEvaluatedKey: qitem("tenant", "0"), ConsumedCapacity: &types.ConsumedCapacity{TableName: capacityTestString("existing-events"), CapacityUnits: capacityTestFloat(0.5)}}, nil
				case 2:
					close(entered)
					select {
					case <-ctx.Done():
					case <-release:
					}
					close(finished)
					return nil, ctx.Err()
				default:
					return &dynamodb.QueryOutput{Items: []Item{qitem("tenant", "9")}, Count: 1, ScannedCount: 1}, nil
				}
			}}, func(o *Options) { o.PageSize = 1 })

			parent := context.Background()
			var cancel context.CancelFunc
			if deadline {
				parent, cancel = context.WithTimeout(parent, time.Second)
			} else {
				parent, cancel = context.WithCancel(parent)
			}
			defer cancel()

			type outcome struct {
				page QueryPage
				err  error
			}
			result := make(chan outcome, 1)
			go func() {
				page, err := c.QueryPartition(parent, QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1, ReturnConsumedCapacity: CapacityTotal})
				result <- outcome{page: page, err: err}
			}()

			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("second native page was not entered")
			}
			if !deadline {
				cancel()
			}
			select {
			case got := <-result:
				expected := context.Canceled
				if deadline {
					expected = context.DeadlineExceeded
				}
				if !errors.Is(got.err, expected) {
					t.Fatalf("caller/deadline error = %v, want %v", got.err, expected)
				}
				if !reflect.DeepEqual(got.page, QueryPage{}) {
					t.Fatalf("cancellation leaked a partial page: %+v", got.page)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("QueryPartition did not return after cancellation")
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("borrowed fake query did not finish")
			}
			if calls.Load() != 2 {
				t.Fatalf("cancellation dispatched %d native pages, want 2", calls.Load())
			}
		})
	}
}
