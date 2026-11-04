package dalgo2dynamodb

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func newScanBoundsClient(t *testing.T, api API, amend func(*Options)) *Client {
	t.Helper()
	return newTestClient(t, api, func(o *Options) {
		o.PageSize = 1
		o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"events"}}
		if amend != nil {
			amend(o)
		}
	})
}

func openScanBounds(t *testing.T, c *Client, request ScanRequest) (*ReadCursor, func(*ReadCursor)) {
	t.Helper()
	cursor, err := c.OpenScanJourney(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	active := cursor
	t.Cleanup(func() {
		if active != nil {
			c.CloseReadJourney(active)
		}
	})
	return cursor, func(next *ReadCursor) { active = next }
}

func nextScanBounds(t *testing.T, c *Client, cursor *ReadCursor) (ReadPage, bool, error) {
	t.Helper()
	prepared := false
	page, err := c.NextReadPageWith(context.Background(), cursor, func(ReadPage) error {
		prepared = true
		return nil
	})
	return page, prepared, err
}

func assertScanBoundsOnly(t *testing.T, scans, gets, queries *atomic.Int32, want int32) {
	t.Helper()
	if scans.Load() != want || gets.Load() != 0 || queries.Load() != 0 {
		t.Fatalf("method usage: scans=%d gets=%d queries=%d, want scans=%d", scans.Load(), gets.Load(), queries.Load(), want)
	}
}

func TestPublicScanJourneyUnfilteredCountScannedContract(t *testing.T) {
	t.Run("mismatch-refused-whole", func(t *testing.T) {
		var scans, gets, queries atomic.Int32
		api := newScanCounters(&scans, &gets, &queries)
		api.scan = func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			scans.Add(1)
			return &dynamodb.ScanOutput{Count: 0, ScannedCount: 1, LastEvaluatedKey: qitem("tenant", "2")}, nil
		}
		c := newScanBoundsClient(t, api, nil)
		cursor, _ := openScanBounds(t, c, ScanRequest{Collection: "events", MaxItems: 2, EvaluatedLimit: 2})
		page, prepared, err := nextScanBounds(t, c, cursor)
		requireCode(t, err, "invalid-response")
		if prepared || !reflect.DeepEqual(page, ReadPage{}) {
			t.Fatal("malformed Count/ScannedCount reached prepare or leaked a page")
		}
		assertScanBoundsOnly(t, &scans, &gets, &queries, 1)
	})

	t.Run("zero-row-continuation-admitted", func(t *testing.T) {
		var scans, gets, queries atomic.Int32
		api := newScanCounters(&scans, &gets, &queries)
		api.scan = func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			scans.Add(1)
			return &dynamodb.ScanOutput{Count: 0, ScannedCount: 0, LastEvaluatedKey: qitem("tenant", "2")}, nil
		}
		c := newScanBoundsClient(t, api, nil)
		cursor, own := openScanBounds(t, c, ScanRequest{Collection: "events", MaxItems: 2, EvaluatedLimit: 2})
		page, prepared, err := nextScanBounds(t, c, cursor)
		if err != nil {
			t.Fatal(err)
		}
		own(page.Next)
		if !prepared || len(page.Items) != 0 || !page.HasMore || page.Next == nil || page.Receipt.StopReason != ReadStopNonterminal {
			t.Fatal("zero-row native page with continuation was misclassified")
		}
		assertScanBoundsOnly(t, &scans, &gets, &queries, 1)
	})
}

func TestPublicScanJourneyBorrowedInputMutation(t *testing.T) {
	t.Run("limit-mutation-cannot-widen-admission", func(t *testing.T) {
		var scans, gets, queries atomic.Int32
		api := newScanCounters(&scans, &gets, &queries)
		api.scan = func(_ context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			scans.Add(1)
			*input.Limit = 3
			return &dynamodb.ScanOutput{Items: []Item{qitem("tenant", "1"), qitem("tenant", "2"), qitem("tenant", "3")}, Count: 3, ScannedCount: 3}, nil
		}
		c := newScanBoundsClient(t, api, nil)
		cursor, _ := openScanBounds(t, c, ScanRequest{Collection: "events", MaxItems: 5, EvaluatedLimit: 5})
		page, prepared, err := nextScanBounds(t, c, cursor)
		requireCode(t, err, "invalid-response")
		if prepared || !reflect.DeepEqual(page, ReadPage{}) {
			t.Fatal("mutated wire limit widened the admitted page or reached prepare")
		}
		assertScanBoundsOnly(t, &scans, &gets, &queries, 1)
	})

	t.Run("table-mutation-cannot-change-capacity-scope", func(t *testing.T) {
		var scans, gets, queries atomic.Int32
		foreign := "foreign-events"
		api := newScanCounters(&scans, &gets, &queries)
		api.scan = func(_ context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			scans.Add(1)
			*input.TableName = foreign
			return &dynamodb.ScanOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, ConsumedCapacity: &types.ConsumedCapacity{TableName: &foreign, CapacityUnits: journeyPublicFloat(1)}}, nil
		}
		c := newScanBoundsClient(t, api, nil)
		cursor, _ := openScanBounds(t, c, ScanRequest{Collection: "events", MaxItems: 2, EvaluatedLimit: 2, ReturnConsumedCapacity: CapacityIndexes})
		page, prepared, err := nextScanBounds(t, c, cursor)
		requireCode(t, err, "invalid-response")
		if prepared || !reflect.DeepEqual(page, ReadPage{}) {
			t.Fatal("mutated table or foreign capacity replaced the original admission scope")
		}
		assertScanBoundsOnly(t, &scans, &gets, &queries, 1)
	})

	t.Run("original-row-and-report-still-succeed", func(t *testing.T) {
		var scans, gets, queries atomic.Int32
		original, foreign := "existing-events", "foreign-events"
		api := newScanCounters(&scans, &gets, &queries)
		api.scan = func(_ context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			scans.Add(1)
			*input.Limit = 3
			*input.TableName = foreign
			return &dynamodb.ScanOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, ConsumedCapacity: &types.ConsumedCapacity{TableName: &original, CapacityUnits: journeyPublicFloat(0.5)}}, nil
		}
		c := newScanBoundsClient(t, api, nil)
		cursor, own := openScanBounds(t, c, ScanRequest{Collection: "events", MaxItems: 5, EvaluatedLimit: 5, ReturnConsumedCapacity: CapacityIndexes})
		page, prepared, err := nextScanBounds(t, c, cursor)
		if err != nil {
			t.Fatal(err)
		}
		own(page.Next)
		if !prepared || !reflect.DeepEqual(page.Items, []Item{qitem("tenant", "1")}) || page.Receipt.StopReason != ReadStopNativeEOF {
			t.Fatal("valid original row was rejected after borrowed input mutation")
		}
		current := page.Receipt.Capacity
		if current == nil || current.Current == nil || current.Current.TableName == nil || *current.Current.TableName != original || current.Current.CapacityUnits == nil || *current.Current.CapacityUnits != 0.5 {
			t.Fatal("valid original-table capacity report was lost")
		}
		assertScanBoundsOnly(t, &scans, &gets, &queries, 1)
	})
}

func TestPublicScanJourneyEvaluationAndEOFMatrix(t *testing.T) {
	fixture := func(t *testing.T, output *dynamodb.ScanOutput, amend func(*Options)) (*Client, *atomic.Int32, *atomic.Int32, *atomic.Int32) {
		t.Helper()
		var scans, gets, queries atomic.Int32
		api := newScanCounters(&scans, &gets, &queries)
		api.scan = func(context.Context, *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			scans.Add(1)
			return output, nil
		}
		return newScanBoundsClient(t, api, amend), &scans, &gets, &queries
	}

	t.Run("evaluated-limit-refuses-before-prepare", func(t *testing.T) {
		request := ScanRequest{Collection: "events", MaxItems: 2, EvaluatedLimit: 1}
		output := &dynamodb.ScanOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, LastEvaluatedKey: qitem("tenant", "2")}
		c, scans, gets, queries := fixture(t, output, func(o *Options) { o.MaxEvaluated = 5 })
		cursor, _ := openScanBounds(t, c, request)
		page, prepared, err := nextScanBounds(t, c, cursor)
		requireCode(t, err, "limit")
		if prepared || !reflect.DeepEqual(page, ReadPage{}) {
			t.Fatal("evaluated ceiling reached prepare or returned a candidate page")
		}
		assertScanBoundsOnly(t, scans, gets, queries, 1)
		before := scans.Load()
		if _, retryErr := c.NextReadPage(context.Background(), cursor); retryErr == nil || scans.Load() != before {
			t.Fatal("capped handle produced an impossible successor")
		}
	})

	t.Run("native-eof-at-same-cap-succeeds", func(t *testing.T) {
		request := ScanRequest{Collection: "events", MaxItems: 2, EvaluatedLimit: 1}
		output := &dynamodb.ScanOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1}
		c, scans, gets, queries := fixture(t, output, func(o *Options) { o.MaxEvaluated = 5 })
		cursor, own := openScanBounds(t, c, request)
		page, prepared, err := nextScanBounds(t, c, cursor)
		if err != nil {
			t.Fatal(err)
		}
		own(page.Next)
		if !prepared || len(page.Items) != 1 || page.HasMore || page.Next != nil || page.Receipt.StopReason != ReadStopNativeEOF {
			t.Fatal("native EOF at the evaluated cap was not a successful terminal page")
		}
		assertScanBoundsOnly(t, scans, gets, queries, 1)
	})

	t.Run("row-limit-terminal-page-succeeds", func(t *testing.T) {
		request := ScanRequest{Collection: "events", MaxItems: 1, EvaluatedLimit: 1}
		output := &dynamodb.ScanOutput{Items: []Item{qitem("tenant", "1")}, Count: 1, ScannedCount: 1, LastEvaluatedKey: qitem("tenant", "2")}
		c, scans, gets, queries := fixture(t, output, func(o *Options) { o.MaxEvaluated = 5 })
		cursor, own := openScanBounds(t, c, request)
		page, prepared, err := nextScanBounds(t, c, cursor)
		if err != nil {
			t.Fatal(err)
		}
		own(page.Next)
		if !prepared || len(page.Items) != 1 || !page.HasMore || page.Next != nil || page.Receipt.StopReason != ReadStopRequestedRowLimit {
			t.Fatal("requested row limit did not terminate with a visible continuation")
		}
		assertScanBoundsOnly(t, scans, gets, queries, 1)
	})
}

func TestPublicScanJourneyNativeKeyCycle(t *testing.T) {
	a, b, row := qitem("tenant", "1"), qitem("tenant", "2"), qitem("tenant", "9")
	var scans, gets, queries atomic.Int32
	api := newScanCounters(&scans, &gets, &queries)
	api.scan = func(_ context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
		switch call := scans.Add(1); call {
		case 1:
			if len(input.ExclusiveStartKey) != 0 {
				t.Error("first scan carried a continuation key")
			}
			return &dynamodb.ScanOutput{Items: []Item{row}, Count: 1, ScannedCount: 1, LastEvaluatedKey: a}, nil
		case 2:
			if !reflect.DeepEqual(input.ExclusiveStartKey, a) {
				t.Error("second scan did not use key A")
			}
			return &dynamodb.ScanOutput{Items: []Item{row}, Count: 1, ScannedCount: 1, LastEvaluatedKey: b}, nil
		case 3:
			if !reflect.DeepEqual(input.ExclusiveStartKey, b) {
				t.Error("third scan did not use key B")
			}
			return &dynamodb.ScanOutput{Items: []Item{row}, Count: 1, ScannedCount: 1, LastEvaluatedKey: a}, nil
		default:
			return nil, fmt.Errorf("unexpected scan call %d", call)
		}
	}
	c := newScanBoundsClient(t, api, nil)
	cursor, own := openScanBounds(t, c, ScanRequest{Collection: "events", MaxItems: 4, EvaluatedLimit: 4})
	page1, prepared1, err := nextScanBounds(t, c, cursor)
	if err != nil {
		t.Fatal(err)
	}
	own(page1.Next)
	if !prepared1 || !page1.HasMore || page1.Next == nil || page1.Receipt.StopReason != ReadStopNonterminal || !reflect.DeepEqual(page1.Items, []Item{row}) {
		t.Fatal("first cycle page was misclassified or deduplicated")
	}
	page2, prepared2, err := nextScanBounds(t, c, page1.Next)
	if err != nil {
		t.Fatal(err)
	}
	own(page2.Next)
	if !prepared2 || !page2.HasMore || page2.Next == nil || page2.Receipt.StopReason != ReadStopNonterminal || !reflect.DeepEqual(page2.Items, []Item{row}) {
		t.Fatal("second cycle page was misclassified or reordered")
	}
	page3, prepared3, err := nextScanBounds(t, c, page2.Next)
	requireCode(t, err, "cursor")
	if prepared3 || !reflect.DeepEqual(page3, ReadPage{}) {
		t.Fatal("repeated native key delivered a page or reached prepare")
	}
	assertScanBoundsOnly(t, &scans, &gets, &queries, 3)
}
