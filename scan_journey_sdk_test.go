package dalgo2dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestOfficialSDKScanJourneyOwnedTLS(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode CapacityMode
	}{
		{"omitted", ""},
		{"NONE", CapacityNone},
		{"TOTAL", CapacityTotal},
		{"INDEXES", CapacityIndexes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests, credentialCalls atomic.Int32
			firstPage := `{"Items":[],"Count":0,"ScannedCount":0,"LastEvaluatedKey":{"account":{"S":"other-partition"},"sequence":{"N":"0"}},"ConsumedCapacity":{"TableName":"existing-events","CapacityUnits":0}}`
			richSecondPage := `{"Items":[{"account":{"S":"tenant"},"sequence":{"N":"9007199254740993.000"},"binary":{"B":"AP+A"},"ns":{"NS":["1.0","9007199254740993"]},"ss":{"SS":["a","b"]},"bs":{"BS":["AP8=","AQ=="]},"missing":{"NULL":true}}],"Count":1,"ScannedCount":1,"ConsumedCapacity":{"TableName":"existing-events","CapacityUnits":0.5,"ReadCapacityUnits":0.5,"Table":{"CapacityUnits":0.5,"ReadCapacityUnits":0.5},"LocalSecondaryIndexes":{"lsi-1":{"CapacityUnits":0.25,"ReadCapacityUnits":0.25}},"GlobalSecondaryIndexes":{"gsi-1":{"CapacityUnits":0.25,"ReadCapacityUnits":0.25}}}}`
			sparseSecondPage := `{"Items":[{"account":{"S":"tenant"},"sequence":{"N":"9007199254740993.000"},"binary":{"B":"AP+A"},"ns":{"NS":["1.0","9007199254740993"]},"ss":{"SS":["a","b"]},"bs":{"BS":["AP8=","AQ=="]},"missing":{"NULL":true}}],"Count":1,"ScannedCount":1,"ConsumedCapacity":{"TableName":"existing-events","CapacityUnits":0.5}}`
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := requests.Add(1)
				raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8192))
				if err != nil || n > 2 {
					t.Error("scan journey body not fully consumed or unexpected request")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/" || r.URL.RawQuery != "" || r.TLS == nil ||
					!strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") ||
					r.Header.Get("X-Amz-Date") == "" || r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.Scan" {
					t.Error("unsigned or unexpected scan transport")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var body map[string]json.RawMessage
				if err := json.Unmarshal(raw, &body); err != nil {
					t.Error("scan journey body is not one closed JSON object")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				expectedKeys := 3
				if tc.mode != "" {
					expectedKeys++
				}
				if n == 2 {
					expectedKeys++
				}
				if len(body) != expectedKeys {
					t.Errorf("scan journey body has %d top-level fields, want %d", len(body), expectedKeys)
				}
				var table string
				if json.Unmarshal(body["TableName"], &table) != nil || table != "existing-events" {
					t.Error("physical table changed")
				}
				var limit int
				var consistent bool
				if json.Unmarshal(body["Limit"], &limit) != nil || limit != 1 {
					t.Error("wire scan page size changed")
				}
				if json.Unmarshal(body["ConsistentRead"], &consistent) != nil || consistent {
					t.Error("scan consistency changed")
				}
				for _, forbidden := range []string{"FilterExpression", "KeyConditionExpression", "IndexName", "ProjectionExpression", "Select", "AttributesToGet", "KeyConditions", "QueryFilter", "ConditionalOperator", "ReturnValues", "ScanFilter", "ScanIndexForward", "Segment", "TotalSegments", "ExpressionAttributeNames", "ExpressionAttributeValues"} {
					if _, ok := body[forbidden]; ok {
						t.Errorf("unexpected scan field %s", forbidden)
					}
				}
				if tc.mode == "" {
					if _, ok := body["ReturnConsumedCapacity"]; ok {
						t.Error("omitted mode was transmitted")
					}
				} else {
					var mode string
					if json.Unmarshal(body["ReturnConsumedCapacity"], &mode) != nil || mode != string(tc.mode) {
						t.Error("explicit mode was not transmitted verbatim")
					}
				}
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				if n == 1 {
					if _, ok := body["ExclusiveStartKey"]; ok {
						t.Error("first scan started from a cursor")
					}
					_, _ = w.Write([]byte(firstPage))
					return
				}
				var start map[string]map[string]string
				if json.Unmarshal(body["ExclusiveStartKey"], &start) != nil || !reflect.DeepEqual(start, map[string]map[string]string{"account": {"S": "other-partition"}, "sequence": {"N": "0"}}) {
					t.Error("second scan did not use the exact native LEK")
				}
				if tc.mode == CapacityTotal {
					_, _ = w.Write([]byte(sparseSecondPage))
					return
				}
				_, _ = w.Write([]byte(richSecondPage))
			}))
			t.Cleanup(server.Close)
			sdk := sdkFor(server, ownedCredentials{retrieve: func(context.Context) (aws.Credentials, error) {
				credentialCalls.Add(1)
				return fixtureCredentials(), nil
			}})
			t.Cleanup(server.Client().CloseIdleConnections)
			c := newTestClient(t, sdk, func(o *Options) {
				o.PageSize = 1
				o.Journeys = &JourneyOptions{Authority: journeyPublicAuthority(), ScanCollections: []string{"events"}}
			})
			var active *ReadCursor
			t.Cleanup(func() {
				if active != nil {
					c.CloseReadJourney(active)
				}
			})
			cursor, err := c.OpenScanJourney(context.Background(), ScanRequest{Collection: "events", MaxItems: 3, EvaluatedLimit: 5, ReturnConsumedCapacity: tc.mode})
			if err != nil {
				t.Fatal(err)
			}
			active = cursor
			if requests.Load() != 0 || credentialCalls.Load() != 0 {
				t.Fatal("Open dispatched fetch or credentials")
			}
			prepareEntered := false
			failed, err := c.NextReadPageWith(context.Background(), cursor, func(page ReadPage) error {
				prepareEntered = true
				if len(page.Items) != 0 || page.Ordinal != 1 || page.Receipt.FetchedPages != 1 {
					t.Errorf("prepare frame changed: ordinal=%d fetched=%d items=%d", page.Ordinal, page.Receipt.FetchedPages, len(page.Items))
				}
				return fmt.Errorf("secret prepare token: %w", &Error{Code: "transport"})
			})
			if !prepareEntered {
				t.Fatal("prepare callback was not entered")
			}
			requireCode(t, err, "prepare")
			if !reflect.DeepEqual(failed, ReadPage{}) || requests.Load() != 1 {
				t.Fatal("failed prepare leaked a page or dispatched again")
			}
			if credentialCalls.Load() == 0 {
				t.Fatal("official signer did not acquire synthetic credentials")
			}
			page1, err := c.NextReadPage(context.Background(), cursor)
			if err != nil {
				t.Fatal(err)
			}
			active = page1.Next
			if requests.Load() != 1 || page1.Ordinal != 1 || !page1.HasMore || page1.Next == nil || page1.Receipt.StopReason != ReadStopNonterminal || len(page1.Items) != 0 {
				t.Fatal("retry refetched or the first empty native page changed")
			}
			receipt1 := page1.Receipt
			if receipt1.FetchedPages != 1 || receipt1.AckedPages != 1 || receipt1.ReceivedRows != 0 || receipt1.AckedRows != 0 || receipt1.ObservedScannedCount != 0 || receipt1.LogicalWork != 1 {
				t.Fatalf("first scan receipt changed: %+v", receipt1)
			}
			if tc.mode != CapacityTotal && tc.mode != CapacityIndexes {
				if page1.Receipt.Capacity != nil {
					t.Fatal("omitted/NONE exposed unsolicited capacity")
				}
			} else {
				cap1 := page1.Receipt.Capacity
				if cap1 == nil || cap1.Mode != tc.mode || cap1.NativePages != 1 || cap1.ReportedPages != 1 ||
					cap1.CapacityUnits.ReportedTotal == nil || *cap1.CapacityUnits.ReportedTotal != 0 || cap1.CapacityUnits.ReportedPages != 1 || !cap1.CapacityUnits.Complete ||
					cap1.ReadCapacityUnits.ReportedTotal != nil || cap1.ReadCapacityUnits.ReportedPages != 0 || cap1.ReadCapacityUnits.Complete ||
					cap1.WriteCapacityUnits.ReportedTotal != nil || cap1.WriteCapacityUnits.ReportedPages != 0 || cap1.WriteCapacityUnits.Complete ||
					cap1.Current == nil || cap1.Current.CapacityUnits == nil || *cap1.Current.CapacityUnits != 0 || cap1.Current.ReadCapacityUnits != nil ||
					cap1.Current.Table != nil || cap1.Current.LocalSecondaryIndexes != nil || cap1.Current.GlobalSecondaryIndexes != nil {
					t.Fatalf("first scan capacity receipt changed: %+v", cap1)
				}
			}
			before := requests.Load()
			_, err = c.NextReadPage(context.Background(), cursor)
			requireCode(t, err, "cursor")
			if requests.Load() != before {
				t.Fatal("stale handle dispatched SDK work")
			}
			page2, err := c.NextReadPage(context.Background(), page1.Next)
			if err != nil {
				t.Fatal(err)
			}
			active = page2.Next
			if requests.Load() != 2 {
				t.Fatalf("scan journey requests = %d, want 2", requests.Load())
			}
			if page2.Ordinal != 2 || page2.HasMore || page2.Next != nil || page2.Receipt.StopReason != ReadStopNativeEOF || len(page2.Items) != 1 {
				t.Fatal("native EOF scan page classification changed")
			}
			expectedItem := Item{
				"account":  s("tenant"),
				"sequence": n("9007199254740993.000"),
				"binary":   &types.AttributeValueMemberB{Value: []byte{0, 255, 128}},
				"ns":       &types.AttributeValueMemberNS{Value: []string{"1.0", "9007199254740993"}},
				"ss":       &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
				"bs":       &types.AttributeValueMemberBS{Value: [][]byte{{0, 255}, {1}}},
				"missing":  &types.AttributeValueMemberNULL{Value: true},
			}
			if !reflect.DeepEqual(page2.Items[0], expectedItem) {
				t.Fatal("official SDK number/binary/set/NULL fidelity changed")
			}
			r2 := page2.Receipt
			if r2.FetchedPages != 2 || r2.AckedPages != 2 || r2.ReceivedRows != 1 || r2.AckedRows != 1 || r2.ObservedScannedCount != 1 || r2.LogicalWork != 2 {
				t.Fatalf("final scan receipt changed: %+v", r2)
			}
			if !r2.OriginalExpiry.Equal(receipt1.OriginalExpiry) {
				t.Fatal("scan journey expiry was not stable across pages")
			}
			if tc.mode != CapacityTotal && tc.mode != CapacityIndexes {
				if page2.Receipt.Capacity != nil {
					t.Fatal("omitted/NONE exposed unsolicited capacity")
				}
				return
			}
			if receipt1.Capacity == nil || receipt1.Capacity.NativePages != 1 || receipt1.Capacity.ReportedPages != 1 ||
				receipt1.Capacity.CapacityUnits.ReportedTotal == nil || *receipt1.Capacity.CapacityUnits.ReportedTotal != 0 {
				t.Fatal("prior scan capacity snapshot was mutated")
			}
			cap2 := page2.Receipt.Capacity
			if cap2 == nil || cap2.Mode != tc.mode || cap2.NativePages != 2 || cap2.ReportedPages != 2 ||
				cap2.CapacityUnits.ReportedTotal == nil || *cap2.CapacityUnits.ReportedTotal != 0.5 || cap2.CapacityUnits.ReportedPages != 2 || !cap2.CapacityUnits.Complete ||
				cap2.Current == nil || cap2.Current.CapacityUnits == nil || *cap2.Current.CapacityUnits != 0.5 {
				t.Fatalf("final scan capacity receipt changed: %+v", cap2)
			}
			if tc.mode == CapacityIndexes {
				if cap2.ReadCapacityUnits.ReportedTotal == nil || *cap2.ReadCapacityUnits.ReportedTotal != 0.5 || cap2.ReadCapacityUnits.ReportedPages != 1 || cap2.ReadCapacityUnits.Complete ||
					cap2.WriteCapacityUnits.ReportedTotal != nil || cap2.WriteCapacityUnits.ReportedPages != 0 || cap2.WriteCapacityUnits.Complete ||
					cap2.Current.ReadCapacityUnits == nil || *cap2.Current.ReadCapacityUnits != 0.5 ||
					cap2.Current.Table == nil || cap2.Current.Table.CapacityUnits == nil || *cap2.Current.Table.CapacityUnits != 0.5 || cap2.Current.Table.ReadCapacityUnits == nil || *cap2.Current.Table.ReadCapacityUnits != 0.5 {
					t.Fatalf("final scan index capacity receipt changed: %+v", cap2)
				}
				lsi, lok := cap2.Current.LocalSecondaryIndexes["lsi-1"]
				gsi, gok := cap2.Current.GlobalSecondaryIndexes["gsi-1"]
				if !lok || !gok || lsi.CapacityUnits == nil || *lsi.CapacityUnits != 0.25 || lsi.ReadCapacityUnits == nil || *lsi.ReadCapacityUnits != 0.25 ||
					gsi.CapacityUnits == nil || *gsi.CapacityUnits != 0.25 || gsi.ReadCapacityUnits == nil || *gsi.ReadCapacityUnits != 0.25 {
					t.Fatalf("scan index breakdown changed: %+v", cap2)
				}
			} else if cap2.ReadCapacityUnits.ReportedTotal != nil || cap2.ReadCapacityUnits.ReportedPages != 0 || cap2.ReadCapacityUnits.Complete ||
				cap2.WriteCapacityUnits.ReportedTotal != nil || cap2.WriteCapacityUnits.ReportedPages != 0 || cap2.WriteCapacityUnits.Complete ||
				cap2.Current.ReadCapacityUnits != nil || cap2.Current.WriteCapacityUnits != nil ||
				cap2.Current.Table != nil || cap2.Current.LocalSecondaryIndexes != nil || cap2.Current.GlobalSecondaryIndexes != nil {
				t.Fatalf("aggregate TOTAL scan capacity receipt changed: %+v", cap2)
			}
		})
	}
}
