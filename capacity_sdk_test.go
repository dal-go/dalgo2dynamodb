package dalgo2dynamodb

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestOfficialSDKQueryCapacityOwnedTLS(t *testing.T) {
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
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := requests.Add(1)
				raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8192))
				if err != nil {
					t.Error("query body not fully consumed")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if n > 2 {
					t.Error("unexpected extra query")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/" || r.URL.RawQuery != "" || r.TLS == nil ||
					!strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") ||
					r.Header.Get("X-Amz-Date") == "" || r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.Query" {
					t.Error("unsigned or unexpected query transport")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var body map[string]json.RawMessage
				if err := json.Unmarshal(raw, &body); err != nil {
					t.Error("query body is not one closed JSON object")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				expectedKeys := 7
				if tc.mode != "" {
					expectedKeys++
				}
				if n == 2 {
					expectedKeys++
				}
				if len(body) != expectedKeys {
					t.Errorf("query field count = %d, want %d", len(body), expectedKeys)
				}
				var table, expression string
				if json.Unmarshal(body["TableName"], &table) != nil || table != "existing-events" {
					t.Error("physical table changed")
				}
				if json.Unmarshal(body["KeyConditionExpression"], &expression) != nil || expression != "#p = :p" {
					t.Error("partition expression changed")
				}
				var names map[string]string
				if json.Unmarshal(body["ExpressionAttributeNames"], &names) != nil || names["#p"] != "account" {
					t.Error("partition name binding changed")
				}
				var values map[string]map[string]string
				if json.Unmarshal(body["ExpressionAttributeValues"], &values) != nil || values[":p"]["S"] != "tenant" {
					t.Error("partition value binding changed")
				}
				var limit int
				var forward, consistent bool
				if json.Unmarshal(body["Limit"], &limit) != nil || limit != 2 {
					t.Error("wire limit changed")
				}
				if json.Unmarshal(body["ScanIndexForward"], &forward) != nil || !forward {
					t.Error("scan direction changed")
				}
				if json.Unmarshal(body["ConsistentRead"], &consistent) != nil || consistent {
					t.Error("consistent read changed")
				}
				for _, forbidden := range []string{"FilterExpression", "IndexName", "ProjectionExpression", "Select", "AttributesToGet", "KeyConditions", "QueryFilter", "ConditionalOperator", "ReturnValues", "ScanFilter"} {
					if _, ok := body[forbidden]; ok {
						t.Errorf("unexpected query field %s", forbidden)
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
						t.Error("first page started from a cursor")
					}
					_, _ = w.Write([]byte(`{"Count":0,"ScannedCount":0,"LastEvaluatedKey":{"account":{"S":"tenant"},"sequence":{"N":"0"}},"ConsumedCapacity":{"TableName":"existing-events","CapacityUnits":0}}`))
					return
				}
				var start map[string]map[string]string
				if json.Unmarshal(body["ExclusiveStartKey"], &start) != nil || start["account"]["S"] != "tenant" || start["sequence"]["N"] != "0" {
					t.Error("second page cursor changed")
				}
				_, _ = w.Write([]byte(`{"Items":[{"account":{"S":"tenant"},"sequence":{"N":"9007199254740993.000"}}],"Count":1,"ScannedCount":1,"ConsumedCapacity":{"TableName":"existing-events","CapacityUnits":0.5,"ReadCapacityUnits":0.5,"Table":{"CapacityUnits":0.5,"ReadCapacityUnits":0.5},"LocalSecondaryIndexes":{"lsi-1":{"CapacityUnits":0.25,"ReadCapacityUnits":0.25}},"GlobalSecondaryIndexes":{"gsi-1":{"CapacityUnits":0.25,"ReadCapacityUnits":0.25}}}}`))
			}))
			t.Cleanup(server.Close)
			sdk := sdkFor(server, ownedCredentials{retrieve: func(context.Context) (aws.Credentials, error) { return fixtureCredentials(), nil }})
			c := newTestClient(t, sdk, func(o *Options) { o.PageSize = 2 })
			page, err := c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 2, ReturnConsumedCapacity: tc.mode})
			if err != nil {
				t.Fatal("official SDK query failed")
			}
			if requests.Load() != 2 {
				t.Fatalf("query requests = %d, want 2", requests.Load())
			}
			if page.Pages != 2 || page.Evaluated != 2 || len(page.Items) != 1 {
				t.Fatalf("page traversal: pages=%d evaluated=%d items=%d", page.Pages, page.Evaluated, len(page.Items))
			}
			sequence, ok := page.Items[0]["sequence"].(*types.AttributeValueMemberN)
			if !ok || sequence.Value != "9007199254740993.000" {
				t.Fatal("native wide number changed")
			}
			if tc.mode != CapacityTotal && tc.mode != CapacityIndexes {
				if page.Capacity != nil {
					t.Fatal("default/NONE exposed unsolicited capacity")
				}
				return
			}
			receipt := page.Capacity
			if receipt == nil || receipt.Mode != tc.mode || receipt.NativePages != 2 || receipt.ReportedPages != 2 || receipt.ObservedScannedCount != 1 || len(receipt.Pages) != 2 {
				t.Fatalf("capacity receipt summary changed: %+v", receipt)
			}
			first, second := receipt.Pages[0], receipt.Pages[1]
			if first.Ordinal != 1 || first.ScannedCount != 0 || first.ConsumedCapacity == nil || first.ConsumedCapacity.CapacityUnits == nil || *first.ConsumedCapacity.CapacityUnits != 0 {
				t.Fatalf("explicit zero first page changed: %+v", first)
			}
			if second.Ordinal != 2 || second.ScannedCount != 1 || second.ConsumedCapacity == nil {
				t.Fatalf("second page capacity changed: %+v", second)
			}
			sum := receipt.CapacityUnits
			if sum.ReportedTotal == nil || *sum.ReportedTotal != 0.5 || sum.ReportedPages != 2 || !sum.Complete {
				t.Fatalf("top capacity sum changed: %+v", sum)
			}
			read := receipt.ReadCapacityUnits
			if read.ReportedTotal == nil || *read.ReportedTotal != 0.5 || read.ReportedPages != 1 || read.Complete {
				t.Fatalf("read coverage changed: %+v", read)
			}
			write := receipt.WriteCapacityUnits
			if write.ReportedTotal != nil || write.ReportedPages != 0 || write.Complete {
				t.Fatalf("write coverage changed: %+v", write)
			}
			detail := second.ConsumedCapacity
			if detail.CapacityUnits == nil || *detail.CapacityUnits != 0.5 || detail.ReadCapacityUnits == nil || *detail.ReadCapacityUnits != 0.5 ||
				detail.Table == nil || detail.Table.CapacityUnits == nil || *detail.Table.CapacityUnits != 0.5 ||
				detail.Table.ReadCapacityUnits == nil || *detail.Table.ReadCapacityUnits != 0.5 {
				t.Fatalf("table breakdown changed: %+v", detail)
			}
			if tc.mode == CapacityIndexes {
				lsi, lok := detail.LocalSecondaryIndexes["lsi-1"]
				gsi, gok := detail.GlobalSecondaryIndexes["gsi-1"]
				if !lok || !gok || lsi.CapacityUnits == nil || *lsi.CapacityUnits != 0.25 || lsi.ReadCapacityUnits == nil || *lsi.ReadCapacityUnits != 0.25 ||
					gsi.CapacityUnits == nil || *gsi.CapacityUnits != 0.25 || gsi.ReadCapacityUnits == nil || *gsi.ReadCapacityUnits != 0.25 {
					t.Fatalf("index breakdown changed: %+v", detail)
				}
			}
		})
	}
}
