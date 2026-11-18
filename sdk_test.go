package dalgo2dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ownedCredentials struct {
	retrieve func(context.Context) (aws.Credentials, error)
}

func (c ownedCredentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return c.retrieve(ctx)
}
func fixtureCredentials() aws.Credentials {
	return aws.Credentials{AccessKeyID: "owned-fixture-access", SecretAccessKey: "owned-fixture-secret", Source: "synthetic"}
}
func sdkFor(server *httptest.Server, credentials aws.CredentialsProvider) *dynamodb.Client {
	return dynamodb.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials, HTTPClient: server.Client()}, func(o *dynamodb.Options) { o.BaseEndpoint = &server.URL; o.RetryMaxAttempts = 1 })
}

func TestOfficialSDKOwnedTLSExistingTableContract(t *testing.T) {
	var gets, queries atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/" || r.URL.RawQuery != "" || !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") || r.Header.Get("X-Amz-Date") == "" || r.TLS == nil {
			t.Error("official signed TLS contract changed")
			w.WriteHeader(400)
			return
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192)).Decode(&body); err != nil {
			t.Error("invalid SDK JSON body")
			w.WriteHeader(400)
			return
		}
		var table string
		if err := json.Unmarshal(body["TableName"], &table); err != nil {
			t.Error("table absent")
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch r.Header.Get("X-Amz-Target") {
		case "DynamoDB_20120810.GetItem":
			gets.Add(1)
			if table != "existing-table" {
				t.Error("hidden table layout invented")
			}
			var key map[string]map[string]string
			if err := json.Unmarshal(body["Key"], &key); err != nil || key["account"]["S"] != "one" {
				t.Error("native existing key changed")
			}
			_, _ = w.Write([]byte(`{"Item":{"account":{"S":"one"},"wide":{"N":"9007199254740993.000"},"binary":{"B":"AP+A"},"ns":{"NS":["1.0","9007199254740993"]},"ss":{"SS":["a","b"]},"bs":{"BS":["AP8=","AQ=="]},"nested":{"M":{"list":{"L":[{"NULL":true},{"BOOL":false}]}}}}}`))
		case "DynamoDB_20120810.Query":
			queries.Add(1)
			if table != "existing-events" || body["FilterExpression"] != nil || body["IndexName"] != nil {
				t.Error("unexpected query expansion")
			}
			var expression string
			if err := json.Unmarshal(body["KeyConditionExpression"], &expression); err != nil || expression != "#p = :p" {
				t.Error("query partition expression changed")
			}
			var values map[string]map[string]string
			if err := json.Unmarshal(body["ExpressionAttributeValues"], &values); err != nil || values[":p"]["S"] != "tenant" {
				t.Error("query partition parameter changed")
			}
			var limit int
			if err := json.Unmarshal(body["Limit"], &limit); err != nil || limit != 1 {
				t.Error("query limit changed")
			}
			_, _ = w.Write([]byte(`{"Items":[{"account":{"S":"tenant"},"sequence":{"N":"9007199254740993"}}],"Count":1,"ScannedCount":1}`))
		default:
			t.Error("non-read operation dispatched")
			w.WriteHeader(400)
		}
	}))
	t.Cleanup(server.Close)
	sdk := sdkFor(server, ownedCredentials{retrieve: func(context.Context) (aws.Credentials, error) { return fixtureCredentials(), nil }})
	c := newTestClient(t, sdk, nil)
	got, err := c.Get(context.Background(), "items", pk("one"))
	if err != nil {
		t.Fatal(err)
	}
	if got["wide"].(*types.AttributeValueMemberN).Value != "9007199254740993.000" || !reflect.DeepEqual(got["binary"].(*types.AttributeValueMemberB).Value, []byte{0, 255, 128}) {
		t.Fatal("official SDK number/binary fidelity failed")
	}
	expectedSets := Item{
		"ns": &types.AttributeValueMemberNS{Value: []string{"1.0", "9007199254740993"}},
		"ss": &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
		"bs": &types.AttributeValueMemberBS{Value: [][]byte{{0, 255}, {1}}},
		"nested": &types.AttributeValueMemberM{Value: Item{
			"list": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberNULL{Value: true},
				&types.AttributeValueMemberBOOL{Value: false},
			}},
		}},
	}
	for name, expected := range expectedSets {
		if !reflect.DeepEqual(got[name], expected) {
			t.Fatalf("official SDK native %s changed", name)
		}
	}
	if _, err = c.QueryPartition(context.Background(), QueryRequest{Collection: "events", Partition: s("tenant"), Limit: 1}); err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 1 || queries.Load() != 1 {
		t.Fatal("unexpected SDK operations")
	}
	// Borrowed SDK stays usable after adapter Close; no transport destruction.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sdk.GetItem(context.Background(), &dynamodb.GetItemInput{TableName: aws.String("existing-table"), Key: pk("one")}); err != nil {
		t.Fatal("borrowed SDK was closed")
	}
}

func TestOfficialSDKDispatchedCallerDeadlineDisconnectAndReopen(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "caller"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			dispatched := make(chan struct{}, 1)
			disconnected := make(chan struct{}, 1)
			cleanup := make(chan struct{})
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var in struct{ Key map[string]map[string]string }
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Error("bad request")
					return
				}
				if in.Key["account"]["S"] == "slow" {
					dispatched <- struct{}{}
					select {
					case <-r.Context().Done():
						disconnected <- struct{}{}
					case <-cleanup:
					}
					return
				}
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				_, _ = w.Write([]byte(`{"Item":{"account":{"S":"one"}}}`))
			}))
			t.Cleanup(server.Close)
			t.Cleanup(func() { close(cleanup) })
			c := newTestClient(t, sdkFor(server, ownedCredentials{retrieve: func(context.Context) (aws.Credentials, error) { return fixtureCredentials(), nil }}), func(o *Options) { o.Timeout = 500 * time.Millisecond })
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			done := make(chan error, 1)
			go func() { _, err := c.Get(ctx, "items", pk("slow")); done <- err }()
			select {
			case <-dispatched:
			case <-time.After(2 * time.Second):
				t.Fatal("no actual server dispatch")
			}
			start := time.Now()
			if !deadline {
				cancel(errors.New("secret caller reason"))
			}
			select {
			case err := <-done:
				expected := context.Canceled
				if deadline {
					expected = context.DeadlineExceeded
				}
				if !errors.Is(err, expected) || strings.Contains(err.Error(), "secret") {
					t.Fatal("cancellation classification/redaction changed")
				}
			case <-time.After(750 * time.Millisecond):
				t.Fatal("dispatched cancellation exceeded750ms")
			}
			if time.Since(start) > 750*time.Millisecond {
				t.Fatal("late cancellation")
			}
			select {
			case <-disconnected:
			case <-time.After(2 * time.Second):
				t.Fatal("official HTTP request did not disconnect")
			}
			if _, err := c.Get(context.Background(), "items", pk("one")); err != nil {
				t.Fatal("read after cancellation failed")
			}
		})
	}
}

func TestOfficialSDKCredentialWaitAndProviderErrorRedaction(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(403)
		_, _ = w.Write([]byte(`{"__type":"AccessDeniedException","message":"secret token endpoint cause"}`))
	}))
	t.Cleanup(server.Close)
	started := make(chan struct{})
	credentials := ownedCredentials{retrieve: func(ctx context.Context) (aws.Credentials, error) {
		close(started)
		<-ctx.Done()
		return aws.Credentials{}, errors.New("secret credential cause")
	}}
	c := newTestClient(t, sdkFor(server, credentials), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.Get(ctx, "items", pk("one")); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("credential not acquired")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("credential cancellation delayed")
	}
	if requests.Load() != 0 {
		t.Fatal("credential wait dispatched request")
	}
	read := newTestClient(t, sdkFor(server, ownedCredentials{retrieve: func(context.Context) (aws.Credentials, error) { return fixtureCredentials(), nil }}), nil)
	_, err := read.Get(context.Background(), "items", pk("one"))
	requireCode(t, err, "transport")
}

func TestOfficialSDKPreabortDoesNotAcquireCredentials(t *testing.T) {
	var credentials, requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	t.Cleanup(server.Close)
	c := newTestClient(t, sdkFor(server, ownedCredentials{retrieve: func(context.Context) (aws.Credentials, error) { credentials.Add(1); return fixtureCredentials(), nil }}), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Get(ctx, "items", pk("one"))
	if !errors.Is(err, context.Canceled) || credentials.Load() != 0 || requests.Load() != 0 {
		t.Fatal("preabort acquired credentials or dispatched")
	}
}
