package dalgo2dynamodb

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

type fakeAPI struct {
	get   func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	query func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
}

func (f *fakeAPI) GetItem(ctx context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.get == nil {
		return nil, errors.New("unexpected Get")
	}
	return f.get(ctx, input)
}
func (f *fakeAPI) Query(ctx context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if f.query == nil {
		return nil, errors.New("unexpected Query")
	}
	return f.query(ctx, input)
}
func s(v string) types.AttributeValue   { return &types.AttributeValueMemberS{Value: v} }
func n(v string) types.AttributeValue   { return &types.AttributeValueMemberN{Value: v} }
func pk(v string) Item                  { return Item{"account": s(v)} }
func qitem(partition, sort string) Item { return Item{"account": s(partition), "sequence": n(sort)} }
func newTestClient(t *testing.T, api API, amend func(*Options)) *Client {
	t.Helper()
	options := Options{Tables: map[string]Table{"items": {Name: "existing-table", PartitionKey: "account"}, "events": {Name: "existing-events", PartitionKey: "account", SortKey: "sequence"}}}
	if amend != nil {
		amend(&options)
	}
	c, err := New(api, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("error classification: got %T, want code %s", err, code)
	}
	if strings.Contains(err.Error(), "secret") || errors.Unwrap(err) != nil {
		t.Fatal("provider detail escaped safe error")
	}
}

func TestNativeGetFidelityAndIsolation(t *testing.T) {
	binary := []byte{0, 255, 128}
	original := Item{"account": s("one"), "wide": n("9007199254740993.000"), "binary": &types.AttributeValueMemberB{Value: binary}, "strings": &types.AttributeValueMemberSS{Value: []string{"a", "b"}}, "numbers": &types.AttributeValueMemberNS{Value: []string{"1.0", "9007199254740993"}}, "binaries": &types.AttributeValueMemberBS{Value: [][]byte{{0, 255}, {1}}}, "nested": &types.AttributeValueMemberM{Value: Item{"list": &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberNULL{Value: true}, &types.AttributeValueMemberBOOL{Value: false}}}}}}
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if *input.TableName != "existing-table" || !reflect.DeepEqual(input.Key, pk("one")) {
			t.Error("existing table/native key changed")
		}
		return &dynamodb.GetItemOutput{Item: original}, nil
	}}, nil)
	got, err := c.Get(context.Background(), "items", pk("one"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, original) {
		t.Fatal("native union changed")
	}
	got["binary"].(*types.AttributeValueMemberB).Value[0] = 42
	got["strings"].(*types.AttributeValueMemberSS).Value[0] = "changed"
	delete(got["nested"].(*types.AttributeValueMemberM).Value, "list")
	if binary[0] != 0 || original["strings"].(*types.AttributeValueMemberSS).Value[0] != "a" || len(original["nested"].(*types.AttributeValueMemberM).Value) != 1 {
		t.Fatal("borrowed response mutated")
	}
}

func TestConstructorValidationAndNoIO(t *testing.T) {
	for _, api := range []API{nil, (*fakeAPI)(nil)} {
		if _, err := New(api, Options{}); err == nil {
			t.Fatal("nil borrowed client accepted")
		}
	}
	for _, amend := range []func(*Options){func(o *Options) { o.Timeout = -time.Second }, func(o *Options) { o.MaxConcurrent = 33 }, func(o *Options) { o.MaxRecords = -1 }, func(o *Options) { o.Tables["items"] = Table{Name: "secret", PartitionKey: "x", SortKey: "x"} }} {
		options := Options{Tables: map[string]Table{"items": {Name: "existing", PartitionKey: "account"}}}
		amend(&options)
		if _, err := New(&fakeAPI{}, options); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	c := newTestClient(t, &fakeAPI{}, nil)
	if c == nil {
		t.Fatal("constructor performed unexpected I/O")
	}
}

func TestGetManyAggregateBudgetStopsLaterDispatch(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		calls.Add(1)
		return &dynamodb.GetItemOutput{Item: in.Key}, nil
	}}, func(o *Options) { o.MaxParsedBytes = 65 })
	got, err := c.GetMany(context.Background(), "items", []Item{pk("one"), pk("two"), pk("tri")})
	requireCode(t, err, "limit")
	if got != nil || calls.Load() != 1 {
		t.Fatal("whole-operation budget reset or partial result escaped")
	}
}

func TestGetManyOrderMissingDuplicatesAndAdmission(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		calls.Add(1)
		if equalKeyValue(in.Key["account"], s("missing")) {
			return &dynamodb.GetItemOutput{}, nil
		}
		return &dynamodb.GetItemOutput{Item: in.Key}, nil
	}}, func(o *Options) { o.MaxRecords = 3 })
	got, err := c.GetMany(context.Background(), "items", []Item{pk("two"), pk("missing"), pk("two")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1] != nil || !equalKeyValue(got[0]["account"], s("two")) || !reflect.DeepEqual(got[0], got[2]) {
		t.Fatal("order/duplicates/absence changed")
	}
	before := calls.Load()
	_, err = c.GetMany(context.Background(), "items", []Item{pk("one"), {"wrong": s("secret")}})
	requireCode(t, err, "key")
	if calls.Load() != before {
		t.Fatal("invalid tail dispatched earlier key")
	}
	_, err = c.GetMany(context.Background(), "items", []Item{pk("one"), pk("one"), pk("one"), pk("one")})
	requireCode(t, err, "limit")
}

func TestNativeKeyTypesAndNormalizedNumberIdentity(t *testing.T) {
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		item := in.Key
		if _, ok := item["account"].(*types.AttributeValueMemberN); ok {
			item = Item{"account": n("1")}
		}
		return &dynamodb.GetItemOutput{Item: item}, nil
	}}, nil)
	for _, value := range []types.AttributeValue{n("1.000"), &types.AttributeValueMemberB{Value: []byte{0, 255}}} {
		got, err := c.Get(context.Background(), "items", Item{"account": value})
		if err != nil || got == nil {
			t.Fatalf("native key type rejected: %T %v", value, err)
		}
	}
	for _, value := range []types.AttributeValue{s(""), n("NaN"), n("1e999999"), &types.AttributeValueMemberBOOL{Value: true}, (*types.AttributeValueMemberS)(nil)} {
		_, err := c.Get(context.Background(), "items", Item{"account": value})
		requireCode(t, err, "key")
	}
}

func TestResultRefusalsAndBudgets(t *testing.T) {
	deep := types.AttributeValue(s("bottom"))
	for i := 0; i < 35; i++ {
		deep = &types.AttributeValueMemberL{Value: []types.AttributeValue{deep}}
	}
	for _, tc := range []struct {
		name  string
		item  Item
		code  string
		bytes int
	}{
		{"foreign-key", pk("foreign-secret"), "invalid-response", 0},
		{"nil-value", Item{"account": s("one"), "bad": nil}, "invalid-response", 0},
		{"duplicate-number-set", Item{"account": s("one"), "bad": &types.AttributeValueMemberNS{Value: []string{"1", "1.0"}}}, "invalid-response", 0},
		{"depth", Item{"account": s("one"), "bad": deep}, "limit", 0},
		{"bytes", Item{"account": s("one"), "bad": s(strings.Repeat("x", 1000))}, "limit", 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, &fakeAPI{get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{Item: tc.item}, nil
			}}, func(o *Options) { o.MaxParsedBytes = tc.bytes })
			_, err := c.Get(context.Background(), "items", pk("one"))
			requireCode(t, err, tc.code)
		})
	}
	c := newTestClient(t, &fakeAPI{get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return nil, errors.New("secret https://credential@example.test/token")
	}}, nil)
	_, err := c.Get(context.Background(), "items", pk("one"))
	requireCode(t, err, "transport")
}

func TestCancellationLateCompletionAndReopen(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	var calls atomic.Int32
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if calls.Add(1) == 1 {
			cancel(errors.New("secret cause"))
		}
		return &dynamodb.GetItemOutput{Item: in.Key}, nil
	}}, nil)
	got, err := c.Get(ctx, "items", pk("one"))
	if got != nil || !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
		t.Fatal("late success/cause escaped cancellation")
	}
	if _, err = c.Get(context.Background(), "items", pk("one")); err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	_, err = c.Get(ctx, "items", pk("one"))
	if !errors.Is(err, context.Canceled) || calls.Load() != before {
		t.Fatal("preabort dispatched")
	}
}

func TestUncooperativeBorrowedClientRetainsBoundedSlot(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	var once sync.Once
	var count atomic.Int32
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if count.Add(1) == 1 {
			close(started)
			<-release
			close(returned)
		}
		return &dynamodb.GetItemOutput{Item: in.Key}, nil
	}}, func(o *Options) { o.MaxConcurrent = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := c.Get(ctx, "items", pk("one")); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("SDK not dispatched")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("caller cancellation delayed")
	}
	deadline, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()
	_, err := c.Get(deadline, "items", pk("two"))
	if !errors.Is(err, context.DeadlineExceeded) || count.Load() != 1 {
		t.Fatal("abandoned SDK slot reused")
	}
	once.Do(func() { close(release) })
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("owned fake did not finish")
	}
	if _, err = c.Get(context.Background(), "items", pk("three")); err != nil {
		t.Fatal(err)
	}
}

func TestCloseCancelsOwnedContextAndStopsAdmissions(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	c := newTestClient(t, &fakeAPI{get: func(ctx context.Context, _ *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return nil, ctx.Err()
	}}, nil)
	done := make(chan error, 1)
	go func() { _, err := c.Get(context.Background(), "items", pk("one")); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("SDK did not start")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(750 * time.Millisecond):
		t.Fatal("active context survived Close")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("success after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("caller did not return")
	}
	_, err := c.Get(context.Background(), "items", pk("one"))
	requireCode(t, err, "closed")
}

type cancellingRecord struct {
	record.Record
	cancel context.CancelFunc
	target Item
}

func (r *cancellingRecord) SetError(err error) record.Record {
	r.Record.SetError(err)
	if err == nil && len(r.target) > 0 {
		r.cancel()
	}
	return r
}
func TestDALFacadeNativeTargetRetryAndFinalGuard(t *testing.T) {
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return &dynamodb.GetItemOutput{Item: in.Key}, nil
	}}, nil)
	target := Item{}
	r := record.NewRecordWithData(record.NewKeyWithID("items", "one"), target)
	r.SetError(errors.New("previous failure"))
	if err := c.DB().Get(context.Background(), r); err != nil || !r.Exists() || !equalKeyValue(target["account"], s("one")) {
		t.Fatal("record retry/native mapping failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	target = Item{}
	wrapped := &cancellingRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "one"), target), cancel: cancel, target: target}
	if err := c.DB().Get(ctx, wrapped); !errors.Is(err, context.Canceled) || !errors.Is(wrapped.Error(), context.Canceled) {
		t.Fatal("record callback late success")
	}
	var item Item
	composite := record.NewKeyWithFields("events", record.FieldVal{Name: "account", Value: s("tenant")}, record.FieldVal{Name: "sequence", Value: n("9007199254740993")})
	if err := c.DB().Get(context.Background(), record.NewRecordWithData(composite, &item)); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := c.DB().RunReadonlyTransaction(context.Background(), func(context.Context, dal.ReadTransaction) error { called = true; return nil }); !errors.Is(err, dal.ErrNotSupported) || called {
		t.Fatal("invented transaction")
	}
}

type deadlineRecord struct {
	record.Record
	ctx     context.Context
	target  Item
	entered bool
}

func (r *deadlineRecord) SetError(err error) record.Record {
	r.Record.SetError(err)
	if err == nil && len(r.target) > 0 {
		r.entered = true
		<-r.ctx.Done()
	}
	return r
}
func TestDALFinalRecordCallbackDeadlineAndMultiState(t *testing.T) {
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if equalKeyValue(in.Key["account"], s("missing")) {
			return &dynamodb.GetItemOutput{}, nil
		}
		return &dynamodb.GetItemOutput{Item: in.Key}, nil
	}}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	target := Item{}
	r := &deadlineRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "one"), target), ctx: ctx, target: target}
	if err := c.DB().Get(ctx, r); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(r.Error(), context.DeadlineExceeded) {
		t.Fatal("final callback deadline delivered success")
	}
	if !r.entered {
		t.Fatal("deadline expired before target-populated final callback was entered")
	}
	makeRecord := func(id string) record.Record {
		return record.NewRecordWithData(record.NewKeyWithID("items", id), Item{})
	}
	records := []record.Record{makeRecord("two"), makeRecord("missing"), makeRecord("two")}
	if err := c.DB().GetMulti(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if !records[0].Exists() || records[1].Exists() || !records[2].Exists() {
		t.Fatal("GetMulti duplicate/missing states changed")
	}
	invalid := record.NewRecordWithData(record.NewKeyWithID("items", "bad"), &struct{}{})
	records = []record.Record{makeRecord("one"), invalid}
	if err := c.DB().GetMulti(context.Background(), records); err == nil {
		t.Fatal("unsupported data target accepted")
	}
	for _, r := range records {
		if r.Error() == nil {
			t.Fatal("GetMulti failed preflight left success state")
		}
	}
}

type switchingRecord struct {
	record.Record
	current *record.Key
	number  *types.AttributeValueMemberN
	other   *record.Key
}

func (r *switchingRecord) Key() *record.Key { return r.current }
func (r *switchingRecord) SetError(err error) record.Record {
	r.Record.SetError(err)
	if err == nil {
		r.current = r.other
		r.number.Value = "9007199254740994"
	}
	return r
}
func TestDALCapturesOriginalCollectionAndNativeKeyBeforeCallbacks(t *testing.T) {
	for _, multi := range []bool{false, true} {
		name := "Get"
		if multi {
			name = "GetMulti"
		}
		t.Run(name, func(t *testing.T) {
			originalNumber := &types.AttributeValueMemberN{Value: "9007199254740993"}
			original := record.NewKeyWithFields("events", record.FieldVal{Name: "account", Value: s("tenant")}, record.FieldVal{Name: "sequence", Value: originalNumber})
			other := record.NewKeyWithFields("other", record.FieldVal{Name: "account", Value: s("tenant")}, record.FieldVal{Name: "sequence", Value: n("9007199254740994")})
			r := &switchingRecord{Record: record.NewRecordWithData(original, Item{}), current: original, number: originalNumber, other: other}
			c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
				if *in.TableName != "existing-events" || !equalKeyValue(in.Key["sequence"], n("9007199254740993")) {
					t.Error("record callback redirected original native read")
				}
				return &dynamodb.GetItemOutput{Item: in.Key}, nil
			}}, func(o *Options) {
				o.Tables["other"] = Table{Name: "other-table", PartitionKey: "account", SortKey: "sequence"}
			})
			var err error
			if multi {
				err = c.DB().GetMulti(context.Background(), []record.Record{r})
			} else {
				err = c.DB().Get(context.Background(), r)
			}
			if err != nil {
				t.Fatal(err)
			}
			if originalNumber.Value != "9007199254740994" || r.Key().Collection() != "other" {
				t.Fatal("callback mutation did not occur")
			}
		})
	}
}

func TestDALMultiCapturesAllKeysBeforeFirstRecordCallback(t *testing.T) {
	secondNumber := &types.AttributeValueMemberN{Value: "2"}
	firstKey := record.NewKeyWithFields("events", record.FieldVal{Name: "account", Value: s("tenant")}, record.FieldVal{Name: "sequence", Value: n("1")})
	secondKey := record.NewKeyWithFields("events", record.FieldVal{Name: "account", Value: s("tenant")}, record.FieldVal{Name: "sequence", Value: secondNumber})
	first := &switchingRecord{Record: record.NewRecordWithData(firstKey, Item{}), current: firstKey, number: secondNumber, other: firstKey}
	second := record.NewRecordWithData(secondKey, Item{})
	calls := 0
	c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		calls++
		expected := n("1")
		if calls == 2 {
			expected = n("2")
		}
		if !equalKeyValue(in.Key["sequence"], expected) {
			t.Error("earlier record callback changed a later request key")
		}
		return &dynamodb.GetItemOutput{Item: in.Key}, nil
	}}, nil)
	if err := c.DB().GetMulti(context.Background(), []record.Record{first, second}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || secondNumber.Value != "9007199254740994" {
		t.Fatal("cross-record mutation regression not exercised")
	}
}

type sliceMutatingRecord struct {
	record.Record
	mutate func()
}

func (r *sliceMutatingRecord) SetError(err error) record.Record {
	r.Record.SetError(err)
	if err == nil {
		r.mutate()
	}
	return r
}
func TestDALMultiSnapshotsCallerRecordSliceBeforeCallbacks(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "nil"
		if replace {
			name = "replacement"
		}
		t.Run(name, func(t *testing.T) {
			originalTarget := Item{}
			originalSecond := record.NewRecordWithData(record.NewKeyWithID("items", "two"), originalTarget)
			replacementTarget := Item{}
			replacement := record.NewRecordWithData(record.NewKeyWithID("items", "other"), replacementTarget)
			records := []record.Record{nil, originalSecond}
			first := &sliceMutatingRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "one"), Item{}), mutate: func() {
				if replace {
					records[1] = replacement
				} else {
					records[1] = nil
				}
			}}
			records[0] = first
			c := newTestClient(t, &fakeAPI{get: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{Item: in.Key}, nil
			}}, nil)
			if err := c.DB().GetMulti(context.Background(), records); err != nil {
				t.Fatal(err)
			}
			if !originalSecond.Exists() || !equalKeyValue(originalTarget["account"], s("two")) {
				t.Fatal("original admitted target was lost")
			}
			if len(replacementTarget) != 0 {
				t.Fatal("captured key delivered into substituted record")
			}
			if replace && records[1] != replacement {
				t.Fatal("replacement callback not exercised")
			}
			if !replace && records[1] != nil {
				t.Fatal("nil callback not exercised")
			}
		})
	}
}

type cancellingErrorRecord struct {
	record.Record
	cancel context.CancelFunc
}

func (r *cancellingErrorRecord) SetError(err error) record.Record {
	r.Record.SetError(err)
	if typed, ok := err.(*Error); ok && typed.Code == "transport" {
		r.cancel()
	}
	return r
}
func TestDALMultiErrorRestorationCallbackPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := &cancellingErrorRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "one"), Item{}), cancel: cancel}
	second := record.NewRecordWithData(record.NewKeyWithID("items", "two"), Item{})
	c := newTestClient(t, &fakeAPI{get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		return nil, errors.New("secret provider cause")
	}}, nil)
	if err := c.DB().GetMulti(ctx, []record.Record{first, second}); !errors.Is(err, context.Canceled) {
		t.Fatal("restoration callback cancellation lost")
	}
	if !errors.Is(first.Error(), context.Canceled) || !errors.Is(second.Error(), context.Canceled) {
		t.Fatal("admitted records retained stale error classification")
	}
}
