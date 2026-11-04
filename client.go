// Package dalgo2dynamodb reads explicitly mapped existing DynamoDB tables.
package dalgo2dynamodb

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// API is satisfied by the official SDK v2 client. The adapter borrows it and
// never closes its HTTP transport or destroys credentials. No writes or Scan.
type API interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// Item retains the official AttributeValue union, including decimal number
// strings, binary values, sets, nested maps/lists and explicit NULL.
type Item = map[string]types.AttributeValue

// Table maps a DALgo collection to an EXISTING table and actual key names.
// Credential IAM policies remain authoritative; this mapping grants no access.
type Table struct {
	Name           string
	PartitionKey   string
	SortKey        string
	ConsistentRead bool
}

// Options bounds each public operation, including all GetMany/Query pages.
// Parsed bytes are measured after SDK decoding, NOT at the HTTP wire boundary.
type Options struct {
	Tables         map[string]Table
	Timeout        time.Duration
	MaxConcurrent  int
	MaxRecords     int
	MaxParsedBytes int
	MaxPages       int
	MaxEvaluated   int
	PageSize       int
	Journeys       *JourneyOptions
}

// Error has a safe, fixed code and never wraps provider errors or signal causes.
type Error struct{ Code string }

func (e *Error) Error() string  { return "DynamoDB read failed: " + e.Code }
func failure(code string) error { return &Error{Code: code} }

// Client owns only local admission and cancellation state, not the borrowed SDK.
// An SDK ignoring context retains its slot until it actually returns. This
// bounds outstanding SDK calls, rather than creating unlimited detached work.
type Client struct {
	api       API
	scanAPI   ScanAPI
	options   Options
	semaphore chan struct{}
	life      context.Context
	cancel    context.CancelFunc
	once      sync.Once
	registry  readJourneyRegistry
}

// New does no credential, network, table discovery or schema I/O.
func New(api API, options Options) (*Client, error) {
	if api == nil || (reflect.ValueOf(api).Kind() == reflect.Pointer && reflect.ValueOf(api).IsNil()) {
		return nil, failure("configuration")
	}
	if options.Timeout == 0 {
		options.Timeout = 30 * time.Second
	}
	if options.MaxConcurrent == 0 {
		options.MaxConcurrent = 4
	}
	if options.MaxRecords == 0 {
		options.MaxRecords = 1000
	}
	if options.MaxParsedBytes == 0 {
		options.MaxParsedBytes = 4_000_000
	}
	if options.MaxPages == 0 {
		options.MaxPages = 100
	}
	if options.MaxEvaluated == 0 {
		options.MaxEvaluated = 10_000
	}
	if options.PageSize == 0 {
		options.PageSize = 100
	}
	if options.Timeout < time.Millisecond || options.Timeout > 2*time.Minute || options.MaxConcurrent < 1 || options.MaxConcurrent > 32 || options.MaxRecords < 1 || options.MaxRecords > 10_000 || options.MaxParsedBytes < 1 || options.MaxParsedBytes > 16_000_000 || options.MaxPages < 1 || options.MaxPages > 1000 || options.MaxEvaluated < 1 || options.MaxEvaluated > 100_000 || options.PageSize < 1 || options.PageSize > 1000 || len(options.Tables) == 0 || len(options.Tables) > 128 {
		return nil, failure("configuration")
	}
	tables := make(map[string]Table, len(options.Tables))
	for collection, table := range options.Tables {
		if !validName(collection) || !validName(table.Name) || !validName(table.PartitionKey) || (table.SortKey != "" && !validName(table.SortKey)) || table.PartitionKey == table.SortKey {
			return nil, failure("configuration")
		}
		tables[collection] = table
	}
	options.Tables = tables
	journeys, err := copyJourneyOptions(options.Journeys, tables)
	if err != nil {
		return nil, err
	}
	options.Journeys = journeys
	life, cancel := context.WithCancel(context.Background())
	scanAPI, _ := api.(ScanAPI)
	return &Client{api: api, scanAPI: scanAPI, options: options, semaphore: make(chan struct{}, options.MaxConcurrent), life: life, cancel: cancel}, nil
}

func validName(s string) bool { return s != "" && len(s) <= 255 && utf8.ValidString(s) }

// Close stops new admissions and cancels active SDK contexts, idempotently.
// It cannot force an uncooperative borrowed SDK to return, and never closes it.
func (c *Client) Close() error {
	c.once.Do(func() {
		c.closeReadJourneys()
		c.cancel()
	})
	return nil
}

type operation struct {
	client    *Client
	ctx       context.Context
	cancel    context.CancelFunc
	stop      func() bool
	retain    func() (func(), error)
	bytes     int
	nodes     int
	byteLimit int
	nodeLimit int
	once      sync.Once
}

func (c *Client) begin(ctx context.Context) (*operation, error) {
	if ctx == nil {
		return nil, failure("input")
	}
	if err := active(ctx); err != nil {
		return nil, err
	}
	if c.life.Err() != nil {
		return nil, failure("closed")
	}
	bounded, cancel := context.WithTimeout(ctx, c.options.Timeout)
	o := &operation{client: c, ctx: bounded, cancel: cancel}
	o.stop = context.AfterFunc(c.life, cancel)
	if err := o.check(); err != nil {
		o.close()
		return nil, err
	}
	return o, nil
}
func active(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		return context.Canceled
	}
	// Synchronous parsing/mapping can cross a deadline before its timer runs.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
func (o *operation) check() error {
	if err := active(o.ctx); err != nil {
		return err
	}
	if o.client.life.Err() != nil {
		return failure("closed")
	}
	return nil
}
func (o *operation) close() {
	o.once.Do(func() {
		o.stop()
		o.cancel()
	})
}
func (o *operation) charge(n int) error {
	if err := o.check(); err != nil {
		return err
	}
	if n < 0 || n > o.client.options.MaxParsedBytes-o.bytes {
		return failure("limit")
	}
	if o.byteLimit > 0 && n > o.byteLimit-o.bytes {
		return failure("limit")
	}
	if o.nodeLimit > 0 && o.nodes >= o.nodeLimit {
		return failure("limit")
	}
	o.bytes += n
	o.nodes++
	if o.nodes > 100_000 {
		return failure("limit")
	}
	return nil
}

type completion[T any] struct {
	value T
	err   error
}

func invoke[T any](o *operation, call func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := o.check(); err != nil {
		return zero, err
	}
	select {
	case o.client.semaphore <- struct{}{}:
	case <-o.ctx.Done():
		return zero, active(o.ctx)
	}
	if err := o.check(); err != nil {
		<-o.client.semaphore
		return zero, err
	}
	release := func() {}
	if o.retain != nil {
		var err error
		release, err = o.retain()
		if err != nil {
			<-o.client.semaphore
			if checkErr := o.check(); checkErr != nil {
				return zero, checkErr
			}
			return zero, err
		}
		if release == nil {
			release = func() {}
		}
	}
	if err := o.check(); err != nil {
		release()
		<-o.client.semaphore
		return zero, err
	}
	done := make(chan completion[T], 1)
	go func() {
		value, err := call(o.ctx)
		// Retained until the borrowed SDK actually returns, even if the caller
		// already gave up, so no logical charge is released while it still runs.
		release()
		// No public callbacks or result mutation after caller cancellation.
		<-o.client.semaphore
		done <- completion[T]{value: value, err: err}
	}()
	select {
	case <-o.ctx.Done():
		return zero, active(o.ctx)
	case result := <-done:
		if err := o.check(); err != nil {
			return zero, err
		}
		if result.err != nil {
			return zero, failure("transport")
		}
		return result.value, nil
	}
}

func (c *Client) table(collection string) (Table, error) {
	table, ok := c.options.Tables[collection]
	if !ok {
		return Table{}, failure("mapping")
	}
	return table, nil
}

// Get reads one exact native key. An absent item is nil, without an error.
func (c *Client) Get(ctx context.Context, collection string, key Item) (Item, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.close()
	item, err := c.get(o, collection, key)
	if err == nil {
		err = o.check()
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}
func (c *Client) get(o *operation, collection string, key Item) (Item, error) {
	table, err := c.table(collection)
	if err != nil {
		return nil, err
	}
	admitted, err := cloneKey(o, table, key)
	if err != nil {
		return nil, err
	}
	output, err := invoke(o, func(ctx context.Context) (*dynamodb.GetItemOutput, error) {
		return c.api.GetItem(ctx, &dynamodb.GetItemInput{TableName: &table.Name, Key: admitted, ConsistentRead: &table.ConsistentRead})
	})
	if err != nil {
		return nil, err
	}
	if output == nil {
		return nil, failure("invalid-response")
	}
	if len(output.Item) == 0 {
		return nil, o.check()
	}
	item, err := cloneItem(o, output.Item, 0)
	if err != nil {
		return nil, err
	}
	for name, value := range admitted {
		if !equalKeyValue(value, item[name]) {
			return nil, failure("invalid-response")
		}
	}
	return item, o.check()
}
func (c *Client) Exists(ctx context.Context, collection string, key Item) (bool, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return false, err
	}
	defer o.close()
	item, err := c.get(o, collection, key)
	if err == nil {
		err = o.check()
	}
	if err != nil {
		return false, err
	}
	return item != nil, nil
}

// GetMany is sequential, preserves order/duplicates/missing nil items, and
// shares ONE deadline and aggregate parsed-byte budget across every request.
func (c *Client) GetMany(ctx context.Context, collection string, keys []Item) ([]Item, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.close()
	items, err := c.getMany(o, collection, keys)
	if err == nil {
		err = o.check()
	}
	if err != nil {
		return nil, err
	}
	return items, nil
}
func (c *Client) getMany(o *operation, collection string, keys []Item) ([]Item, error) {
	if len(keys) > c.options.MaxRecords {
		return nil, failure("limit")
	}
	table, err := c.table(collection)
	if err != nil {
		return nil, err
	}
	// Validate all keys BEFORE any dispatch; no partial request on bad input.
	admitted := make([]Item, len(keys))
	for i, key := range keys {
		admitted[i], err = cloneKey(o, table, key)
		if err != nil {
			return nil, err
		}
	}
	items := make([]Item, len(keys))
	for i, key := range admitted {
		items[i], err = c.get(o, collection, key)
		if err != nil {
			return nil, err
		}
	}
	return items, o.check()
}
