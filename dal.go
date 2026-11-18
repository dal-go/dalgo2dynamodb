package dalgo2dynamodb

import (
	"context"
	"maps"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

// DB exposes the same existing-table reads through DALgo's sealed facade.
// Data targets must be Item or *Item: no JSON/float64 coercion of native values.
// Generic structured queries, writes, transactions and schema are not implemented.
func (c *Client) DB() dal.DB { return dal.NewDB(&backend{client: c}) }

type backend struct {
	dal.ConcurrencyAvailable
	client *Client
}

var _ dal.Backend = (*backend)(nil)

func (*backend) ID() string           { return "dynamodb:mapped" }
func (*backend) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2dynamodb", "unreleased") }
func (*backend) Schema() dal.Schema   { return nil }

func nativeRecordKey(c *Client, key *record.Key) (Item, error) {
	if key == nil || key.Parent() != nil {
		return nil, failure("key")
	}
	table, err := c.table(key.Collection())
	if err != nil {
		return nil, err
	}
	result := Item{}
	switch id := key.ID.(type) {
	case string:
		if table.SortKey != "" {
			return nil, failure("key")
		}
		result[table.PartitionKey] = &types.AttributeValueMemberS{Value: id}
	case []record.FieldVal:
		for _, field := range id {
			if _, duplicate := result[field.Name]; duplicate {
				return nil, failure("key")
			}
			value, ok := field.Value.(types.AttributeValue)
			if !ok {
				return nil, failure("key")
			}
			result[field.Name] = value
		}
	default:
		return nil, failure("key")
	}
	return result, nil
}

func dataTarget(r record.Record) (Item, *Item, error) {
	// Record.Data panics after an earlier error. Clear the retrieval state before
	// inspecting its target, then always restore the actual operation outcome.
	r.SetError(nil)
	switch target := r.Data().(type) {
	case Item:
		if target == nil {
			return nil, nil, failure("target")
		}
		return target, nil, nil
	case *Item:
		if target == nil {
			return nil, nil, failure("target")
		}
		return nil, target, nil
	default:
		return nil, nil, failure("target")
	}
}
func putItem(target Item, pointer *Item, item Item) {
	if pointer != nil {
		*pointer = item
		return
	}
	clear(target)
	maps.Copy(target, item)
}

func (b *backend) Get(ctx context.Context, r record.Record) error {
	if r == nil {
		return failure("input")
	}
	o, err := b.client.begin(ctx)
	if err != nil {
		r.SetError(err)
		return err
	}
	defer o.close()
	original := r.Key()
	key, err := nativeRecordKey(b.client, original)
	collection := ""
	if err == nil {
		collection = original.Collection()
		table, _ := b.client.table(collection)
		key, err = cloneKey(o, table, key)
	}
	var target Item
	var pointer *Item
	if err == nil {
		target, pointer, err = dataTarget(r)
	}
	var item Item
	if err == nil {
		item, err = b.client.get(o, collection, key)
	}
	if err == nil && item == nil {
		err = record.ErrRecordNotFound
	}
	if err == nil {
		putItem(target, pointer, item)
	}
	if final := o.check(); final != nil {
		err = final
	}
	r.SetError(err)
	// A caller-owned record callback may cancel during the final state update.
	if final := o.check(); final != nil {
		r.SetError(final)
		return final
	}
	return err
}
func (b *backend) Exists(ctx context.Context, key *record.Key) (bool, error) {
	o, err := b.client.begin(ctx)
	if err != nil {
		return false, err
	}
	defer o.close()
	native, err := nativeRecordKey(b.client, key)
	if err != nil {
		return false, err
	}
	item, err := b.client.get(o, key.Collection(), native)
	if err == nil {
		err = o.check()
	}
	if err != nil {
		return false, err
	}
	return item != nil, nil
}
func (b *backend) GetMulti(ctx context.Context, records []record.Record) error {
	if len(records) > b.client.options.MaxRecords {
		return failure("limit")
	}
	// The slice itself is caller-owned too. Snapshot before callbacks, including
	// error restoration: one record must never replace a later admitted target.
	records = append([]record.Record(nil), records...)
	o, err := b.client.begin(ctx)
	check := func() error {
		if o != nil {
			return o.check()
		}
		if ctx != nil {
			return active(ctx)
		}
		return nil
	}
	finish := func(outcome error) error {
		if final := check(); final != nil {
			outcome = final
		}
		if outcome != nil {
			for _, r := range records {
				if r != nil {
					r.SetError(outcome)
				}
			}
		}
		// A restoration callback may itself cancel the caller; this also catches
		// a newly observed cancellation after the successful delivery loop.
		if final := check(); final != nil {
			same := outcome == final
			if a, ok := outcome.(*Error); ok {
				if b, ok := final.(*Error); ok {
					same = a.Code == b.Code
				}
			}
			if !same {
				for _, r := range records {
					if r != nil {
						r.SetError(final)
					}
				}
				// Fresh guard AFTER reclassification callbacks, with no callbacks
				// following this last check and no duplicate deferred restoration.
				if last := check(); last != nil {
					return last
				}
			}
			return final
		}
		return outcome
	}
	if err != nil {
		return finish(err)
	}
	defer o.close()
	keys := make([]Item, len(records))
	targets := make([]Item, len(records))
	pointers := make([]*Item, len(records))
	collections := make([]string, len(records))
	for i, r := range records {
		if r == nil {
			return finish(failure("input"))
		}
		original := r.Key()
		keys[i], err = nativeRecordKey(b.client, original)
		if err != nil {
			break
		}
		collections[i] = original.Collection()
		table, _ := b.client.table(collections[i])
		keys[i], err = cloneKey(o, table, keys[i])
		if err != nil {
			break
		}
	}
	// Capture ALL identities before any caller-owned record callback can mutate
	// another record's key or change a wrapper's selected collection.
	if err == nil {
		for i, r := range records {
			targets[i], pointers[i], err = dataTarget(r)
			if err != nil {
				break
			}
		}
	}
	items := make([]Item, len(records))
	if err == nil {
		for i := range records {
			items[i], err = b.client.get(o, collections[i], keys[i])
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		err = o.check()
	}
	if err == nil {
		for i, r := range records {
			item := items[i]
			if item == nil {
				r.SetError(record.ErrRecordNotFound)
			} else {
				putItem(targets[i], pointers[i], item)
				r.SetError(nil)
			}
			if err = o.check(); err != nil {
				break
			}
		}
	}
	return finish(err)
}

func (*backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}
func (*backend) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}
