package dalgo2dynamodb

import (
	"bytes"
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// SortCondition supports native key comparisons only, never raw expressions.
// Operators: =, <, <=, >, >=, between (2 values), begins_with (S/B only).
type SortCondition struct {
	Operator string
	Values   []types.AttributeValue
}

type QueryRequest struct {
	Collection string
	Partition  types.AttributeValue
	Sort       *SortCondition
	Limit      int
	Descending bool
	Cursor     *Cursor
	// ReturnConsumedCapacity optionally requests per-native-page capacity
	// metrics; the zero value preserves the original wire request.
	ReturnConsumedCapacity CapacityMode
}

// Cursor is an opaque, immutable in-process continuation. It is bound to the
// issuing Client, collection, partition, sort condition and direction. There is
// no decoder accepting an arbitrary service key or caller expression.
type Cursor struct {
	owner *Client
	scope string
	key   Item
}
type QueryPage struct {
	Items     []Item
	Next      *Cursor
	Pages     int
	Evaluated int
	// Capacity is nil unless TOTAL or INDEXES was explicitly requested.
	Capacity *QueryCapacityReceipt `json:"Capacity,omitempty"`
}

func (c *Client) QueryPartition(ctx context.Context, request QueryRequest) (QueryPage, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return QueryPage{}, err
	}
	defer o.close()
	page, err := c.query(o, request)
	if err == nil {
		err = o.check()
	}
	if err != nil {
		return QueryPage{}, err
	}
	return page, nil
}

func (c *Client) query(o *operation, request QueryRequest) (QueryPage, error) {
	var result QueryPage
	plan, err := c.compilePartitionQuery(o, request)
	if err != nil {
		return result, err
	}
	table := plan.table
	partition := plan.partition
	sortCondition := plan.sortCondition
	expression := plan.expression
	names := plan.names
	values := plan.values
	scope := plan.scope
	metrics := plan.metrics
	requestLimit := plan.limit
	descending := plan.descending
	mode := plan.mode
	var cursor Item
	if request.Cursor != nil {
		if request.Cursor.owner != c || request.Cursor.scope != scope {
			return result, failure("cursor")
		}
		cursor, err = cloneKey(o, table, request.Cursor.key)
		if err != nil {
			return result, err
		}
	}
	seenCursors := map[string]bool{}
	if len(cursor) > 0 {
		seenCursors[keyIdentity(cursor)] = true
	}
	seenItems := map[string]bool{}
	var previousSort types.AttributeValue
	if len(cursor) > 0 && table.SortKey != "" {
		previousSort = cursor[table.SortKey]
	}
	if metrics {
		result.Capacity = &QueryCapacityReceipt{Mode: mode}
	}
	forward := !descending
	for {
		if err := o.check(); err != nil {
			return result, err
		}
		if result.Pages >= c.options.MaxPages || result.Evaluated >= c.options.MaxEvaluated {
			return result, failure("limit")
		}
		limit := min(requestLimit-len(result.Items), c.options.PageSize, c.options.MaxEvaluated-result.Evaluated)
		wireLimit := int32(limit)
		output, err := invoke(o, func(ctx context.Context) (*dynamodb.QueryOutput, error) {
			return c.api.Query(ctx, &dynamodb.QueryInput{TableName: &table.Name, KeyConditionExpression: &expression, ExpressionAttributeNames: names, ExpressionAttributeValues: values, ExclusiveStartKey: cursor, Limit: &wireLimit, ConsistentRead: &table.ConsistentRead, ScanIndexForward: &forward, ReturnConsumedCapacity: types.ReturnConsumedCapacity(mode)})
		})
		if err != nil {
			return result, err
		}
		if output == nil || len(output.Items) > limit || output.Count != int32(len(output.Items)) || output.ScannedCount < output.Count || output.ScannedCount > wireLimit {
			return result, failure("invalid-response")
		}
		if metrics {
			if err := appendQueryCapacity(o, result.Capacity, table.Name, result.Pages+1, output.ScannedCount, output.ConsumedCapacity); err != nil {
				return result, err
			}
		}
		result.Pages++
		result.Evaluated += max(1, int(output.ScannedCount))
		for _, raw := range output.Items {
			item, err := cloneItem(o, raw, 0)
			if err != nil {
				return result, err
			}
			key := Item{table.PartitionKey: item[table.PartitionKey]}
			if table.SortKey != "" {
				key[table.SortKey] = item[table.SortKey]
			}
			if _, err := responseKey(o, table, key); err != nil {
				return result, err
			}
			if !equalKeyValue(partition, item[table.PartitionKey]) || (sortCondition != nil && !matchesSort(item[table.SortKey], sortCondition)) {
				return result, failure("invalid-response")
			}
			if previousSort != nil {
				cmp, ok := compareKeyValue(item[table.SortKey], previousSort)
				if !ok || (!descending && cmp <= 0) || (descending && cmp >= 0) {
					return result, failure("invalid-response")
				}
			}
			if table.SortKey != "" {
				previousSort = item[table.SortKey]
			}
			identity := keyIdentity(key)
			if seenItems[identity] {
				return result, failure("invalid-response")
			}
			seenItems[identity] = true
			result.Items = append(result.Items, item)
		}
		if len(output.LastEvaluatedKey) == 0 {
			return result, o.check()
		}
		next, err := responseKey(o, table, output.LastEvaluatedKey)
		if err != nil {
			return result, err
		}
		if !equalKeyValue(partition, next[table.PartitionKey]) || (sortCondition != nil && !matchesSort(next[table.SortKey], sortCondition)) {
			return result, failure("invalid-response")
		}
		if table.SortKey != "" {
			if previousSort != nil {
				cmp, ok := compareKeyValue(next[table.SortKey], previousSort)
				if !ok || (!descending && cmp < 0) || (descending && cmp > 0) {
					return result, failure("invalid-response")
				}
			}
			if len(cursor) > 0 {
				cmp, ok := compareKeyValue(next[table.SortKey], cursor[table.SortKey])
				if !ok || (!descending && cmp <= 0) || (descending && cmp >= 0) {
					return result, failure("cursor")
				}
			}
			previousSort = next[table.SortKey]
		}
		identity := keyIdentity(next)
		if seenCursors[identity] {
			return result, failure("cursor")
		}
		seenCursors[identity] = true
		if len(result.Items) == requestLimit {
			result.Next = &Cursor{owner: c, scope: scope, key: next}
			return result, o.check()
		}
		cursor = next
	}
}

func matchesSort(value types.AttributeValue, condition *SortCondition) bool {
	if condition.Operator == "begins_with" {
		switch v := value.(type) {
		case *types.AttributeValueMemberS:
			p, ok := condition.Values[0].(*types.AttributeValueMemberS)
			return ok && v != nil && strings.HasPrefix(v.Value, p.Value)
		case *types.AttributeValueMemberB:
			p, ok := condition.Values[0].(*types.AttributeValueMemberB)
			return ok && v != nil && bytes.HasPrefix(v.Value, p.Value)
		default:
			return false
		}
	}
	n, ok := compareKeyValue(value, condition.Values[0])
	if !ok {
		return false
	}
	switch condition.Operator {
	case "=":
		return n == 0
	case "<":
		return n < 0
	case "<=":
		return n <= 0
	case ">":
		return n > 0
	case ">=":
		return n >= 0
	case "between":
		upper, ok := compareKeyValue(value, condition.Values[1])
		return ok && n >= 0 && upper <= 0
	default:
		return false
	}
}
