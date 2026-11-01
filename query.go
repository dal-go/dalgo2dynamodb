package dalgo2dynamodb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
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
	table, err := c.table(request.Collection)
	if err != nil {
		return result, err
	}
	if request.Limit < 1 || request.Limit > c.options.MaxRecords || !keyValue(request.Partition) {
		return result, failure("query")
	}
	partition, err := cloneValue(o, request.Partition, 0)
	if err != nil {
		return result, err
	}
	values := Item{":p": partition}
	names := map[string]string{"#p": table.PartitionKey}
	expression := "#p = :p"
	var sortCondition *SortCondition
	if request.Sort != nil {
		if table.SortKey == "" {
			return result, failure("query")
		}
		operator := request.Sort.Operator
		count := 1
		switch operator {
		case "=", "<", "<=", ">", ">=", "begins_with":
		case "between":
			count = 2
		default:
			return result, failure("query")
		}
		if len(request.Sort.Values) != count {
			return result, failure("query")
		}
		sortCondition = &SortCondition{Operator: operator, Values: make([]types.AttributeValue, count)}
		for i, value := range request.Sort.Values {
			if !keyValue(value) {
				return result, failure("query")
			}
			sortCondition.Values[i], err = cloneValue(o, value, 0)
			if err != nil {
				return result, err
			}
		}
		if count == 2 {
			cmp, ok := compareKeyValue(sortCondition.Values[0], sortCondition.Values[1])
			if !ok || cmp > 0 {
				return result, failure("query")
			}
		}
		if operator == "begins_with" {
			switch sortCondition.Values[0].(type) {
			case *types.AttributeValueMemberS, *types.AttributeValueMemberB:
			default:
				return result, failure("query")
			}
		}
		names["#s"] = table.SortKey
		values[":s0"] = sortCondition.Values[0]
		switch operator {
		case "between":
			values[":s1"] = sortCondition.Values[1]
			expression += " AND #s BETWEEN :s0 AND :s1"
		case "begins_with":
			expression += " AND begins_with(#s, :s0)"
		default:
			expression += " AND #s " + operator + " :s0"
		}
	}
	fingerprint := strconv.Itoa(len(request.Collection)) + ":" + request.Collection + "\x00" + expression + "\x00" + keyIdentity(values)
	if request.Descending {
		fingerprint += "\x00descending"
	}
	sum := sha256.Sum256([]byte(fingerprint))
	scope := hex.EncodeToString(sum[:])
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
	forward := !request.Descending
	for {
		if err := o.check(); err != nil {
			return result, err
		}
		if result.Pages >= c.options.MaxPages || result.Evaluated >= c.options.MaxEvaluated {
			return result, failure("limit")
		}
		limit := min(request.Limit-len(result.Items), c.options.PageSize, c.options.MaxEvaluated-result.Evaluated)
		wireLimit := int32(limit)
		output, err := invoke(o, func(ctx context.Context) (*dynamodb.QueryOutput, error) {
			return c.api.Query(ctx, &dynamodb.QueryInput{TableName: &table.Name, KeyConditionExpression: &expression, ExpressionAttributeNames: names, ExpressionAttributeValues: values, ExclusiveStartKey: cursor, Limit: &wireLimit, ConsistentRead: &table.ConsistentRead, ScanIndexForward: &forward})
		})
		if err != nil {
			return result, err
		}
		if output == nil || len(output.Items) > limit || output.Count != int32(len(output.Items)) || output.ScannedCount < output.Count || output.ScannedCount > wireLimit {
			return result, failure("invalid-response")
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
				if !ok || (!request.Descending && cmp <= 0) || (request.Descending && cmp >= 0) {
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
				if !ok || (!request.Descending && cmp < 0) || (request.Descending && cmp > 0) {
					return result, failure("invalid-response")
				}
			}
			if len(cursor) > 0 {
				cmp, ok := compareKeyValue(next[table.SortKey], cursor[table.SortKey])
				if !ok || (!request.Descending && cmp <= 0) || (request.Descending && cmp >= 0) {
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
		if len(result.Items) == request.Limit {
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
