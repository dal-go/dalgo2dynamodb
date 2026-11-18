package dalgo2dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type queryJourneyCharge struct {
	bytes int
	nodes int
}

type queryJourneyNativePage struct {
	items        []Item
	next         Item
	scanned      int32
	capacity     *ConsumedCapacity
	requestCost  queryJourneyCharge
	itemsCost    queryJourneyCharge
	nextCost     queryJourneyCharge
	capacityCost queryJourneyCharge
}

func (c *Client) fetchQueryJourneyNativePage(o *operation, plan partitionQueryPlan, start Item, limit int32) (queryJourneyNativePage, error) {
	var result queryJourneyNativePage
	if limit < 1 || int(limit) > c.options.PageSize {
		return queryJourneyNativePage{}, failure("limit")
	}
	requestBytes, requestNodes := o.bytes, o.nodes
	table := plan.table
	values, err := cloneItem(o, plan.values, 0)
	if err != nil {
		return queryJourneyNativePage{}, err
	}
	var cursor Item
	if len(start) > 0 {
		cursor, err = cloneKey(o, table, start)
		if err != nil {
			return queryJourneyNativePage{}, err
		}
	}
	if err := o.charge(len(plan.names)); err != nil {
		return queryJourneyNativePage{}, err
	}
	names := make(map[string]string, len(plan.names))
	for name, value := range plan.names {
		if err := o.charge(len(name) + len(value)); err != nil {
			return queryJourneyNativePage{}, err
		}
		names[name] = value
	}
	expression := plan.expression
	forward := !plan.descending
	mode := plan.mode
	result.requestCost = queryJourneyCharge{bytes: o.bytes - requestBytes, nodes: o.nodes - requestNodes}
	output, err := invoke(o, func(ctx context.Context) (*dynamodb.QueryOutput, error) {
		return c.api.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &table.Name,
			KeyConditionExpression:    &expression,
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
			ExclusiveStartKey:         cursor,
			Limit:                     &limit,
			ConsistentRead:            &table.ConsistentRead,
			ScanIndexForward:          &forward,
			ReturnConsumedCapacity:    types.ReturnConsumedCapacity(mode),
		})
	})
	if err != nil {
		return queryJourneyNativePage{}, err
	}
	if output == nil || len(output.Items) > int(limit) || output.Count != int32(len(output.Items)) || output.ScannedCount < output.Count || output.ScannedCount > limit {
		return queryJourneyNativePage{}, failure("invalid-response")
	}
	result.scanned = output.ScannedCount
	itemsBytes, itemsNodes := o.bytes, o.nodes
	if err := o.charge(len(output.Items) * 8); err != nil {
		return queryJourneyNativePage{}, err
	}
	result.items = make([]Item, 0, len(output.Items))
	for _, raw := range output.Items {
		item, err := cloneItem(o, raw, 0)
		if err != nil {
			return queryJourneyNativePage{}, err
		}
		key := Item{table.PartitionKey: item[table.PartitionKey]}
		if table.SortKey != "" {
			key[table.SortKey] = item[table.SortKey]
		}
		if _, err := responseKey(o, table, key); err != nil {
			return queryJourneyNativePage{}, err
		}
		if !equalKeyValue(plan.partition, item[table.PartitionKey]) || (plan.sortCondition != nil && !matchesSort(item[table.SortKey], plan.sortCondition)) {
			return queryJourneyNativePage{}, failure("invalid-response")
		}
		result.items = append(result.items, item)
	}
	result.itemsCost = queryJourneyCharge{bytes: o.bytes - itemsBytes, nodes: o.nodes - itemsNodes}
	nextBytes, nextNodes := o.bytes, o.nodes
	if len(output.LastEvaluatedKey) > 0 {
		next, err := responseKey(o, table, output.LastEvaluatedKey)
		if err != nil {
			return queryJourneyNativePage{}, err
		}
		if !equalKeyValue(plan.partition, next[table.PartitionKey]) || (plan.sortCondition != nil && !matchesSort(next[table.SortKey], plan.sortCondition)) {
			return queryJourneyNativePage{}, failure("invalid-response")
		}
		result.next = next
	}
	result.nextCost = queryJourneyCharge{bytes: o.bytes - nextBytes, nodes: o.nodes - nextNodes}
	capacityBytes, capacityNodes := o.bytes, o.nodes
	if plan.metrics {
		result.capacity, err = copyConsumedCapacity(o, table.Name, output.ConsumedCapacity)
		if err != nil {
			return queryJourneyNativePage{}, err
		}
	}
	result.capacityCost = queryJourneyCharge{bytes: o.bytes - capacityBytes, nodes: o.nodes - capacityNodes}
	if err := o.check(); err != nil {
		return queryJourneyNativePage{}, err
	}
	return result, nil
}
