package dalgo2dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (c *Client) fetchScanJourneyNativePage(o *operation, plan scanJourneyPlan, start Item, limit int32) (queryJourneyNativePage, error) {
	if limit < 1 || int(limit) > c.options.PageSize {
		return queryJourneyNativePage{}, failure("limit")
	}
	if c.scanAPI == nil {
		return queryJourneyNativePage{}, failure("scan-disabled")
	}
	if err := o.check(); err != nil {
		return queryJourneyNativePage{}, err
	}

	requestBytes, requestNodes := o.bytes, o.nodes
	table := plan.common.table
	wireLimit := limit
	wireTableName := table.Name
	wireConsistentRead := table.ConsistentRead
	var cursor Item
	var err error
	if len(start) > 0 {
		cursor, err = cloneKey(o, table, start)
		if err != nil {
			return queryJourneyNativePage{}, err
		}
	}

	var result queryJourneyNativePage
	result.requestCost = queryJourneyCharge{bytes: o.bytes - requestBytes, nodes: o.nodes - requestNodes}
	output, err := invoke(o, func(ctx context.Context) (*dynamodb.ScanOutput, error) {
		return c.scanAPI.Scan(ctx, &dynamodb.ScanInput{
			TableName:              &wireTableName,
			ExclusiveStartKey:      cursor,
			Limit:                  &wireLimit,
			ConsistentRead:         &wireConsistentRead,
			ReturnConsumedCapacity: types.ReturnConsumedCapacity(plan.common.mode),
		})
	})
	if err != nil {
		return queryJourneyNativePage{}, err
	}
	if output == nil || len(output.Items) > int(limit) || output.Count != int32(len(output.Items)) || output.ScannedCount != output.Count || output.ScannedCount > limit {
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
		result.items = append(result.items, item)
	}
	result.itemsCost = queryJourneyCharge{bytes: o.bytes - itemsBytes, nodes: o.nodes - itemsNodes}

	nextBytes, nextNodes := o.bytes, o.nodes
	if len(output.LastEvaluatedKey) > 0 {
		next, err := responseKey(o, table, output.LastEvaluatedKey)
		if err != nil {
			return queryJourneyNativePage{}, err
		}
		result.next = next
	}
	result.nextCost = queryJourneyCharge{bytes: o.bytes - nextBytes, nodes: o.nodes - nextNodes}

	capacityBytes, capacityNodes := o.bytes, o.nodes
	if plan.common.metrics {
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
