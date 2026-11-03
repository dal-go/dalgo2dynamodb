package dalgo2dynamodb

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type partitionQueryPlan struct {
	table         Table
	partition     types.AttributeValue
	sortCondition *SortCondition
	expression    string
	names         map[string]string
	values        Item
	scope         string
	metrics       bool
	limit         int
	descending    bool
	mode          CapacityMode
}

func (c *Client) compilePartitionQuery(o *operation, request QueryRequest) (partitionQueryPlan, error) {
	var plan partitionQueryPlan
	if err := validateCapacityMode(request.ReturnConsumedCapacity); err != nil {
		return plan, err
	}
	plan.metrics = request.ReturnConsumedCapacity == CapacityTotal || request.ReturnConsumedCapacity == CapacityIndexes
	table, err := c.table(request.Collection)
	if err != nil {
		return plan, err
	}
	plan.table = table
	if request.Limit < 1 || request.Limit > c.options.MaxRecords || !keyValue(request.Partition) {
		return plan, failure("query")
	}
	partition, err := cloneValue(o, request.Partition, 0)
	if err != nil {
		return plan, err
	}
	plan.partition = partition
	values := Item{":p": partition}
	names := map[string]string{"#p": table.PartitionKey}
	expression := "#p = :p"
	var sortCondition *SortCondition
	if request.Sort != nil {
		if table.SortKey == "" {
			return plan, failure("query")
		}
		operator := request.Sort.Operator
		count := 1
		switch operator {
		case "=", "<", "<=", ">", ">=", "begins_with":
		case "between":
			count = 2
		default:
			return plan, failure("query")
		}
		if len(request.Sort.Values) != count {
			return plan, failure("query")
		}
		sortCondition = &SortCondition{Operator: operator, Values: make([]types.AttributeValue, count)}
		for i, value := range request.Sort.Values {
			if !keyValue(value) {
				return plan, failure("query")
			}
			sortCondition.Values[i], err = cloneValue(o, value, 0)
			if err != nil {
				return plan, err
			}
		}
		if count == 2 {
			cmp, ok := compareKeyValue(sortCondition.Values[0], sortCondition.Values[1])
			if !ok || cmp > 0 {
				return plan, failure("query")
			}
		}
		if operator == "begins_with" {
			switch sortCondition.Values[0].(type) {
			case *types.AttributeValueMemberS, *types.AttributeValueMemberB:
			default:
				return plan, failure("query")
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
	plan.sortCondition = sortCondition
	plan.expression = expression
	plan.names = names
	plan.values = values
	fingerprint := strconv.Itoa(len(request.Collection)) + ":" + request.Collection + "\x00" + expression + "\x00" + keyIdentity(values)
	if request.Descending {
		fingerprint += "\x00descending"
	}
	if request.ReturnConsumedCapacity != "" {
		mode := string(request.ReturnConsumedCapacity)
		fingerprint += "\x00consumed-capacity:" + strconv.Itoa(len(mode)) + ":" + mode
	}
	sum := sha256.Sum256([]byte(fingerprint))
	plan.scope = hex.EncodeToString(sum[:])
	plan.limit = request.Limit
	plan.descending = request.Descending
	plan.mode = request.ReturnConsumedCapacity
	return plan, nil
}
