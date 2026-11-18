package dalgo2dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// ScanAPI is an OPTIONAL extension of API matching the official SDK v2 Scan
// method exactly. Existing API implementations that expose only GetItem and
// Query stay valid and simply leave Scan disabled; New never requires it and
// never calls it.
type ScanAPI interface {
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// ScanRequest is the initial whole-journey Scan shape: one explicitly granted
// collection, a logical evaluated-item ceiling and a returned-row ceiling. It
// deliberately carries no raw expressions, keys, index, parallel mode,
// direction, partition, Filter or Projection; those arrive in later stages.
type ScanRequest struct {
	Collection             string
	EvaluatedLimit         int
	MaxItems               int
	ReturnConsumedCapacity CapacityMode
}

// scanJourneyPlan reuses the shared partitionQueryPlan for table, scope, limit,
// capacity mode and metrics so a Scan journey is accounted by the SAME B1
// registry and budgets as a query journey. Query-only fields (partition, sort
// condition, expressions, names, values, descending) stay zero and are never
// used by a query fetch. A later state variant routes the explicit Scan helper.
type scanJourneyPlan struct {
	common         partitionQueryPlan
	evaluatedLimit int
}

// compileScanJourney validates one initial unfiltered Scan request over an
// EXISTING mapped table. The wire shape is a plain whole-table DynamoDB Scan
// with no filter, projection, index or segment, and the SDK Scan interface may
// be absent. It compiles local state only and performs no credential, network
// or SDK call. It does NOT claim complete Scan runtime; public page integration
// and tests land in the next connected stage.
func (c *Client) compileScanJourney(o *operation, request ScanRequest) (scanJourneyPlan, error) {
	if o == nil {
		return scanJourneyPlan{}, failure("input")
	}
	if err := o.check(); err != nil {
		return scanJourneyPlan{}, err
	}
	profile := c.options.Journeys
	if profile == nil || c.scanAPI == nil {
		return scanJourneyPlan{}, failure("scan-disabled")
	}
	if !validName(request.Collection) {
		return scanJourneyPlan{}, failure("scan")
	}
	if !scanGranted(profile.ScanCollections, request.Collection) {
		return scanJourneyPlan{}, failure("scan-disabled")
	}
	if err := validateCapacityMode(request.ReturnConsumedCapacity); err != nil {
		return scanJourneyPlan{}, failure("scan")
	}
	if request.MaxItems < 1 || request.MaxItems > c.options.MaxRecords || request.EvaluatedLimit < 1 || request.EvaluatedLimit > c.options.MaxEvaluated {
		return scanJourneyPlan{}, failure("scan")
	}
	table, err := c.table(request.Collection)
	if err != nil {
		return scanJourneyPlan{}, err
	}
	charge := 64 + len(request.Collection) + len(table.Name) + len(table.PartitionKey) + len(table.SortKey)
	charge += len(profile.Authority.PrincipalScope) + len(profile.Authority.Generation) + len(profile.Authority.ResourceScope)
	if err := o.charge(charge); err != nil {
		return scanJourneyPlan{}, err
	}
	var b strings.Builder
	writeScanField(&b, "scan")
	writeScanField(&b, request.Collection)
	writeScanField(&b, table.Name)
	writeScanField(&b, table.PartitionKey)
	writeScanField(&b, table.SortKey)
	writeScanField(&b, strconv.FormatBool(table.ConsistentRead))
	writeScanField(&b, strconv.Itoa(request.MaxItems))
	writeScanField(&b, strconv.Itoa(request.EvaluatedLimit))
	writeScanField(&b, string(request.ReturnConsumedCapacity))
	writeScanField(&b, profile.Authority.PrincipalScope)
	writeScanField(&b, profile.Authority.Generation)
	writeScanField(&b, profile.Authority.ResourceScope)
	sum := sha256.Sum256([]byte(b.String()))
	plan := scanJourneyPlan{evaluatedLimit: request.EvaluatedLimit}
	plan.common.table = table
	plan.common.scope = hex.EncodeToString(sum[:])
	plan.common.metrics = request.ReturnConsumedCapacity == CapacityTotal || request.ReturnConsumedCapacity == CapacityIndexes
	plan.common.limit = request.MaxItems
	plan.common.mode = request.ReturnConsumedCapacity
	if err := o.check(); err != nil {
		return scanJourneyPlan{}, err
	}
	return plan, nil
}

func scanGranted(collections []string, collection string) bool {
	for _, granted := range collections {
		if granted == collection {
			return true
		}
	}
	return false
}

func writeScanField(b *strings.Builder, field string) {
	b.WriteString(strconv.Itoa(len(field)))
	b.WriteByte(':')
	b.WriteString(field)
	b.WriteByte('|')
}
