package dalgo2dynamodb

import (
	"math"
	"regexp"
	"unicode"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CapacityMode selects the DynamoDB ReturnConsumedCapacity value for one
// QueryPartition call. The zero value leaves the wire field unset; NONE, TOTAL
// and INDEXES are sent verbatim. Stage A reports Query capacity only.
type CapacityMode string

const (
	CapacityNone    CapacityMode = "NONE"
	CapacityTotal   CapacityMode = "TOTAL"
	CapacityIndexes CapacityMode = "INDEXES"
)

// CapacityValues is the adapter-owned copy of an AWS capacity breakdown.
// Numbers are the finite, nonnegative float64 values already decoded by the
// official SDK, not raw decimal lexemes or a billing guarantee.
type CapacityValues struct {
	CapacityUnits      *float64 `json:"CapacityUnits,omitempty"`
	ReadCapacityUnits  *float64 `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits *float64 `json:"WriteCapacityUnits,omitempty"`
}

// ConsumedCapacity is an adapter-owned per-native-page report. Missing fields
// and maps stay missing; explicit zero values stay present. It is observed
// usage detail, not an index-discovery, IAM or billing proof.
type ConsumedCapacity struct {
	TableName              *string                   `json:"TableName,omitempty"`
	CapacityUnits          *float64                  `json:"CapacityUnits,omitempty"`
	ReadCapacityUnits      *float64                  `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits     *float64                  `json:"WriteCapacityUnits,omitempty"`
	Table                  *CapacityValues           `json:"Table,omitempty"`
	LocalSecondaryIndexes  map[string]CapacityValues `json:"LocalSecondaryIndexes,omitempty"`
	GlobalSecondaryIndexes map[string]CapacityValues `json:"GlobalSecondaryIndexes,omitempty"`
}

// QueryCapacityPage covers ONE native SDK Query response in this public call.
type QueryCapacityPage struct {
	Ordinal          int               `json:"Ordinal"`
	ScannedCount     int32             `json:"ScannedCount"`
	ConsumedCapacity *ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
}

// CapacitySum aggregates one top-level metric across this public call only.
// ReportedTotal is nil until a page supplies it; Complete is true only when
// every native page supplied that metric.
type CapacitySum struct {
	ReportedTotal *float64 `json:"ReportedTotal,omitempty"`
	ReportedPages int      `json:"ReportedPages"`
	Complete      bool     `json:"Complete"`
}

// QueryCapacityReceipt reports capacity observed for THIS QueryPartition call,
// including every native page fetched while filling the request limit. Numbers
// are the official SDK's decoded float64 observations, not a snapshot, index
// discovery, IAM verification or billing bound. It does not include earlier
// continuation calls, failed-call usage (which may have been charged but is
// unknown to the caller), a journey total, or an enforced budget. Explicit Scan
// and original-scope capacity accounting remain Stage B; DynamoDB Local,
// hosted/browser and installed-release checks remain separate.
type QueryCapacityReceipt struct {
	Mode                 CapacityMode        `json:"Mode"`
	NativePages          int                 `json:"NativePages"`
	ReportedPages        int                 `json:"ReportedPages"`
	ObservedScannedCount int64               `json:"ObservedScannedCount"`
	Pages                []QueryCapacityPage `json:"Pages"`
	CapacityUnits        CapacitySum         `json:"CapacityUnits"`
	ReadCapacityUnits    CapacitySum         `json:"ReadCapacityUnits"`
	WriteCapacityUnits   CapacitySum         `json:"WriteCapacityUnits"`
}

const (
	capacityTableNameMaxBytes = 1024
	capacityIndexMaxEntries   = 32
)

var capacityIndexName = regexp.MustCompile(`^[A-Za-z0-9_.-]{3,255}$`)

func validateCapacityMode(mode CapacityMode) error {
	switch mode {
	case "", CapacityNone, CapacityTotal, CapacityIndexes:
		return nil
	default:
		return failure("query")
	}
}

func validCapacityTableName(name string) bool {
	if name == "" || len(name) > capacityTableNameMaxBytes || !utf8.ValidString(name) {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func finiteNonNegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

func copyCapacityMetric(o *operation, source *float64) (*float64, error) {
	if source == nil {
		return nil, nil
	}
	if err := o.charge(8); err != nil {
		return nil, err
	}
	value := *source
	if !finiteNonNegative(value) {
		return nil, failure("invalid-response")
	}
	return &value, nil
}

func copyCapacityValues(o *operation, source *types.Capacity) (*CapacityValues, error) {
	if source == nil {
		return nil, nil
	}
	if err := o.charge(1); err != nil {
		return nil, err
	}
	capacityUnits, err := copyCapacityMetric(o, source.CapacityUnits)
	if err != nil {
		return nil, err
	}
	readCapacityUnits, err := copyCapacityMetric(o, source.ReadCapacityUnits)
	if err != nil {
		return nil, err
	}
	writeCapacityUnits, err := copyCapacityMetric(o, source.WriteCapacityUnits)
	if err != nil {
		return nil, err
	}
	return &CapacityValues{
		CapacityUnits:      capacityUnits,
		ReadCapacityUnits:  readCapacityUnits,
		WriteCapacityUnits: writeCapacityUnits,
	}, o.check()
}

func copyCapacityIndexes(o *operation, source map[string]types.Capacity) (map[string]CapacityValues, error) {
	if len(source) == 0 {
		return nil, nil
	}
	if len(source) > capacityIndexMaxEntries {
		return nil, failure("invalid-response")
	}
	result := make(map[string]CapacityValues, len(source))
	for label, raw := range source {
		if !capacityIndexName.MatchString(label) {
			return nil, failure("invalid-response")
		}
		if err := o.charge(len(label) + 1); err != nil {
			return nil, err
		}
		values, err := copyCapacityValues(o, &raw)
		if err != nil {
			return nil, err
		}
		result[label] = *values
	}
	return result, o.check()
}

func copyConsumedCapacity(o *operation, expectedTable string, source *types.ConsumedCapacity) (*ConsumedCapacity, error) {
	if source == nil {
		return nil, nil
	}
	if err := o.charge(1); err != nil {
		return nil, err
	}
	if len(source.VectorIndexes) != 0 {
		return nil, failure("invalid-response")
	}
	var tableName *string
	if source.TableName != nil {
		name := *source.TableName
		if err := o.charge(len(name) + 1); err != nil {
			return nil, err
		}
		if !validCapacityTableName(name) || name != expectedTable {
			return nil, failure("invalid-response")
		}
		tableName = &name
	}
	capacityUnits, err := copyCapacityMetric(o, source.CapacityUnits)
	if err != nil {
		return nil, err
	}
	readCapacityUnits, err := copyCapacityMetric(o, source.ReadCapacityUnits)
	if err != nil {
		return nil, err
	}
	writeCapacityUnits, err := copyCapacityMetric(o, source.WriteCapacityUnits)
	if err != nil {
		return nil, err
	}
	table, err := copyCapacityValues(o, source.Table)
	if err != nil {
		return nil, err
	}
	local, err := copyCapacityIndexes(o, source.LocalSecondaryIndexes)
	if err != nil {
		return nil, err
	}
	global, err := copyCapacityIndexes(o, source.GlobalSecondaryIndexes)
	if err != nil {
		return nil, err
	}
	if err := o.check(); err != nil {
		return nil, err
	}
	return &ConsumedCapacity{
		TableName:              tableName,
		CapacityUnits:          capacityUnits,
		ReadCapacityUnits:      readCapacityUnits,
		WriteCapacityUnits:     writeCapacityUnits,
		Table:                  table,
		LocalSecondaryIndexes:  local,
		GlobalSecondaryIndexes: global,
	}, nil
}

func checkedCapacityAdd(total *float64, add float64) (*float64, error) {
	if !finiteNonNegative(add) {
		return nil, failure("invalid-response")
	}
	if total == nil {
		value := add
		return &value, nil
	}
	sum := *total + add
	if !finiteNonNegative(sum) {
		return nil, failure("invalid-response")
	}
	return &sum, nil
}

func addCapacitySum(sum CapacitySum, value *float64, nativePages int) (CapacitySum, error) {
	if value == nil {
		sum.Complete = sum.ReportedPages == nativePages
		return sum, nil
	}
	total, err := checkedCapacityAdd(sum.ReportedTotal, *value)
	if err != nil {
		return CapacitySum{}, err
	}
	sum.ReportedTotal = total
	sum.ReportedPages++
	sum.Complete = sum.ReportedPages == nativePages
	return sum, nil
}

func addObservedScannedCount(total int64, add int32) (int64, error) {
	if total < 0 || add < 0 || total > 1<<63-1-int64(add) {
		return 0, failure("invalid-response")
	}
	return total + int64(add), nil
}

// appendQueryCapacity copies, validates and charges one native page before
// mutating the receipt. expectedTable is the configured physical table name.
func appendQueryCapacity(o *operation, receipt *QueryCapacityReceipt, expectedTable string, ordinal int, scanned int32, source *types.ConsumedCapacity) error {
	if receipt == nil || ordinal != receipt.NativePages+1 {
		return failure("invalid-response")
	}
	copied, err := copyConsumedCapacity(o, expectedTable, source)
	if err != nil {
		return err
	}
	observed, err := addObservedScannedCount(receipt.ObservedScannedCount, scanned)
	if err != nil {
		return err
	}
	nativePages := receipt.NativePages + 1
	var capacityUnits, readCapacityUnits, writeCapacityUnits *float64
	if copied != nil {
		capacityUnits = copied.CapacityUnits
		readCapacityUnits = copied.ReadCapacityUnits
		writeCapacityUnits = copied.WriteCapacityUnits
	}
	capacitySum, err := addCapacitySum(receipt.CapacityUnits, capacityUnits, nativePages)
	if err != nil {
		return err
	}
	readSum, err := addCapacitySum(receipt.ReadCapacityUnits, readCapacityUnits, nativePages)
	if err != nil {
		return err
	}
	writeSum, err := addCapacitySum(receipt.WriteCapacityUnits, writeCapacityUnits, nativePages)
	if err != nil {
		return err
	}
	reportedPages := receipt.ReportedPages
	if copied != nil {
		reportedPages++
	}
	receipt.NativePages = nativePages
	receipt.ReportedPages = reportedPages
	receipt.ObservedScannedCount = observed
	receipt.Pages = append(receipt.Pages, QueryCapacityPage{Ordinal: ordinal, ScannedCount: scanned, ConsumedCapacity: copied})
	receipt.CapacityUnits = capacitySum
	receipt.ReadCapacityUnits = readSum
	receipt.WriteCapacityUnits = writeSum
	return o.check()
}
