package dalgo2dynamodb

import "time"

// ReadStopReason explains why a delivered page is terminal. An empty value is
// a nonterminal page that has a process-local successor cursor.
type ReadStopReason string

const (
	// ReadStopNonterminal marks a page whose journey continues with ReadPage.Next.
	ReadStopNonterminal ReadStopReason = ""
	// ReadStopNativeEOF means the native source produced no continuation key.
	ReadStopNativeEOF ReadStopReason = "native-eof"
	// ReadStopRequestedRowLimit means the admitted request row limit was reached.
	ReadStopRequestedRowLimit ReadStopReason = "requested-row-limit"
)

// ReadPage is one locally delivered query page. Next is a process-local handle
// bound to the issuing Client and is intentionally not portable JSON. The
// receipt is provisional until the owning caller acknowledges this delivery.
type ReadPage struct {
	Items   []Item             `json:"Items"`
	Next    *ReadCursor        `json:"-"`
	Ordinal int                `json:"Ordinal"`
	HasMore bool               `json:"HasMore"`
	Receipt ReadJourneyReceipt `json:"Receipt"`
}

// ReadJourneyReceipt snapshots the journey accounting projected for this page.
// During preparation it is provisional; it becomes the acknowledged owner view
// only after the final delivery commit. It publishes no claim and releases no
// retained capacity by itself. Capacity observations are SDK-decoded numbers,
// not a billing guarantee.
type ReadJourneyReceipt struct {
	DispatchedCalls      int                  `json:"DispatchedCalls"`
	FetchedPages         int                  `json:"FetchedPages"`
	AckedPages           int                  `json:"AckedPages"`
	ReceivedRows         int                  `json:"ReceivedRows"`
	AckedRows            int                  `json:"AckedRows"`
	ObservedScannedCount int                  `json:"ObservedScannedCount"`
	LogicalWork          int                  `json:"LogicalWork"`
	ParsedWorkBytes      int                  `json:"ParsedWorkBytes"`
	ParsedWorkNodes      int                  `json:"ParsedWorkNodes"`
	OriginalExpiry       time.Time            `json:"OriginalExpiry"`
	StopReason           ReadStopReason       `json:"StopReason"`
	Capacity             *ReadCapacityReceipt `json:"Capacity,omitempty"`
}

// ReadCapacityReceipt is the adapter-owned capacity view for one journey. It
// preserves missing versus explicit zero observations and never turns a page
// breakdown into a total or a billing bound.
type ReadCapacityReceipt struct {
	Mode               CapacityMode      `json:"Mode"`
	NativePages        int               `json:"NativePages"`
	ReportedPages      int               `json:"ReportedPages"`
	CapacityUnits      CapacitySum       `json:"CapacityUnits"`
	ReadCapacityUnits  CapacitySum       `json:"ReadCapacityUnits"`
	WriteCapacityUnits CapacitySum       `json:"WriteCapacityUnits"`
	Current            *ConsumedCapacity `json:"Current,omitempty"`
}

// copyQueryJourneyFrame builds a detached page from the already staged native
// pending page. The caller owns the logical journey state and supplies o as a
// separately charged transient preview workspace with an owned context. This
// builder never mutates the registry, never acknowledges, never calls a timer
// or provider, and returns a zero page on every error.
func copyQueryJourneyFrame(o *operation, state *queryJourneyState, next *ReadCursor, stop ReadStopReason) (ReadPage, error) {
	var zero ReadPage
	if o == nil || state == nil || state.pending == nil {
		return zero, failure("prepare")
	}
	pending := state.pending
	maxInt := int(^uint(0) >> 1)
	if len(pending.items) > maxInt/8 {
		return zero, failure("limit")
	}
	if err := o.charge(len(pending.items) * 8); err != nil {
		return zero, err
	}
	items := make([]Item, len(pending.items))
	for i, item := range pending.items {
		cloned, err := cloneItem(o, item, 0)
		if err != nil {
			return zero, err
		}
		items[i] = cloned
	}
	ackedPages := state.work.ackedPages + 1
	if ackedPages < 1 || state.work.ackedRows < 0 || len(pending.items) > maxInt-state.work.ackedRows {
		return zero, failure("limit")
	}
	receipt := ReadJourneyReceipt{
		DispatchedCalls:      state.work.admittedCalls,
		FetchedPages:         state.work.fetchedPages,
		AckedPages:           ackedPages,
		ReceivedRows:         state.work.receivedRows,
		AckedRows:            state.work.ackedRows + len(pending.items),
		ObservedScannedCount: state.work.observedScanned,
		LogicalWork:          state.work.logicalWork,
		ParsedWorkBytes:      state.work.parsedBytes,
		ParsedWorkNodes:      state.work.parsedNodes,
		OriginalExpiry:       state.expires,
		StopReason:           stop,
	}
	if state.plan.metrics {
		capacity, err := copyQueryJourneyCapacityReceipt(o, state, pending.capacity)
		if err != nil {
			return zero, err
		}
		receipt.Capacity = capacity
	}
	if err := o.check(); err != nil {
		return zero, err
	}
	return ReadPage{
		Items:   items,
		Next:    next,
		Ordinal: ackedPages,
		HasMore: len(pending.next) > 0,
		Receipt: receipt,
	}, nil
}

func copyQueryJourneyCapacityReceipt(o *operation, state *queryJourneyState, current *ConsumedCapacity) (*ReadCapacityReceipt, error) {
	if err := o.charge(1); err != nil {
		return nil, err
	}
	units, err := copyQueryJourneyCapacitySum(o, state.capacity.units)
	if err != nil {
		return nil, err
	}
	read, err := copyQueryJourneyCapacitySum(o, state.capacity.read)
	if err != nil {
		return nil, err
	}
	write, err := copyQueryJourneyCapacitySum(o, state.capacity.write)
	if err != nil {
		return nil, err
	}
	copied, err := copyQueryJourneyConsumedCapacity(o, current)
	if err != nil {
		return nil, err
	}
	if err := o.check(); err != nil {
		return nil, err
	}
	return &ReadCapacityReceipt{
		Mode:               CapacityMode(state.plan.mode),
		NativePages:        state.capacity.pages,
		ReportedPages:      state.capacity.reported,
		CapacityUnits:      units,
		ReadCapacityUnits:  read,
		WriteCapacityUnits: write,
		Current:            copied,
	}, nil
}

func copyQueryJourneyCapacitySum(o *operation, sum CapacitySum) (CapacitySum, error) {
	result := sum
	if sum.ReportedTotal != nil {
		total, err := copyCapacityMetric(o, sum.ReportedTotal)
		if err != nil {
			return CapacitySum{}, err
		}
		result.ReportedTotal = total
	}
	return result, o.check()
}

func copyQueryJourneyCapacityValues(o *operation, source *CapacityValues) (*CapacityValues, error) {
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
	if err := o.check(); err != nil {
		return nil, err
	}
	return &CapacityValues{
		CapacityUnits:      capacityUnits,
		ReadCapacityUnits:  readCapacityUnits,
		WriteCapacityUnits: writeCapacityUnits,
	}, nil
}

func copyQueryJourneyCapacityIndexes(o *operation, source map[string]CapacityValues) (map[string]CapacityValues, error) {
	if len(source) == 0 {
		return nil, nil
	}
	if len(source) > capacityIndexMaxEntries {
		return nil, failure("invalid-response")
	}
	if err := o.charge(len(source)); err != nil {
		return nil, err
	}
	result := make(map[string]CapacityValues, len(source))
	for label, raw := range source {
		if !capacityIndexName.MatchString(label) {
			return nil, failure("invalid-response")
		}
		if err := o.charge(len(label) + 1); err != nil {
			return nil, err
		}
		values, err := copyQueryJourneyCapacityValues(o, &raw)
		if err != nil {
			return nil, err
		}
		result[label] = *values
	}
	return result, o.check()
}

func copyQueryJourneyConsumedCapacity(o *operation, source *ConsumedCapacity) (*ConsumedCapacity, error) {
	if source == nil {
		return nil, nil
	}
	if err := o.charge(1); err != nil {
		return nil, err
	}
	var tableName *string
	if source.TableName != nil {
		name := *source.TableName
		if !validCapacityTableName(name) {
			return nil, failure("invalid-response")
		}
		if err := o.charge(len(name) + 1); err != nil {
			return nil, err
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
	table, err := copyQueryJourneyCapacityValues(o, source.Table)
	if err != nil {
		return nil, err
	}
	local, err := copyQueryJourneyCapacityIndexes(o, source.LocalSecondaryIndexes)
	if err != nil {
		return nil, err
	}
	global, err := copyQueryJourneyCapacityIndexes(o, source.GlobalSecondaryIndexes)
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
