package dalgo2dynamodb

import (
	"context"
	"errors"
	"time"
)

const (
	queryJourneyPendingBytes = 512
	queryJourneyPendingNodes = 4
	queryJourneySeenCost     = 32
)

func (c *Client) stageQueryJourneyPage(o *operation, cursor *ReadCursor, state *queryJourneyState) error {
	if o == nil || cursor == nil || cursor.owner != c {
		return failure("cursor")
	}
	pending, err := c.checkQueryJourneyPage(cursor, state)
	if err != nil {
		return err
	}
	if pending {
		return active(o.ctx)
	}
	if err := active(o.ctx); err != nil {
		return c.failQueryJourneyPage(cursor, state, state.work.parsedBytes, state.work.parsedNodes, err)
	}

	res, err := c.reserveQueryJourneyPage(cursor, state)
	if err != nil {
		if queryJourneyPageTerminal(err) {
			return c.failQueryJourneyPage(cursor, state, state.work.parsedBytes, state.work.parsedNodes, err)
		}
		return err
	}
	parsedBytes, parsedNodes, expires, work, err := c.admitQueryJourneyPage(cursor, state, res)
	if err != nil {
		return c.failQueryJourneyPage(cursor, state, state.work.parsedBytes, state.work.parsedNodes, err)
	}

	o.bytes = parsedBytes
	o.nodes = parsedNodes
	o.byteLimit = parsedBytes + res.byteBudget
	o.nodeLimit = parsedNodes + res.nodeBudget
	o.retain = c.readJourneyPhysicalRetain(cursor, state)
	previousCancel := o.cancel
	nativeCtx, nativeCancel := context.WithDeadline(o.ctx, expires)
	o.ctx = nativeCtx
	o.cancel = func() {
		nativeCancel()
		previousCancel()
	}

	page, err := c.fetchQueryJourneyNativePage(o, state.plan, state.key, int32(res.limit))
	if err != nil {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, queryJourneyPagePriority(o, err))
	}
	progress, err := validateQueryJourneyProgress(o, state.plan, state.key, state.seen, page)
	if err != nil {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, queryJourneyPagePriority(o, err))
	}

	oldSeen := state.seen
	for range oldSeen {
		if err := o.charge(queryJourneySeenCost); err != nil {
			return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, queryJourneyPagePriority(o, err))
		}
	}
	newSeen := make(map[[32]byte]struct{}, len(oldSeen)+len(progress))
	for digest := range oldSeen {
		newSeen[digest] = struct{}{}
	}
	for _, digest := range progress {
		newSeen[digest] = struct{}{}
	}

	pages := state.capacity.pages + 1
	reported := state.capacity.reported
	var unitsValue, readValue, writeValue *float64
	if page.capacity != nil {
		reported++
		unitsValue = page.capacity.CapacityUnits
		readValue = page.capacity.ReadCapacityUnits
		writeValue = page.capacity.WriteCapacityUnits
	}
	units, err := addCapacitySum(state.capacity.units, unitsValue, pages)
	if err != nil {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, err)
	}
	read, err := addCapacitySum(state.capacity.read, readValue, pages)
	if err != nil {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, err)
	}
	write, err := addCapacitySum(state.capacity.write, writeValue, pages)
	if err != nil {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, err)
	}
	capacity := queryJourneyCapacity{pages: pages, reported: reported, units: units, read: read, write: write}

	observed, err := addObservedScannedCount(int64(work.observedScanned), page.scanned)
	if err != nil || observed > int64(int(^uint(0)>>1)) {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, failure("invalid-response"))
	}
	if work.receivedRows < 0 || len(page.items) > int(^uint(0)>>1)-work.receivedRows {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, failure("invalid-response"))
	}
	if work.logicalWork < res.limit {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, failure("limit"))
	}
	actualLogical := int(page.scanned)
	if actualLogical < 1 {
		actualLogical = 1
	}
	updatedWork := work
	updatedWork.parsedBytes = o.bytes
	updatedWork.parsedNodes = o.nodes
	updatedWork.fetchedPages++
	updatedWork.receivedRows += len(page.items)
	updatedWork.logicalWork = work.logicalWork - res.limit + actualLogical
	updatedWork.observedScanned = int(observed)

	pendingCost := queryJourneyCharge{
		bytes: page.itemsCost.bytes + page.nextCost.bytes + page.capacityCost.bytes + queryJourneyPendingBytes,
		nodes: page.itemsCost.nodes + page.nextCost.nodes + page.capacityCost.nodes + queryJourneyPendingNodes,
	}
	historyCost := queryJourneyCharge{bytes: queryJourneySeenCost * len(progress), nodes: len(progress)}
	retainedBytes := pendingCost.bytes + historyCost.bytes
	retainedNodes := pendingCost.nodes + historyCost.nodes
	if retainedBytes < 0 || retainedBytes > res.bytes || retainedNodes < 0 || retainedNodes > res.nodes {
		return c.failQueryJourneyPage(cursor, state, o.bytes, o.nodes, failure("limit"))
	}

	ownedDone := o.ctx.Done()
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	current, ok := reg.open[cursor]
	if !ok || current != state {
		return failure("cursor")
	}
	if reg.closed || c.life.Err() != nil {
		return failure("closed")
	}
	if state.terminal {
		return failure("cursor")
	}
	if state.logical != 1 || state.reservation != res {
		return failure("busy")
	}
	if !time.Now().Before(state.expires) {
		c.failQueryJourneyPageLocked(cursor, state, o.bytes, o.nodes)
		return context.DeadlineExceeded
	}
	select {
	case <-ownedDone:
		c.failQueryJourneyPageLocked(cursor, state, o.bytes, o.nodes)
		ownedErr := o.ctx.Err()
		if ownedErr == nil {
			ownedErr = context.Canceled
		}
		return ownedErr
	default:
	}
	if err := c.settleQueryJourneyReservationLocked(state, res, retainedBytes, retainedNodes); err != nil {
		c.failQueryJourneyPageLocked(cursor, state, o.bytes, o.nodes)
		return err
	}
	state.seen = newSeen
	state.capacity = capacity
	state.work = updatedWork
	state.pending = &page
	state.pendingCost = pendingCost
	state.historyCost.bytes += historyCost.bytes
	state.historyCost.nodes += historyCost.nodes
	return nil
}

func (c *Client) checkQueryJourneyPage(cursor *ReadCursor, state *queryJourneyState) (bool, error) {
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	current, ok := reg.open[cursor]
	if !ok || current != state {
		return false, failure("cursor")
	}
	if reg.closed || c.life.Err() != nil {
		return false, failure("closed")
	}
	if !time.Now().Before(state.expires) {
		c.terminateReadJourneyLocked(cursor, state)
		return false, context.DeadlineExceeded
	}
	if state.terminal {
		return false, failure("cursor")
	}
	if state.logical != 1 {
		return false, failure("busy")
	}
	return state.pending != nil, nil
}

func (c *Client) admitQueryJourneyPage(cursor *ReadCursor, state *queryJourneyState, res *queryJourneyReservation) (int, int, time.Time, queryJourneyAccounting, error) {
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	current, ok := reg.open[cursor]
	if !ok || current != state || state.reservation != res {
		return 0, 0, time.Time{}, queryJourneyAccounting{}, failure("reservation")
	}
	if reg.closed || c.life.Err() != nil {
		return 0, 0, time.Time{}, queryJourneyAccounting{}, failure("closed")
	}
	if !time.Now().Before(state.expires) {
		c.terminateReadJourneyLocked(cursor, state)
		return 0, 0, time.Time{}, queryJourneyAccounting{}, context.DeadlineExceeded
	}
	if state.terminal || state.logical != 1 {
		return 0, 0, time.Time{}, queryJourneyAccounting{}, failure("cursor")
	}
	state.work.admittedCalls++
	state.work.logicalWork += res.limit
	return state.work.parsedBytes, state.work.parsedNodes, state.expires, state.work, nil
}

func (c *Client) failQueryJourneyPage(cursor *ReadCursor, state *queryJourneyState, parsedBytes, parsedNodes int, original error) error {
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	c.failQueryJourneyPageLocked(cursor, state, parsedBytes, parsedNodes)
	return original
}

func (c *Client) failQueryJourneyPageLocked(cursor *ReadCursor, state *queryJourneyState, parsedBytes, parsedNodes int) {
	current, ok := c.registry.open[cursor]
	if !ok || current != state {
		return
	}
	state.work.parsedBytes = parsedBytes
	state.work.parsedNodes = parsedNodes
	c.terminateReadJourneyLocked(cursor, state)
}

func queryJourneyPagePriority(o *operation, original error) error {
	if o != nil {
		if err := active(o.ctx); err != nil {
			return err
		}
		if o.client.life.Err() != nil {
			return failure("closed")
		}
	}
	return original
}

func queryJourneyPageTerminal(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var e *Error
	return errors.As(err, &e) && (e.Code == "limit" || e.Code == "transport" || e.Code == "invalid-response")
}
