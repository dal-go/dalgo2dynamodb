package dalgo2dynamodb

import (
	"context"
	"errors"
	"time"
)

const (
	queryJourneyPreviewReceiptBytes = 512
	queryJourneyPreviewReceiptNodes = 32
)

// NextReadPage delivers the next page of one locally admitted query journey.
func (c *Client) NextReadPage(ctx context.Context, cursor *ReadCursor) (ReadPage, error) {
	return c.NextReadPageWith(ctx, cursor, nil)
}

// NextReadPageWith delivers the next page and, when prepare is non-nil, invokes
// it with the detached page before the delivery is committed. A prepare failure
// returns a zero page and a fixed prepare error; the callback's own error is
// never forwarded. For a nonterminal page the successor handle is always
// allocated internally and is never supplied by the caller.
func (c *Client) NextReadPageWith(ctx context.Context, cursor *ReadCursor, prepare func(ReadPage) error) (ReadPage, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return c.failQueryJourneyDelivery(ctx, nil, cursor, nil, nil, err, false)
	}
	defer o.close()

	state, end, err := c.beginReadJourneyCall(ctx, cursor)
	if err != nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, nil, nil, err, false)
	}
	defer end()

	callerDone := ctx.Done()
	callerDeadline, hasCallerDeadline := ctx.Deadline()
	clipQueryJourneyCall(o, state.expires)

	profile := c.options.Journeys
	if profile == nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, failure("journey-disabled"), false)
	}
	if state.attempts < 0 || state.attempts == int(^uint(0)>>1) {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, failure("limit"), true)
	}
	state.attempts++
	if state.attempts > int(profile.MaxDeliveryAttempts) {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, failure("limit"), true)
	}

	if err := c.stageQueryJourneyPage(o, cursor, state); err != nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, err, false)
	}
	pending := state.pending
	if pending == nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, failure("prepare"), false)
	}

	var stop ReadStopReason
	var nextCursor *ReadCursor
	if len(pending.next) == 0 {
		stop = ReadStopNativeEOF
	} else if state.work.receivedRows == int(state.plan.limit) {
		stop = ReadStopRequestedRowLimit
	} else {
		if state.work.fetchedPages >= c.options.MaxPages || state.work.logicalWork >= c.readJourneyEvaluatedLimit(state) {
			return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, failure("limit"), true)
		}
		fits, err := nextQueryJourneyCallFits(c, state, profile)
		if err != nil {
			return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, err, true)
		}
		if !fits {
			return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, failure("limit"), true)
		}
		nextCursor = &ReadCursor{owner: c}
		stop = ReadStopNonterminal
	}

	previewBytes, previewNodes, err := queryJourneyPreviewBound(state)
	if err != nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, err, false)
	}
	preview, err := c.reserveQueryJourneyPreview(cursor, state, previewBytes, previewNodes)
	if err != nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, nil, err, false)
	}
	defer func() {
		defer c.refundQueryJourneyPreview(state, preview)
		o.close()
	}()

	o.bytes = 0
	o.nodes = 0
	o.byteLimit = preview.byteBudget
	o.nodeLimit = preview.nodeBudget
	o.retain = nil

	frame, err := copyQueryJourneyFrame(o, state, nextCursor, stop)
	if err != nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, preview, err, false)
	}
	if err := o.check(); err != nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, preview, err, false)
	}
	if prepare != nil {
		if err := prepare(frame); err != nil {
			return c.failQueryJourneyDelivery(ctx, o, cursor, state, preview, failure("prepare"), false)
		}
	}
	if err := o.check(); err != nil {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, preview, err, false)
	}
	o.close()

	committed, err := c.commitQueryJourneyDelivery(cursor, state, preview, pending, nextCursor, callerDone, callerDeadline, hasCallerDeadline)
	if !committed {
		return c.failQueryJourneyDelivery(ctx, o, cursor, state, preview, err, false)
	}
	return frame, nil
}

func clipQueryJourneyCall(o *operation, expires time.Time) {
	if o == nil {
		return
	}
	previous := o.cancel
	ctx, cancel := context.WithDeadline(o.ctx, expires)
	o.ctx = ctx
	o.cancel = func() {
		cancel()
		if previous != nil {
			previous()
		}
	}
}

func (c *Client) failQueryJourneyDelivery(ctx context.Context, o *operation, cursor *ReadCursor, state *queryJourneyState, preview *queryJourneyReservation, original error, terminal bool) (ReadPage, error) {
	classified := c.openJourneyFailure(ctx, o, normalizeQueryJourneyDeliveryError(original))
	if preview != nil && state != nil {
		c.refundQueryJourneyPreview(state, preview)
	}
	if terminal && cursor != nil && state != nil {
		c.failQueryJourneyPage(cursor, state, state.work.parsedBytes, state.work.parsedNodes, nil)
	}
	return ReadPage{}, classified
}

func normalizeQueryJourneyDeliveryError(err error) error {
	if err == nil {
		return failure("prepare")
	}
	var safe *Error
	if errors.As(err, &safe) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return failure("prepare")
}

func queryJourneyPreviewBound(state *queryJourneyState) (int, int, error) {
	if state == nil || state.pending == nil {
		return 0, 0, failure("prepare")
	}
	bytes, ok := queryJourneyAddInt(state.pendingCost.bytes, queryJourneyPreviewReceiptBytes)
	if !ok {
		return 0, 0, failure("limit")
	}
	nodes, ok := queryJourneyAddInt(state.pendingCost.nodes, queryJourneyPreviewReceiptNodes)
	if !ok {
		return 0, 0, failure("limit")
	}
	return bytes, nodes, nil
}

func nextQueryJourneyCallFits(c *Client, state *queryJourneyState, profile *JourneyOptions) (bool, error) {
	if state == nil || state.pending == nil || profile == nil {
		return false, failure("prepare")
	}
	pending := state.pending
	if pending.requestCost.bytes < state.keyCost.bytes || pending.requestCost.nodes < state.keyCost.nodes {
		return false, failure("limit")
	}
	bytes := pending.requestCost.bytes - state.keyCost.bytes
	nodes := pending.requestCost.nodes - state.keyCost.nodes
	var ok bool
	if bytes, ok = queryJourneyAddInt(bytes, pending.nextCost.bytes); !ok {
		return false, failure("limit")
	}
	if nodes, ok = queryJourneyAddInt(nodes, pending.nextCost.nodes); !ok {
		return false, failure("limit")
	}
	seenBytes, ok := queryJourneyMultiplyInt(len(state.seen), queryJourneySeenCost)
	if !ok {
		return false, failure("limit")
	}
	if bytes, ok = queryJourneyAddInt(bytes, seenBytes); !ok {
		return false, failure("limit")
	}
	if nodes, ok = queryJourneyAddInt(nodes, len(state.seen)); !ok {
		return false, failure("limit")
	}
	// The empty row vector still charges one mandatory node before allocation.
	if nodes, ok = queryJourneyAddInt(nodes, 1); !ok {
		return false, failure("limit")
	}

	byteAllowance := c.options.MaxParsedBytes - state.work.parsedBytes - queryJourneyPendingBytes
	nodeLimit := profile.MaxPageNodes
	if oldRoom := 100_000 - state.work.parsedNodes; oldRoom < nodeLimit {
		nodeLimit = oldRoom
	}
	nodeAllowance := nodeLimit - queryJourneyPendingNodes
	if byteAllowance < 0 || nodeAllowance < 0 {
		return false, nil
	}
	return bytes <= byteAllowance && nodes <= nodeAllowance, nil
}

func queryJourneyAddInt(a, b int) (int, bool) {
	maxInt := int(^uint(0) >> 1)
	if a < 0 || b < 0 || a > maxInt-b {
		return 0, false
	}
	return a + b, true
}

func queryJourneyMultiplyInt(a, b int) (int, bool) {
	maxInt := int(^uint(0) >> 1)
	if a < 0 || b < 0 {
		return 0, false
	}
	if a != 0 && b > maxInt/a {
		return 0, false
	}
	return a * b, true
}

func (c *Client) reserveQueryJourneyPreview(cursor *ReadCursor, state *queryJourneyState, bytes, nodes int) (*queryJourneyReservation, error) {
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	return c.reserveQueryJourneyPreviewLocked(cursor, state, bytes, nodes)
}

func (c *Client) reserveQueryJourneyPreviewLocked(cursor *ReadCursor, state *queryJourneyState, bytes, nodes int) (*queryJourneyReservation, error) {
	reg := &c.registry
	current, ok := reg.open[cursor]
	if !ok || current != state {
		return nil, failure("cursor")
	}
	if reg.closed || c.life.Err() != nil {
		return nil, failure("closed")
	}
	if !time.Now().Before(state.expires) {
		c.terminateReadJourneyLocked(cursor, state)
		return nil, context.DeadlineExceeded
	}
	if state.terminal {
		return nil, failure("cursor")
	}
	if state.logical != 1 {
		return nil, failure("busy")
	}
	if bytes < 0 || nodes < 0 {
		return nil, failure("limit")
	}
	profile := c.options.Journeys
	if profile == nil {
		return nil, failure("journey-disabled")
	}
	if bytes > profile.MaxRetainedBytes-reg.bytes || nodes > profile.MaxRetainedNodes-reg.nodes {
		return nil, failure("quota")
	}
	state.bytes += bytes
	state.nodes += nodes
	reg.bytes += bytes
	reg.nodes += nodes
	return &queryJourneyReservation{bytes: bytes, nodes: nodes, byteBudget: bytes, nodeBudget: nodes}, nil
}

func (c *Client) refundQueryJourneyPreview(state *queryJourneyState, preview *queryJourneyReservation) {
	if state == nil || preview == nil {
		return
	}
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	c.refundQueryJourneyPreviewLocked(state, preview)
}

func (c *Client) refundQueryJourneyPreviewLocked(state *queryJourneyState, preview *queryJourneyReservation) {
	if state == nil || preview == nil {
		return
	}
	state.bytes -= preview.bytes
	state.nodes -= preview.nodes
	c.registry.bytes -= preview.bytes
	c.registry.nodes -= preview.nodes
	preview.bytes = 0
	preview.nodes = 0
}

func (c *Client) commitQueryJourneyDelivery(cursor *ReadCursor, state *queryJourneyState, preview *queryJourneyReservation, pending *queryJourneyNativePage, next *ReadCursor, callerDone <-chan struct{}, callerDeadline time.Time, hasCallerDeadline bool) (bool, error) {
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if cursor == nil || cursor.owner != c || state == nil || pending == nil {
		return false, failure("cursor")
	}

	terminalPage := next == nil
	maxInt := int(^uint(0) >> 1)
	if state.work.ackedCalls < 0 || state.work.ackedCalls == maxInt ||
		state.work.ackedPages < 0 || state.work.ackedPages == maxInt ||
		state.work.ackedRows < 0 || len(pending.items) > maxInt-state.work.ackedRows {
		return false, failure("limit")
	}

	oldKey := state.keyCost
	oldPending := state.pendingCost
	newKey := queryJourneyCharge{}
	if !terminalPage {
		newKey = pending.nextCost
	}
	refundBytes, ok := queryJourneyAddInt(oldKey.bytes, oldPending.bytes)
	if !ok || refundBytes < newKey.bytes {
		return false, failure("limit")
	}
	refundBytes -= newKey.bytes
	refundNodes, ok := queryJourneyAddInt(oldKey.nodes, oldPending.nodes)
	if !ok || refundNodes < newKey.nodes {
		return false, failure("limit")
	}
	refundNodes -= newKey.nodes

	previewBytes, previewNodes := 0, 0
	if preview != nil {
		if preview.bytes < 0 || preview.nodes < 0 {
			return false, failure("limit")
		}
		previewBytes = preview.bytes
		previewNodes = preview.nodes
	}
	releaseBytes, ok := queryJourneyAddInt(refundBytes, previewBytes)
	if !ok {
		return false, failure("limit")
	}
	releaseNodes, ok := queryJourneyAddInt(refundNodes, previewNodes)
	if !ok {
		return false, failure("limit")
	}
	if releaseBytes > state.bytes || releaseNodes > state.nodes || releaseBytes > reg.bytes || releaseNodes > reg.nodes {
		return false, failure("limit")
	}

	current, ok := reg.open[cursor]
	if !ok || current != state {
		return false, failure("cursor")
	}
	if reg.closed || c.life.Err() != nil {
		return false, failure("closed")
	}
	if state.terminal {
		return false, failure("cursor")
	}
	if state.logical != 1 {
		return false, failure("busy")
	}
	if state.pending != pending {
		return false, failure("busy")
	}
	if !terminalPage {
		if next.owner != c || next == cursor || reg.open[next] != nil {
			return false, failure("cursor")
		}
	}
	if !time.Now().Before(state.expires) {
		c.terminateReadJourneyLocked(cursor, state)
		return false, context.DeadlineExceeded
	}
	callerDoneClosed := false
	select {
	case <-callerDone:
		callerDoneClosed = true
	default:
	}
	if hasCallerDeadline && !time.Now().Before(callerDeadline) {
		return false, context.DeadlineExceeded
	}
	if callerDoneClosed {
		return false, context.Canceled
	}

	state.bytes -= releaseBytes
	state.nodes -= releaseNodes
	reg.bytes -= releaseBytes
	reg.nodes -= releaseNodes
	if preview != nil {
		preview.bytes = 0
		preview.nodes = 0
	}

	state.work.ackedCalls++
	state.work.ackedPages++
	state.work.ackedRows += len(pending.items)
	if terminalPage {
		state.terminal = true
		state.pending = nil
		state.pendingCost = queryJourneyCharge{}
		state.key = nil
		state.keyCost = queryJourneyCharge{}
	} else {
		delete(reg.open, cursor)
		reg.open[next] = state
		state.current = next
		state.key = pending.next
		state.keyCost = pending.nextCost
		state.pending = nil
		state.pendingCost = queryJourneyCharge{}
	}
	return true, nil
}
