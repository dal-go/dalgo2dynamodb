package dalgo2dynamodb

import (
	"context"
	"time"
)

type queryJourneyAccounting struct {
	parsedBytes     int
	parsedNodes     int
	admittedCalls   int
	fetchedPages    int
	receivedRows    int
	logicalWork     int
	observedScanned int
	ackedCalls      int
	ackedPages      int
	ackedRows       int
}

type queryJourneyReservation struct {
	bytes      int
	nodes      int
	limit      int
	byteBudget int
	nodeBudget int
}

func (c *Client) reserveQueryJourneyPage(cursor *ReadCursor, state *queryJourneyState) (*queryJourneyReservation, error) {
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if cursor == nil || cursor.owner != c {
		return nil, failure("cursor")
	}
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
	if state.logical != 1 || state.physical != 0 || state.reservation != nil || state.pending != nil {
		return nil, failure("busy")
	}
	profile := c.options.Journeys
	if profile == nil {
		return nil, failure("journey-disabled")
	}
	remainingRows := int(state.plan.limit) - state.work.receivedRows
	remainingPages := c.options.MaxPages - state.work.fetchedPages
	remainingWork := c.readJourneyEvaluatedLimit(state) - state.work.logicalWork
	if remainingRows < 1 || remainingPages < 1 || remainingWork < 1 {
		return nil, failure("limit")
	}
	limit := remainingRows
	if c.options.PageSize < limit {
		limit = c.options.PageSize
	}
	if remainingWork < limit {
		limit = remainingWork
	}
	if limit < 1 {
		return nil, failure("limit")
	}

	const pendingBytes = 512
	const pendingNodes = 4
	byteRoom := c.options.MaxParsedBytes - state.work.parsedBytes
	nodeRoom := profile.MaxPageNodes
	if oldRoom := 100_000 - state.work.parsedNodes; oldRoom < nodeRoom {
		nodeRoom = oldRoom
	}
	if byteRoom <= pendingBytes || nodeRoom <= pendingNodes {
		return nil, failure("limit")
	}
	if globalRoom := profile.MaxRetainedBytes - reg.bytes; globalRoom <= pendingBytes {
		return nil, failure("quota")
	} else if globalRoom < byteRoom {
		byteRoom = globalRoom
	}
	if globalRoom := profile.MaxRetainedNodes - reg.nodes; globalRoom <= pendingNodes {
		return nil, failure("quota")
	} else if globalRoom < nodeRoom {
		nodeRoom = globalRoom
	}
	res := &queryJourneyReservation{
		bytes:      byteRoom,
		nodes:      nodeRoom,
		limit:      limit,
		byteBudget: byteRoom - pendingBytes,
		nodeBudget: nodeRoom - pendingNodes,
	}
	state.bytes += res.bytes
	state.nodes += res.nodes
	reg.bytes += res.bytes
	reg.nodes += res.nodes
	state.reservation = res
	return res, nil
}

func (c *Client) settleQueryJourneyReservationLocked(state *queryJourneyState, res *queryJourneyReservation, retainedBytes, retainedNodes int) error {
	if state == nil || res == nil || state.reservation != res {
		return failure("reservation")
	}
	if retainedBytes < 0 || retainedNodes < 0 || retainedBytes > res.bytes || retainedNodes > res.nodes {
		return failure("limit")
	}
	deltaBytes := res.bytes - retainedBytes
	deltaNodes := res.nodes - retainedNodes
	state.bytes -= deltaBytes
	state.nodes -= deltaNodes
	c.registry.bytes -= deltaBytes
	c.registry.nodes -= deltaNodes
	state.reservation = nil
	return nil
}
