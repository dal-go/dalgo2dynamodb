package dalgo2dynamodb

import (
	"context"
	"sync"
	"time"
)

// ReadCursor is an opaque, single-use, process-local handle for one locally
// admitted query journey. It is bound to the issuing Client and cannot be
// persisted, reconstructed, decoded from caller JSON or shared across
// processes. A successful page acknowledgement consumes the old handle and, for
// a nonterminal page, returns an internally allocated successor; there is no
// portable continuation token.
type ReadCursor struct {
	owner *Client
}

type queryJourneyCapacity struct {
	pages    int
	reported int
	units    CapacitySum
	read     CapacitySum
	write    CapacitySum
}

type queryJourneyState struct {
	plan           partitionQueryPlan
	expires        time.Time
	bytes          int
	nodes          int
	terminal       bool
	logical        int
	physical       int
	work           queryJourneyAccounting
	pending        *queryJourneyNativePage
	reservation    *queryJourneyReservation
	current        *ReadCursor
	key            Item
	seen           map[[32]byte]struct{}
	keyCost        queryJourneyCharge
	pendingCost    queryJourneyCharge
	historyCost    queryJourneyCharge
	capacity       queryJourneyCapacity
	attempts       int
	scan           bool
	evaluatedLimit int
}

type readJourneyRegistry struct {
	mu     sync.Mutex
	open   map[*ReadCursor]*queryJourneyState
	bytes  int
	nodes  int
	closed bool
}

// OpenQueryJourney validates, copies and reserves one local query journey. It
// performs no authentication, credential, network or SDK call, so local
// admission is neither service-identity nor hosted-IAM proof. The original
// expiry is the earliest of the Open start plus Options.Timeout, the configured
// TTL and the initial caller deadline; pages and retries never renew it, and a
// later caller may only shorten it. Use NextReadPage/NextReadPageWith to fetch
// and acknowledge pages, and CloseReadJourney or Close to release the journey.
// Expired states are reclaimed lazily on a later registry operation or Close;
// there is no background cleanup goroutine. Scan is NOT_IMPLEMENTED.
func (c *Client) OpenQueryJourney(ctx context.Context, request QueryRequest) (*ReadCursor, error) {
	start := time.Now()
	o, err := c.begin(ctx)
	if err != nil {
		return nil, c.openJourneyFailure(ctx, nil, err)
	}
	defer o.close()
	profile := c.options.Journeys
	if profile == nil {
		return nil, c.openJourneyFailure(ctx, o, failure("journey-disabled"))
	}
	if request.Cursor != nil {
		return nil, c.openJourneyFailure(ctx, o, failure("cursor"))
	}
	expires := start.Add(c.options.Timeout)
	if ttl := start.Add(profile.TTL); ttl.Before(expires) {
		expires = ttl
	}
	callerDone := ctx.Done()
	callerDeadline, hasCallerDeadline := ctx.Deadline()
	if hasCallerDeadline && callerDeadline.Before(expires) {
		expires = callerDeadline
	}
	plan, err := c.compilePartitionQuery(o, request)
	if err != nil {
		return nil, c.openJourneyFailure(ctx, o, err)
	}
	return c.openJourneyCommon(ctx, o, plan, expires, callerDone, callerDeadline, hasCallerDeadline, false, 0)
}

// openJourneyCommon performs the shared, already-reviewed admission and final
// owned registration for one journey, after the caller has validated mode,
// compiled the plan and captured the original expiry and caller fields. It
// closes o before fresh classification or commit, keeps the query charge
// unchanged and adds only the private scan/evaluatedLimit state for a Scan
// journey.
func (c *Client) openJourneyCommon(ctx context.Context, o *operation, plan partitionQueryPlan, expires time.Time, callerDone <-chan struct{}, callerDeadline time.Time, hasCallerDeadline bool, scan bool, evaluatedLimit int) (*ReadCursor, error) {
	profile := c.options.Journeys
	bytes, nodes := journeyCharge(&plan, profile, o)
	if scan {
		bytes += 64
		nodes++
	}
	work := queryJourneyAccounting{parsedBytes: o.bytes, parsedNodes: o.nodes}
	o.close()

	if err := active(ctx); err != nil {
		return nil, c.openJourneyFailure(ctx, o, err)
	}
	now := time.Now()
	if !now.Before(expires) {
		return nil, c.openJourneyFailure(ctx, o, context.DeadlineExceeded)
	}
	if c.life.Err() != nil {
		return nil, c.openJourneyFailure(ctx, o, failure("closed"))
	}

	cursor, err := func() (*ReadCursor, error) {
		reg := &c.registry
		reg.mu.Lock()
		defer reg.mu.Unlock()
		now := time.Now()
		c.pruneReadJourneysLocked(now)
		if reg.closed || c.life.Err() != nil {
			return nil, failure("closed")
		}
		if !now.Before(expires) {
			return nil, context.DeadlineExceeded
		}
		select {
		case <-callerDone:
			if hasCallerDeadline && !time.Now().Before(callerDeadline) {
				return nil, context.DeadlineExceeded
			}
			return nil, context.Canceled
		default:
		}
		if len(reg.open) >= profile.MaxOpen {
			return nil, failure("limit")
		}
		if bytes < 0 || nodes < 1 || bytes > profile.MaxRetainedBytes-reg.bytes || nodes > profile.MaxRetainedNodes-reg.nodes {
			return nil, failure("limit")
		}
		if !time.Now().Before(expires) {
			return nil, context.DeadlineExceeded
		}
		if reg.open == nil {
			reg.open = make(map[*ReadCursor]*queryJourneyState, profile.MaxOpen)
		}
		cursor := &ReadCursor{owner: c}
		reg.open[cursor] = &queryJourneyState{plan: plan, expires: expires, bytes: bytes, nodes: nodes, work: work, current: cursor, scan: scan, evaluatedLimit: evaluatedLimit}
		reg.bytes += bytes
		reg.nodes += nodes
		return cursor, nil
	}()
	if err != nil {
		return nil, c.openJourneyFailure(ctx, o, err)
	}
	return cursor, nil
}

// readJourneyEvaluatedLimit returns the journey's admitted evaluated-item
// ceiling. A Query journey keeps the configured global cap; a Scan journey uses
// its explicit request ceiling without ever raising it.
func (c *Client) readJourneyEvaluatedLimit(state *queryJourneyState) int {
	if state.scan {
		return state.evaluatedLimit
	}
	return c.options.MaxEvaluated
}

// openJourneyFailure completes owned cleanup before classifying a failed open.
// The original caller cancellation/deadline takes priority, then Client
// closure; otherwise the original sanitized error is preserved. The operation
// is nil when begin itself failed, and a nil caller context is not inspected.
func (c *Client) openJourneyFailure(ctx context.Context, o *operation, original error) error {
	if o != nil {
		o.close()
	}
	if ctx != nil {
		if err := active(ctx); err != nil {
			return err
		}
		if c.life.Err() != nil {
			return failure("closed")
		}
	}
	return original
}

// CloseReadJourney releases one locally admitted journey and its logical
// retained-byte/node charge. It performs no SDK, network or credential I/O.
// Closing an already closed or expired same-client cursor is a no-op. A nil,
// foreign or forged cursor returns a fixed cursor error and never exposes plan
// state.
func (c *Client) CloseReadJourney(cursor *ReadCursor) error {
	if cursor == nil || cursor.owner != c {
		return failure("cursor")
	}
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	c.pruneReadJourneysLocked(time.Now())
	state, ok := reg.open[cursor]
	if !ok {
		return nil
	}
	c.terminateReadJourneyLocked(cursor, state)
	return nil
}

// journeyCharge returns a conservative logical cost for one admitted journey.
// It covers a fixed state allowance, declared authority and collection names,
// table/expression/scope/name metadata, and the native input bytes/nodes
// already charged by the existing compiler. It does not charge a whole
// response or page and makes no exact Go heap, cgroup or SDK wire-allocation
// guarantee; it is not a billing bound.
func journeyCharge(plan *partitionQueryPlan, profile *JourneyOptions, o *operation) (int, int) {
	const fixedBytes = 512
	bytes := fixedBytes
	bytes += len(profile.Authority.PrincipalScope) + len(profile.Authority.Generation) + len(profile.Authority.ResourceScope)
	for _, collection := range profile.ScanCollections {
		bytes += len(collection)
	}
	bytes += len(plan.table.Name) + len(plan.table.PartitionKey) + len(plan.table.SortKey)
	bytes += len(plan.expression) + len(plan.scope)
	for name, value := range plan.names {
		bytes += len(name) + len(value)
	}
	bytes += o.bytes
	nodes := 1 + o.nodes
	return bytes, nodes
}

// terminateReadJourneyLocked forbids new ownership. The state stays as a
// tombstone, preserving already acknowledged history, and its original charge
// is refunded exactly once, only after both logical and physical counts reach
// zero. A borrowed SDK call that ignores context keeps the physical count until
// it actually returns.
func (c *Client) terminateReadJourneyLocked(cursor *ReadCursor, state *queryJourneyState) {
	state.terminal = true
	c.finalizeReadJourneyLocked(cursor, state)
}

func (c *Client) finalizeReadJourneyLocked(_ *ReadCursor, state *queryJourneyState) {
	if !state.terminal || state.logical != 0 || state.physical != 0 {
		return
	}
	reg := &c.registry
	if reg.open[state.current] != state {
		return
	}
	delete(reg.open, state.current)
	reg.bytes -= state.bytes
	reg.nodes -= state.nodes
}

func (c *Client) pruneReadJourneysLocked(now time.Time) {
	for cursor, state := range c.registry.open {
		if !now.Before(state.expires) {
			c.terminateReadJourneyLocked(cursor, state)
		}
	}
}

func (c *Client) closeReadJourneys() {
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.closed = true
	for cursor, state := range reg.open {
		c.terminateReadJourneyLocked(cursor, state)
	}
}

// beginReadJourneyCall privately acquires the single logical call a journey
// state may own for one public NextReadPage/NextReadPageWith invocation. It is
// not itself an ACK. Activity is checked before the lock; under the lock only
// captured Done/deadline, c.life, time.Now and owned state are inspected. The
// returned end func runs outside the lock and is required on every
// post-acquisition path.
func (c *Client) beginReadJourneyCall(ctx context.Context, cursor *ReadCursor) (*queryJourneyState, func(), error) {
	if ctx == nil {
		return nil, nil, failure("input")
	}
	if err := active(ctx); err != nil {
		return nil, nil, err
	}
	if cursor == nil || cursor.owner != c {
		return nil, nil, failure("cursor")
	}
	callerDone := ctx.Done()
	callerDeadline, hasCallerDeadline := ctx.Deadline()
	reg := &c.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	state, ok := reg.open[cursor]
	if !ok {
		return nil, nil, failure("cursor")
	}
	if reg.closed || c.life.Err() != nil {
		return nil, nil, failure("closed")
	}
	now := time.Now()
	if !now.Before(state.expires) {
		c.terminateReadJourneyLocked(cursor, state)
		return nil, nil, context.DeadlineExceeded
	}
	if state.terminal {
		return nil, nil, failure("cursor")
	}
	select {
	case <-callerDone:
		if hasCallerDeadline && !time.Now().Before(callerDeadline) {
			return nil, nil, context.DeadlineExceeded
		}
		return nil, nil, context.Canceled
	default:
	}
	if state.logical != 0 {
		return nil, nil, failure("busy")
	}
	state.logical = 1
	return state, func() {
		reg := &c.registry
		reg.mu.Lock()
		defer reg.mu.Unlock()
		state.logical--
		c.finalizeReadJourneyLocked(cursor, state)
	}, nil
}

// readJourneyPhysicalRetain privately produces the operation retain hook for a
// state that already owns a logical call. It claims one physical invocation
// before the SDK is dispatched and its returned release must run only after the
// borrowed SDK call actually returns, so an SDK that ignores context keeps its
// credit until the real return. Time/Close may invalidate admission; late
// results are fenced by NextReadPage/NextReadPageWith and never replayed here.
func (c *Client) readJourneyPhysicalRetain(cursor *ReadCursor, state *queryJourneyState) func() (func(), error) {
	return func() (func(), error) {
		reg := &c.registry
		reg.mu.Lock()
		defer reg.mu.Unlock()
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
		state.physical++
		return func() {
			reg := &c.registry
			reg.mu.Lock()
			defer reg.mu.Unlock()
			state.physical--
			c.finalizeReadJourneyLocked(cursor, state)
		}, nil
	}
}
