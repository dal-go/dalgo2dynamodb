package dalgo2dynamodb

import (
	"context"
	"time"
)

const scanJourneyTokenDomain = "dalgo2dynamodb/scan-journey/token\x00"

// OpenScanJourney validates, copies and reserves one locally admitted
// unfiltered Scan journey over an explicitly granted collection. It performs no
// authentication, credential, network or SDK call at open time, so local
// admission is neither service-identity nor hosted-IAM proof. The original
// expiry is the earliest of the Open start plus Options.Timeout, the configured
// TTL and the initial caller deadline; pages and retries never renew it.
func (c *Client) OpenScanJourney(ctx context.Context, request ScanRequest) (*ReadCursor, error) {
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
	expires := start.Add(c.options.Timeout)
	if ttl := start.Add(profile.TTL); ttl.Before(expires) {
		expires = ttl
	}
	callerDone := ctx.Done()
	callerDeadline, hasCallerDeadline := ctx.Deadline()
	if hasCallerDeadline && callerDeadline.Before(expires) {
		expires = callerDeadline
	}
	plan, err := c.compileScanJourney(o, request)
	if err != nil {
		return nil, c.openJourneyFailure(ctx, o, err)
	}
	return c.openJourneyCommon(ctx, o, plan.common, expires, callerDone, callerDeadline, hasCallerDeadline, true, plan.evaluatedLimit)
}

// validateScanJourneyProgress validates only the native continuation token of
// one already closed and key-validated Scan page. Repeated item values within
// or across pages are deliberately not deduplicated and remain part of the
// delivered row count. An empty continuation key yields no progress; a nonempty
// one is identified by the existing canonical identity under a distinct
// Scan-token domain and is rejected when it repeats the start key or an already
// retained token.
func validateScanJourneyProgress(o *operation, start Item, seen map[[32]byte]struct{}, page queryJourneyNativePage) ([][32]byte, error) {
	if len(page.next) == 0 {
		if err := o.check(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	identity := keyIdentity(page.next)
	if len(start) > 0 && identity == keyIdentity(start) {
		return nil, failure("cursor")
	}
	digest := queryJourneyDigest(scanJourneyTokenDomain, identity)
	if _, ok := seen[digest]; ok {
		return nil, failure("cursor")
	}
	if err := o.charge(32); err != nil {
		return nil, err
	}
	if err := o.check(); err != nil {
		return nil, err
	}
	return [][32]byte{digest}, nil
}
