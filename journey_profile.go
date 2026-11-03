package dalgo2dynamodb

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// ReadAuthority is a trusted DECLARED logical scope only. It performs no SDK or
// IAM identity detection, introspection or hosted proof. The signing identity,
// resource scope and borrowed SDK retry contract must remain fixed while the
// journey lives; changing any underlying identity, resource or retry profile
// requires a new client. The adapter never autoloads admin credentials, a
// default chain or grants.
type ReadAuthority struct {
	PrincipalScope string
	Generation     string
	ResourceScope  string
}

// JourneyOptions is active opt-in configuration for OpenQueryJourney. Values
// are validated and copied at construction. Defaults are a 2-minute TTL, 64
// open journeys, 16 MiB of logical retained bytes, 65536 logically retained
// nodes, page nodes capped at 8192 and 128 whole-journey delivery attempts; a
// configured hard ceiling may only lower these limits. These are logical
// adapter bounds, not Go heap, cgroup, SDK wire-allocation or billing bounds.
// Scan, filters and projections are NOT_IMPLEMENTED later stages.
type JourneyOptions struct {
	Authority           ReadAuthority
	ScanCollections     []string
	TTL                 time.Duration
	MaxOpen             int
	MaxRetainedBytes    int
	MaxRetainedNodes    int
	MaxPageNodes        int
	MaxDeliveryAttempts int
}

func copyJourneyOptions(input *JourneyOptions, tables map[string]Table) (*JourneyOptions, error) {
	if input == nil {
		return nil, nil
	}
	profile := *input
	if !validAuthority(profile.Authority.PrincipalScope) || !validAuthority(profile.Authority.Generation) || !validAuthority(profile.Authority.ResourceScope) {
		return nil, failure("configuration")
	}
	if profile.TTL == 0 {
		profile.TTL = 2 * time.Minute
	}
	if profile.TTL < time.Millisecond || profile.TTL > 2*time.Minute {
		return nil, failure("configuration")
	}
	var err error
	if profile.MaxOpen, err = journeyCount(profile.MaxOpen, 64, 64); err != nil {
		return nil, err
	}
	if profile.MaxRetainedBytes, err = journeyCount(profile.MaxRetainedBytes, 16*1024*1024, 16*1024*1024); err != nil {
		return nil, err
	}
	if profile.MaxRetainedNodes, err = journeyCount(profile.MaxRetainedNodes, 65536, 65536); err != nil || profile.MaxRetainedNodes < 2 {
		return nil, failure("configuration")
	}
	if profile.MaxDeliveryAttempts, err = journeyCount(profile.MaxDeliveryAttempts, 128, 128); err != nil {
		return nil, err
	}
	if profile.MaxPageNodes == 0 {
		profile.MaxPageNodes = min(8192, profile.MaxRetainedNodes-1)
	} else if profile.MaxPageNodes < 1 || profile.MaxPageNodes > 8192 || profile.MaxPageNodes >= profile.MaxRetainedNodes {
		return nil, failure("configuration")
	}
	if len(input.ScanCollections) > len(tables) {
		return nil, failure("configuration")
	}
	profile.ScanCollections = append([]string(nil), input.ScanCollections...)
	seen := make(map[string]bool, len(profile.ScanCollections))
	for _, collection := range profile.ScanCollections {
		if !validName(collection) || seen[collection] {
			return nil, failure("configuration")
		}
		if _, ok := tables[collection]; !ok {
			return nil, failure("configuration")
		}
		seen[collection] = true
	}
	return &profile, nil
}

func validAuthority(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	if strings.TrimSpace(s) == "" || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func journeyCount(value, fallback, ceiling int) (int, error) {
	if value == 0 {
		value = fallback
	}
	if value < 1 || value > ceiling {
		return 0, failure("configuration")
	}
	return value, nil
}
