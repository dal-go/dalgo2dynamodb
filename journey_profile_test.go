package dalgo2dynamodb

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func TestJourneyProfileNilAndDefaults(t *testing.T) {
	if c := newTestClient(t, &fakeAPI{}, nil); c.options.Journeys != nil {
		t.Fatal("nil profile did not stay nil")
	}
	authority := ReadAuthority{PrincipalScope: " tenant-a ", Generation: "gen-1", ResourceScope: "table-a"}
	input := &JourneyOptions{Authority: authority}
	c := newTestClient(t, &fakeAPI{}, func(o *Options) { o.Journeys = input })
	got := c.options.Journeys
	if got == input || got.Authority != authority || input.TTL != 0 || input.MaxOpen != 0 || input.ScanCollections != nil {
		t.Fatal("caller profile was mutated or retained")
	}
	if got.TTL != 2*time.Minute || got.MaxOpen != 64 || got.MaxRetainedBytes != 16*1024*1024 || got.MaxRetainedNodes != 65536 || got.MaxPageNodes != 8192 || got.MaxDeliveryAttempts != 128 || got.ScanCollections != nil {
		t.Fatal("defaults were not normalized")
	}
}

func TestJourneyProfileIsolationAfterNew(t *testing.T) {
	grants := []string{"items", "events"}
	input := &JourneyOptions{Authority: ReadAuthority{PrincipalScope: "p", Generation: "g", ResourceScope: "r"}, ScanCollections: grants}
	c := newTestClient(t, &fakeAPI{}, func(o *Options) { o.Journeys = input })
	input.Authority.PrincipalScope = "changed"
	input.ScanCollections[0], input.ScanCollections[1] = "events", "items"
	input.MaxOpen = 1
	got := c.options.Journeys
	if got.Authority.PrincipalScope != "p" || got.MaxOpen != 64 || !reflect.DeepEqual(got.ScanCollections, []string{"items", "events"}) {
		t.Fatal("stored profile or copied grant retained caller state")
	}
}

func TestJourneyProfileRejectsInvalidConfiguration(t *testing.T) {
	valid := ReadAuthority{PrincipalScope: "p", Generation: "g", ResourceScope: "r"}
	for _, tc := range []struct {
		name  string
		amend func(*JourneyOptions)
	}{
		{"blank-authority", func(o *JourneyOptions) { o.Authority.Generation = " \t " }},
		{"control-authority", func(o *JourneyOptions) { o.Authority.PrincipalScope = "p\x00" }},
		{"invalid-utf8", func(o *JourneyOptions) { o.Authority.ResourceScope = string([]byte{0xff}) }},
		{"long-authority", func(o *JourneyOptions) { o.Authority.PrincipalScope = strings.Repeat("x", 257) }},
		{"long-whitespace-authority", func(o *JourneyOptions) { o.Authority.PrincipalScope = strings.Repeat(" ", 257) }},
		{"short-ttl", func(o *JourneyOptions) { o.TTL = time.Millisecond - 1 }},
		{"long-ttl", func(o *JourneyOptions) { o.TTL = 2*time.Minute + 1 }},
		{"negative-open", func(o *JourneyOptions) { o.MaxOpen = -1 }},
		{"excess-open", func(o *JourneyOptions) { o.MaxOpen = 65 }},
		{"small-retained", func(o *JourneyOptions) { o.MaxRetainedNodes = 1 }},
		{"page-equals-retained", func(o *JourneyOptions) { o.MaxRetainedNodes, o.MaxPageNodes = 8, 8 }},
		{"excess-page", func(o *JourneyOptions) { o.MaxRetainedNodes, o.MaxPageNodes = 10_000, 8193 }},
		{"negative-attempts", func(o *JourneyOptions) { o.MaxDeliveryAttempts = -1 }},
		{"unknown-grant", func(o *JourneyOptions) { o.ScanCollections = []string{"missing"} }},
		{"overlong-grant", func(o *JourneyOptions) { o.ScanCollections = []string{strings.Repeat("x", 256)} }},
		{"duplicate-grant", func(o *JourneyOptions) { o.ScanCollections = []string{"items", "items"} }},
		{"too-many-grants", func(o *JourneyOptions) { o.ScanCollections = []string{"items", "events", "extra"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			journey := &JourneyOptions{Authority: valid}
			tc.amend(journey)
			options := Options{Tables: map[string]Table{"items": {Name: "t1", PartitionKey: "account"}, "events": {Name: "t2", PartitionKey: "account"}}, Journeys: journey}
			if _, err := New(&fakeAPI{}, options); err == nil {
				t.Fatal("invalid journey profile accepted")
			} else {
				requireCode(t, err, "configuration")
			}
		})
	}
}

func TestJourneyProfileAcceptsReducedBounds(t *testing.T) {
	input := &JourneyOptions{Authority: ReadAuthority{PrincipalScope: "p", Generation: "g", ResourceScope: "r"}, MaxRetainedNodes: 32}
	c := newTestClient(t, &fakeAPI{}, func(o *Options) { o.Journeys = input })
	if got := c.options.Journeys; got.MaxRetainedNodes != 32 || got.MaxPageNodes != 31 {
		t.Fatal("derived page ceiling not bounded by reduced retained nodes")
	}
	input.MaxPageNodes = 8
	c = newTestClient(t, &fakeAPI{}, func(o *Options) { o.Journeys = input })
	if got := c.options.Journeys; got.MaxRetainedNodes != 32 || got.MaxPageNodes != 8 || got.MaxOpen != 64 {
		t.Fatal("explicit reduced page ceiling rejected")
	}
}

func TestJourneyProfileConstructorPerformsNoIO(t *testing.T) {
	calls := 0
	api := &fakeAPI{
		get: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			calls++
			return nil, nil
		},
		query: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			calls++
			return nil, nil
		},
	}
	c := newTestClient(t, api, func(o *Options) {
		o.Journeys = &JourneyOptions{Authority: ReadAuthority{PrincipalScope: "p", Generation: "g", ResourceScope: "r"}, ScanCollections: []string{"items"}}
	})
	if c == nil || calls != 0 {
		t.Fatalf("valid constructor performed I/O: %d", calls)
	}
	refusal := Options{Tables: map[string]Table{"items": {Name: "existing", PartitionKey: "account"}}, Journeys: &JourneyOptions{Authority: ReadAuthority{PrincipalScope: "p", Generation: "g", ResourceScope: "r"}, ScanCollections: []string{strings.Repeat("x", 256)}}}
	if _, err := New(api, refusal); err == nil {
		t.Fatal("refusal setup accepted")
	}
	if calls != 0 {
		t.Fatalf("refusal constructor performed I/O: %d", calls)
	}
}
