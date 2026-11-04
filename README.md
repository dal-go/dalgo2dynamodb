# Native DynamoDB reads for DALgo

This Go adapter reads **existing DynamoDB tables with explicitly configured key
attributes**. It does not require a hidden DALgo item envelope. Supply a borrowed
official AWS SDK v2 `*dynamodb.Client` with trusted endpoint, region, credentials,
retry and HTTP settings. Construction performs no network or credential work.

```go
client, err := dalgo2dynamodb.New(sdk, dalgo2dynamodb.Options{
    Tables: map[string]dalgo2dynamodb.Table{
        "orders": {Name: "existing-orders", PartitionKey: "account", SortKey: "id"},
    },
})
// Check err; close adapter admission when finished. The borrowed SDK remains usable.
defer client.Close()
item, err := client.Get(ctx, "orders", dalgo2dynamodb.Item{
    "account": &types.AttributeValueMemberS{Value: "tenant-one"},
    "id": &types.AttributeValueMemberN{Value: "9007199254740993"},
})
```

`Item` retains official `types.AttributeValue` values. Numbers remain decimal
**strings**, binary remains bytes, and native string/number/binary sets, maps,
lists, booleans and NULL remain tagged values. Returned data is recursively
validated and copied. There is no JSON-to-float64 round trip. Native key values
must be nonempty S/B or valid bounded decimal N; numeric key comparisons use
exact rationals while preserving returned number spelling.

`Get` returns nil for an absent item. `Exists` reads the same item. `GetMany`
reads sequentially with one shared deadline and aggregate budget, preserving
input order, duplicate keys and nil entries for missing items. All keys are
validated before the first dispatch. It is not a snapshot or transaction.

`QueryPartition` requires a configured collection, a literal native partition
value and a positive limit. It binds partition equality and optional sort-key
`=`, `<`, `<=`, `>`, `>=`, `between` or S/B `begins_with` conditions with SDK
expression names/values. It never accepts raw expressions, chooses an index,
or falls back to Scan. Empty pages with continuations are followed. Returned
keys, partitions, counts, cursors and sort predicates are checked before
delivery. An opaque in-process `Next` cursor belongs to the issuing client,
mapping, query and direction; it cannot be decoded from caller-controlled JSON.
Each explicit continuation call has its own bounded operation, not a global
snapshot or unlimited whole-journey guarantee. This legacy per-call `Cursor` is
separate from and unchanged by the opt-in journey `ReadCursor` described below;
it is not a single-use process-local journey handle.

`QueryRequest.ReturnConsumedCapacity` is an optional exact selector. Omitted and
`NONE` preserve the legacy result shape and return no capacity report (`NONE` is
still transmitted verbatim). `TOTAL` and `INDEXES` add a per-public-call
`QueryPage.Capacity` receipt built only from the official SDK's decoded `float64`
observations of each native page. Explicit zero stays present while a missing
metric, table or index map stays missing; per-metric
coverage and raw `ScannedCount` are reported next to the legacy `Evaluated` count.
Index and table components are not added into the top-level metric totals, and raw
item `N`/`NS` values remain lossless. The receipt is not an original-journey total,
a billing hard cap, a refund or failure ledger, or AWS/IAM proof. This legacy
per-public-call receipt is separate from the implemented journey **cumulative
observations** receipt described below. Only explicit Scan, soft capacity
budgeting, and any failed-usage or refund guarantee remain **NOT_IMPLEMENTED**
or explicitly not promised for a later stage (neither `UNSUPPORTED` nor
complete); none of these is a claim that the provider is intrinsically
unsupported.

## Opt-in read journeys

`Options.Journeys` enables the active opt-in `ReadCursor` API.
`OpenQueryJourney` validates, copies and reserves one journey locally before any
SDK authorization. It performs no authentication, credential, network or SDK
call, so successful local admission is neither service-identity nor hosted-IAM
proof. `ReadAuthority` is a trusted, **declared** immutable
principal/generation/resource scope plus the borrowed SDK's retry contract; it is
not introspection or an IAM grant. The adapter never autoloads admin
credentials, a default chain or grants. If the underlying signing identity,
resources or retry profile change, create a new client. `OpenQueryJourney`
rejects a legacy `QueryRequest.Cursor`; journeys use only `ReadCursor`.

The original journey expiry is fixed at Open:
`min(Open start + Options.Timeout, Open start + configured TTL, initial caller deadline)`.
Pages and retries never renew it, and a later caller may only shorten it.

```go
client, err := dalgo2dynamodb.New(sdk, dalgo2dynamodb.Options{
    Tables: map[string]dalgo2dynamodb.Table{
        "orders": {Name: "existing-orders", PartitionKey: "account", SortKey: "id"},
    },
    Journeys: &dalgo2dynamodb.JourneyOptions{
        Authority: dalgo2dynamodb.ReadAuthority{
            PrincipalScope: "tenant-one-reader",
            Generation:     "config-v1",
            ResourceScope:  "orders-read",
        },
    },
})
// Check err. The journey API is active only when Journeys is non-nil.
cursor, err := client.OpenQueryJourney(ctx, dalgo2dynamodb.QueryRequest{
    Collection: "orders",
    Partition:  &types.AttributeValueMemberS{Value: "tenant-one"},
    Limit:      500,
})
if err != nil {
    return err
}
defer func() {
    if cursor != nil {
        _ = client.CloseReadJourney(cursor) // idempotent, nonblocking, no SDK I/O
    }
}()
for cursor != nil {
    page, err := client.NextReadPageWith(ctx, cursor, func(page dalgo2dynamodb.ReadPage) error {
        // Synchronous caller-owned staging BEFORE acknowledgement. Make this
        // reversible: a callback is not a transaction or rollback sandbox.
        payload, err := encodePage(page)
        if err != nil {
            return err
        }
        return stageForPublish(payload)
    })
    if err != nil {
        return err
    }
    // Advance first: the successful ACK consumed the old handle, so a publish
    // panic/unwind must leave the defer seeing the actual successor, not the
    // consumed old handle. Publish the prepared frame once. A later partial
    // external write is unknown delivery: never retry this consumed handle or
    // automatically resubmit the paid query.
    cursor = page.Next
    publishStagedOnce()
}
```

`prepare` callbacks are synchronous and caller-owned, not a preemptive CPU
limit, transaction, rollback sandbox or automatic retry engine.
`ReadPage.Receipt` is provisional during preparation and becomes the
acknowledged owner view only after commit. A nonterminal page always carries an
internally allocated successor in `Next`; callers never supply one.

A prepare error, genuine caller abort, or panic recovered by the caller leaves
the valid pending page retained until the original expiry or until
`CloseReadJourney`, with no SDK refetch and no recount of already spent work. An
unknown or in-flight transport loss terminates the journey and is never replayed
implicitly. Successful acknowledgement consumes the old `ReadCursor`; publish
its prepared frame at most once.

`ReadCursor` is process-local, single-use and bound to its issuing client. It
cannot be persisted, reconstructed from caller JSON or shared across processes
or clients. Idle journeys expire honestly at the recorded original expiry;
expired states are reclaimed lazily, without a background goroutine.
`CloseReadJourney` and `Close` are idempotent and nonblocking. A borrowed SDK
that ignores its context keeps its logical/physical credit until it actually
returns; the adapter never destroys the borrowed client.

`HasMore` reports that the native response supplied a `LastEvaluatedKey` (LEK).
This is a continuation possibility, not a promise that the journey can safely
continue. Native EOF always yields `Next == nil` and `ReadStopNativeEOF`, even at
configured caps. An explicit semantic request row limit yields `Next == nil`,
`ReadStopRequestedRowLimit` and `HasMore == true` when a LEK exists; delivered
rows are never silently truncated to fake success. If a definite hard page,
evaluation or original-work budget is exhausted below the semantic row limit
while a LEK exists, the whole current candidate page fails with a fixed `limit`
error before `prepare`/ACK: no fake EOF or successful budget-stop page is
emitted. A prior successful ACK cannot be revoked, and already fetched or known
statistics are not zeroed.

Temporary global byte/node quota exhaustion is reported as `quota`, while a
concurrent call on the same handle is `busy`; both are distinct from a hard
`limit` failure. After capacity credits are refunded, the old valid handle or its
retained pending page may be retried. Journey defaults are 64 open journeys,
16 MiB of logically retained bytes, 65,536 logically retained nodes, 8,192 nodes
per page and 128 delivery attempts for the whole journey. Configuration may
lower these hard ceilings but may not raise them.

Logical retention, copying and native parsed work are accounting
approximations, not exact Go heap, cgroup, SDK wire-allocation or billing
bounds. The native SDK has already received and parsed its response before the
adapter copy guard runs; configure the borrowed SDK's transport, retry and
credential policies separately. When `ReturnConsumedCapacity` is `TOTAL` or
`INDEXES`, journey receipts carry an original-journey cumulative capacity view
plus the current page's SDK detail. Values are the SDK-decoded `float64`
observations checked at fetch; missing metrics stay missing rather than zero,
top-level sums do not double-count index/table breakdowns, and coverage is
observed once. This is not a hard billing or failed-usage guarantee.

The sealed `client.DB()` DALgo facade supports Get/Exists/GetMulti. A string
record ID addresses a table with a string partition key and no sort key.
Composite/native keys use `record.NewKeyWithFields` and `record.FieldVal` values
containing official S/N/B AttributeValues named exactly as configured. Retrieval
targets must be `Item` or `*Item`, retaining native types; arbitrary struct
decoding is not implemented. Missing records use `record.ErrRecordNotFound`.
GetMulti preserves caller record order and marks missing records independently.
It stages SDK reads before populating targets, but caller-owned mutable record
targets are not transactional rollback buffers. Final caller/deadline checks
also run after record callbacks; a failed record must not be read as a success.

Generic structured DALgo queries, recordset readers, schema discovery, indexes,
writes, Scan, PartiQL and transactions are **NOT_IMPLEMENTED**. Explicit Scan,
filter expressions, projections, soft capacity budgeting, CLI and TypeScript
integration are later-stage **NOT_IMPLEMENTED** work, not provider-intrinsic
**UNSUPPORTED** features. Refused DALgo callbacks are never invoked. These
omissions are not claims that DynamoDB intrinsically lacks those features.

Defaults per public operation: 30 seconds (maximum 2 minutes), 4 outstanding
SDK calls (maximum 32), 1,000 requested records (including GetMany duplicates),
4,000,000 parsed bytes, 100 pages, 10,000 evaluated records and 100 evaluated
records per SDK Query page. Hard bounds also cover recursion depth32, 100,000
value nodes, individual list/set/map sizes and configuration sizes. Input keys
and copied continuation/value work also consume the parsed-byte budget.
Oversize, malformed or foreign replies fail closed; no silent truncation or
filtering. Query page/work exhaustion returns an error rather than partial
success. Limits do not estimate billing or validate hosted IAM scope. The
opt-in journey API applies its own lowerable journey defaults on top of these
per-call bounds.

**Parsed-result limits are not wire allocation limits.** The borrowed SDK has
already received and decoded its response before this adapter checks it.
Configure transport response limits, retry policy and credentials separately;
use a narrowly scoped IAM principal with GetItem/Query only for allowed tables.
The mapping is not an IAM replacement. Credentials stay inside the SDK, never
in endpoint query strings or adapter error messages. Borrowed retry/transport
policies remain separate and are not changed by journey admission or receipt
accounting.

The operation deadline starts before waiting for admission or invoking the SDK,
so credential acquisition and send waits are included. Caller cancellation and
deadline errors retain standard context sentinel identity, without raw causes.
Other failures expose only fixed safe error codes. If a custom borrowed client
ignores its context, the caller still returns promptly and that call holds its
admission slot until it really finishes. At most the configured number of such
calls can exist. `Close` stops admission and cancels active contexts, but cannot
force an uncooperative borrowed SDK to terminate or close its shared transport.

Source fixtures exercise the actual official SDK signing requests against owned
loopback TLS endpoints, native values, cancellation after observed dispatch,
disconnect and subsequent reads. Those fixtures are **not DynamoDB Local or an
AWS account**. Actual DynamoDB Local semantics, hosted IAM/STS,
capacity/billing, browser, installed CLI, registry release and application
integrations remain unverified by this source. No registry, tag or main-release
status is asserted here.

Validation commands (run in the admitted container):

```sh
go mod tidy
gofmt -w *.go
go test -race -count=1 -timeout=90s ./...
go vet ./...
go mod verify
```
