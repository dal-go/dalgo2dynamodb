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
snapshot or unlimited whole-journey guarantee.

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
writes, Scan, PartiQL and transactions are **NOT_IMPLEMENTED**. Refused DALgo
callbacks are never invoked. These omissions are not claims that DynamoDB
intrinsically lacks those features.

Defaults per public operation: 30 seconds (maximum 2 minutes), 4 outstanding
SDK calls (maximum 32), 1,000 requested records (including GetMany duplicates),
4,000,000 parsed bytes, 100 pages, 10,000 evaluated records and 100 evaluated
records per SDK Query page. Hard bounds also cover recursion depth32, 100,000
value nodes, individual list/set/map sizes and configuration sizes. Input keys
and copied continuation/value work also consume the parsed-byte budget.
Oversize, malformed or foreign replies fail closed; no silent truncation or
filtering. Query page/work exhaustion returns an error rather than partial
success. Limits do not estimate billing or validate hosted IAM scope.

**Parsed-result limits are not wire allocation limits.** The borrowed SDK has
already received and decoded its response before this adapter checks it.
Configure transport response limits, retry policy and credentials separately;
use a narrowly scoped IAM principal with GetItem/Query only for allowed tables.
The mapping is not an IAM replacement. Credentials stay inside the SDK, never
in endpoint query strings or adapter error messages.

The operation deadline starts before waiting for admission or invoking the SDK,
so credential acquisition and send waits are included. Caller cancellation and
deadline errors retain standard context sentinel identity, without raw causes.
Other failures expose only fixed safe error codes. If a custom borrowed client
ignores its context, the caller still returns promptly and that call holds its
admission slot until it really finishes. At most the configured number of such
calls can exist. `Close` stops admission and cancels active contexts, but cannot
force an uncooperative borrowed SDK to terminate or close its shared transport.

Tests include the actual official SDK signing requests to owned loopback TLS
fixtures, native values, cancellation after observed dispatch, disconnect and
subsequent reads. Those fixtures are **not DynamoDB Local or an AWS account**.
Actual DynamoDB Local semantics, hosted IAM/STS, capacity/billing, installed CLI,
registry release and application integrations remain unverified by this source.

Validation commands (run in the admitted container):

```sh
go mod tidy
gofmt -w *.go
go test -race -count=1 -timeout=90s ./...
go vet ./...
go mod verify
```
