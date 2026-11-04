package dalgo2dynamodb

import (
	"crypto/sha256"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	queryJourneyRowDomain   = "dalgo2dynamodb/query-journey/row\x00"
	queryJourneyTokenDomain = "dalgo2dynamodb/query-journey/token\x00"
)

func queryJourneyDigest(domain, identity string) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write([]byte(identity))
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func queryJourneySortAdvanced(value, previous types.AttributeValue, descending, strict bool) bool {
	if previous == nil {
		return true
	}
	cmp, ok := compareKeyValue(value, previous)
	if !ok {
		return false
	}
	if descending {
		if strict {
			return cmp < 0
		}
		return cmp <= 0
	}
	if strict {
		return cmp > 0
	}
	return cmp >= 0
}

func validateQueryJourneyProgress(o *operation, plan partitionQueryPlan, start Item, seen map[[32]byte]struct{}, page queryJourneyNativePage) ([][32]byte, error) {
	table := plan.table
	var previousSort types.AttributeValue
	if table.SortKey != "" && len(start) > 0 {
		previousSort = start[table.SortKey]
	}
	var progress [][32]byte
	local := make(map[[32]byte]struct{})
	for _, item := range page.items {
		key := Item{table.PartitionKey: item[table.PartitionKey]}
		if table.SortKey != "" {
			key[table.SortKey] = item[table.SortKey]
		}
		if !keyValue(key[table.PartitionKey]) || (table.SortKey != "" && !keyValue(key[table.SortKey])) {
			return nil, failure("invalid-response")
		}
		if table.SortKey != "" {
			if !queryJourneySortAdvanced(key[table.SortKey], previousSort, plan.descending, true) {
				return nil, failure("invalid-response")
			}
			previousSort = key[table.SortKey]
		} else if len(start) > 0 && keyIdentity(key) == keyIdentity(start) {
			return nil, failure("invalid-response")
		}
		digest := queryJourneyDigest(queryJourneyRowDomain, keyIdentity(key))
		if _, ok := seen[digest]; ok {
			return nil, failure("invalid-response")
		}
		if _, ok := local[digest]; ok {
			return nil, failure("invalid-response")
		}
		if err := o.charge(32); err != nil {
			return nil, err
		}
		local[digest] = struct{}{}
		progress = append(progress, digest)
	}
	if len(page.next) == 0 {
		if err := o.check(); err != nil {
			return nil, err
		}
		return progress, nil
	}
	next := page.next
	if !keyValue(next[table.PartitionKey]) || (table.SortKey != "" && !keyValue(next[table.SortKey])) {
		return nil, failure("invalid-response")
	}
	if table.SortKey != "" {
		if !queryJourneySortAdvanced(next[table.SortKey], previousSort, plan.descending, false) {
			return nil, failure("invalid-response")
		}
		if len(start) > 0 && !queryJourneySortAdvanced(next[table.SortKey], start[table.SortKey], plan.descending, true) {
			return nil, failure("cursor")
		}
	} else if len(start) > 0 && keyIdentity(next) == keyIdentity(start) {
		return nil, failure("cursor")
	}
	digest := queryJourneyDigest(queryJourneyTokenDomain, keyIdentity(next))
	if _, ok := seen[digest]; ok {
		return nil, failure("cursor")
	}
	if _, ok := local[digest]; ok {
		return nil, failure("cursor")
	}
	if err := o.charge(32); err != nil {
		return nil, err
	}
	progress = append(progress, digest)
	if err := o.check(); err != nil {
		return nil, err
	}
	return progress, nil
}
