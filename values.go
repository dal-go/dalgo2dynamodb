package dalgo2dynamodb

import (
	"bytes"
	"encoding/base64"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var numberSyntax = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

func number(value string) (*big.Rat, bool) {
	if len(value) > 128 || !numberSyntax.MatchString(value) {
		return nil, false
	}
	if i := strings.IndexAny(value, "eE"); i >= 0 {
		exponent, err := strconv.Atoi(value[i+1:])
		if err != nil || exponent < -130 || exponent > 130 {
			return nil, false
		}
	}
	r, ok := new(big.Rat).SetString(value)
	return r, ok
}

func cloneItem(o *operation, item Item, depth int) (Item, error) {
	if depth > 32 || len(item) > 10_000 {
		return nil, failure("limit")
	}
	if err := o.charge(len(item)); err != nil {
		return nil, err
	}
	result := make(Item, len(item))
	for name, value := range item {
		if name == "" || !utf8.ValidString(name) {
			return nil, failure("invalid-response")
		}
		if err := o.charge(len(name)); err != nil {
			return nil, err
		}
		cloned, err := cloneValue(o, value, depth+1)
		if err != nil {
			return nil, err
		}
		result[name] = cloned
	}
	return result, o.check()
}

func cloneValue(o *operation, value types.AttributeValue, depth int) (types.AttributeValue, error) {
	if depth > 32 {
		return nil, failure("limit")
	}
	if err := o.charge(1); err != nil {
		return nil, err
	}
	switch v := value.(type) {
	case *types.AttributeValueMemberS:
		if v == nil || !utf8.ValidString(v.Value) {
			return nil, failure("invalid-response")
		}
		if err := o.charge(len(v.Value)); err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberS{Value: v.Value}, nil
	case *types.AttributeValueMemberN:
		if v == nil {
			return nil, failure("invalid-response")
		}
		if _, ok := number(v.Value); !ok {
			return nil, failure("invalid-response")
		}
		if err := o.charge(len(v.Value)); err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberN{Value: v.Value}, nil
	case *types.AttributeValueMemberB:
		if v == nil {
			return nil, failure("invalid-response")
		}
		if err := o.charge(len(v.Value)); err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberB{Value: bytes.Clone(v.Value)}, nil
	case *types.AttributeValueMemberBOOL:
		if v == nil {
			return nil, failure("invalid-response")
		}
		return &types.AttributeValueMemberBOOL{Value: v.Value}, nil
	case *types.AttributeValueMemberNULL:
		if v == nil || !v.Value {
			return nil, failure("invalid-response")
		}
		return &types.AttributeValueMemberNULL{Value: true}, nil
	case *types.AttributeValueMemberM:
		if v == nil {
			return nil, failure("invalid-response")
		}
		m, err := cloneItem(o, v.Value, depth)
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberM{Value: m}, nil
	case *types.AttributeValueMemberL:
		if v == nil || len(v.Value) > 10_000 {
			return nil, failure("invalid-response")
		}
		if err := o.charge(len(v.Value)); err != nil {
			return nil, err
		}
		list := make([]types.AttributeValue, len(v.Value))
		for i, entry := range v.Value {
			var err error
			list[i], err = cloneValue(o, entry, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return &types.AttributeValueMemberL{Value: list}, nil
	case *types.AttributeValueMemberSS:
		if v == nil || len(v.Value) == 0 || len(v.Value) > 10_000 {
			return nil, failure("invalid-response")
		}
		result := make([]string, len(v.Value))
		seen := make(map[string]bool, len(v.Value))
		for i, s := range v.Value {
			if !utf8.ValidString(s) || seen[s] {
				return nil, failure("invalid-response")
			}
			seen[s] = true
			if err := o.charge(len(s) + 1); err != nil {
				return nil, err
			}
			result[i] = s
		}
		return &types.AttributeValueMemberSS{Value: result}, nil
	case *types.AttributeValueMemberNS:
		if v == nil || len(v.Value) == 0 || len(v.Value) > 10_000 {
			return nil, failure("invalid-response")
		}
		result := make([]string, len(v.Value))
		seen := make(map[string]bool, len(v.Value))
		for i, s := range v.Value {
			n, ok := number(s)
			if !ok || seen[n.RatString()] {
				return nil, failure("invalid-response")
			}
			seen[n.RatString()] = true
			if err := o.charge(len(s) + 1); err != nil {
				return nil, err
			}
			result[i] = s
		}
		return &types.AttributeValueMemberNS{Value: result}, nil
	case *types.AttributeValueMemberBS:
		if v == nil || len(v.Value) == 0 || len(v.Value) > 10_000 {
			return nil, failure("invalid-response")
		}
		result := make([][]byte, len(v.Value))
		seen := make(map[string]bool, len(v.Value))
		for i, binary := range v.Value {
			if err := o.charge(len(binary) + 1); err != nil {
				return nil, err
			}
			if seen[string(binary)] {
				return nil, failure("invalid-response")
			}
			seen[string(binary)] = true
			result[i] = bytes.Clone(binary)
		}
		return &types.AttributeValueMemberBS{Value: result}, nil
	default:
		return nil, failure("invalid-response")
	}
}

func keyValue(value types.AttributeValue) bool {
	switch v := value.(type) {
	case *types.AttributeValueMemberS:
		return v != nil && v.Value != "" && utf8.ValidString(v.Value)
	case *types.AttributeValueMemberB:
		return v != nil && len(v.Value) > 0
	case *types.AttributeValueMemberN:
		if v == nil {
			return false
		}
		_, ok := number(v.Value)
		return ok
	default:
		return false
	}
}

func cloneKey(o *operation, table Table, key Item) (Item, error) {
	n := 1
	if table.SortKey != "" {
		n++
	}
	if len(key) != n || !keyValue(key[table.PartitionKey]) || (table.SortKey != "" && !keyValue(key[table.SortKey])) {
		return nil, failure("key")
	}
	return cloneItem(o, key, 0)
}
func responseKey(o *operation, table Table, key Item) (Item, error) {
	cloned, err := cloneKey(o, table, key)
	if typed, ok := err.(*Error); ok && typed.Code == "key" {
		return nil, failure("invalid-response")
	}
	return cloned, err
}
func compareKeyValue(a, b types.AttributeValue) (int, bool) {
	switch x := a.(type) {
	case *types.AttributeValueMemberS:
		y, ok := b.(*types.AttributeValueMemberS)
		if !ok || x == nil || y == nil {
			return 0, false
		}
		return strings.Compare(x.Value, y.Value), true
	case *types.AttributeValueMemberB:
		y, ok := b.(*types.AttributeValueMemberB)
		if !ok || x == nil || y == nil {
			return 0, false
		}
		return bytes.Compare(x.Value, y.Value), true
	case *types.AttributeValueMemberN:
		y, ok := b.(*types.AttributeValueMemberN)
		if !ok || x == nil || y == nil {
			return 0, false
		}
		xn, xo := number(x.Value)
		yn, yo := number(y.Value)
		if !xo || !yo {
			return 0, false
		}
		return xn.Cmp(yn), true
	default:
		return 0, false
	}
}
func equalKeyValue(a, b types.AttributeValue) bool {
	n, ok := compareKeyValue(a, b)
	return ok && n == 0
}
func scalarIdentity(value types.AttributeValue) string {
	switch v := value.(type) {
	case *types.AttributeValueMemberS:
		return "S" + strconv.Itoa(len(v.Value)) + ":" + v.Value
	case *types.AttributeValueMemberB:
		return "B" + base64.StdEncoding.EncodeToString(v.Value)
	case *types.AttributeValueMemberN:
		n, _ := number(v.Value)
		return "N" + n.RatString()
	default:
		return ""
	}
}
func keyIdentity(key Item) string {
	names := make([]string, 0, len(key))
	for name := range key {
		names = append(names, name)
	}
	sort.Strings(names)
	var s strings.Builder
	for _, name := range names {
		value := scalarIdentity(key[name])
		s.WriteString(strconv.Itoa(len(name)))
		s.WriteByte(':')
		s.WriteString(name)
		s.WriteString(strconv.Itoa(len(value)))
		s.WriteByte(':')
		s.WriteString(value)
	}
	return s.String()
}
