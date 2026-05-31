package store

import (
	"bytes"
	"strconv"
	"strings"
)

// MatchMode controls how multiple filters are combined.
// MatchAll applies AND semantics; MatchAny applies OR semantics.
type MatchMode uint8

const (
	MatchAll MatchMode = iota
	MatchAny
)

// QueryOrder controls deterministic result ordering for query ID outputs.
// QueryOrderAsc sorts IDs ascending; QueryOrderDesc sorts descending.
type QueryOrder uint8

const (
	QueryOrderAsc QueryOrder = iota
	QueryOrderDesc
)

// PropertyOp defines the comparison operation for a property filter.
type PropertyOp uint8

const (
	PropertyOpEqual PropertyOp = iota
	PropertyOpPrefix
	PropertyOpContains
	PropertyOpGreaterThan
	PropertyOpGreaterThanOrEqual
	PropertyOpLessThan
	PropertyOpLessThanOrEqual
	PropertyOpBetweenInclusive
)

// PropertyFilter describes a single property constraint.
//
// Value is required for all operators.
// ValueUpper is required for PropertyOpBetweenInclusive.
type PropertyFilter struct {
	Key        string
	Op         PropertyOp
	Value      []byte
	ValueUpper []byte
}

// NodeQuery describes node-oriented query constraints.
//
// IDs and Types are optional pre-filters. Types use OR semantics.
// Filters are combined using FilterMode (default MatchAll when zero value).
type NodeQuery struct {
	IDs        []NodeID
	Types      []NodeType
	Filters    []PropertyFilter
	FilterMode MatchMode
	Order      QueryOrder
	Offset     int
	Limit      int
}

// EdgeQuery describes edge-oriented query constraints.
//
// IDs, Types, SrcIDs, and DstIDs are optional pre-filters. Types use OR semantics.
// Filters are combined using FilterMode (default MatchAll when zero value).
type EdgeQuery struct {
	IDs        []EdgeID
	Types      []EdgeType
	SrcIDs     []NodeID
	DstIDs     []NodeID
	Filters    []PropertyFilter
	FilterMode MatchMode
	Order      QueryOrder
	Offset     int
	Limit      int
}

// RelationQuery describes relation retrieval around anchor nodes.
//
// Anchors identifies the node(s) to traverse from.
// Direction controls which side of each relation is considered anchored.
// CounterpartIDs (optional) constrain the opposite endpoint.
// EdgeTypes and Filters constrain relation edges.
type RelationQuery struct {
	Anchors      []NodeID
	Direction    Direction
	Counterparts []NodeID
	EdgeTypes    []EdgeType
	Filters      []PropertyFilter
	FilterMode   MatchMode
	Order        QueryOrder
	Offset       int
	Limit        int
}

// NormalizedFilterMode returns mode when set, otherwise MatchAll.
func NormalizedFilterMode(mode MatchMode) MatchMode {
	if mode == MatchAny {
		return MatchAny
	}
	return MatchAll
}

// NormalizedQueryOrder returns order when set, otherwise QueryOrderAsc.
func NormalizedQueryOrder(order QueryOrder) QueryOrder {
	if order == QueryOrderDesc {
		return QueryOrderDesc
	}
	return QueryOrderAsc
}

// PropertyFilterMatches reports whether actual satisfies the given filter.
//
// For numeric comparisons (>, >=, <, <=, between), this function first attempts
// numeric comparison using ParseFloat on both values. If parsing fails for either
// side, it falls back to byte-wise lexicographic comparison.
func PropertyFilterMatches(filter PropertyFilter, actual []byte) bool {
	switch filter.Op {
	case PropertyOpPrefix:
		return strings.HasPrefix(string(actual), string(filter.Value))
	case PropertyOpContains:
		return strings.Contains(string(actual), string(filter.Value))
	case PropertyOpGreaterThan:
		return comparePropertyValues(actual, filter.Value) > 0
	case PropertyOpGreaterThanOrEqual:
		return comparePropertyValues(actual, filter.Value) >= 0
	case PropertyOpLessThan:
		return comparePropertyValues(actual, filter.Value) < 0
	case PropertyOpLessThanOrEqual:
		return comparePropertyValues(actual, filter.Value) <= 0
	case PropertyOpBetweenInclusive:
		if len(filter.ValueUpper) == 0 {
			return false
		}
		return comparePropertyValues(actual, filter.Value) >= 0 && comparePropertyValues(actual, filter.ValueUpper) <= 0
	case PropertyOpEqual:
		fallthrough
	default:
		return bytes.Equal(actual, filter.Value)
	}
}

func comparePropertyValues(actual []byte, expected []byte) int {
	aNum, aErr := strconv.ParseFloat(string(actual), 64)
	bNum, bErr := strconv.ParseFloat(string(expected), 64)
	if aErr == nil && bErr == nil {
		switch {
		case aNum < bNum:
			return -1
		case aNum > bNum:
			return 1
		default:
			return 0
		}
	}
	return bytes.Compare(actual, expected)
}

// ApplyNodeQueryWindow applies offset/limit pagination over sorted node IDs.
// Offset <= 0 means start from 0. Limit <= 0 means no upper bound.
func ApplyNodeQueryWindow(ids []NodeID, offset, limit int) []NodeID {
	if len(ids) == 0 {
		return nil
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(ids) {
		return nil
	}
	end := len(ids)
	if limit > 0 {
		end = offset + limit
		if end > len(ids) {
			end = len(ids)
		}
	}
	out := make([]NodeID, end-offset)
	copy(out, ids[offset:end])
	return out
}

// ApplyEdgeQueryWindow applies offset/limit pagination over sorted edge IDs.
// Offset <= 0 means start from 0. Limit <= 0 means no upper bound.
func ApplyEdgeQueryWindow(ids []EdgeID, offset, limit int) []EdgeID {
	if len(ids) == 0 {
		return nil
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(ids) {
		return nil
	}
	end := len(ids)
	if limit > 0 {
		end = offset + limit
		if end > len(ids) {
			end = len(ids)
		}
	}
	out := make([]EdgeID, end-offset)
	copy(out, ids[offset:end])
	return out
}
