// Query.go
// Description: Query builder for the HTDB library
// Implements a fluent interface for querying records
// Author: harto.dev

package library

import (
	"fmt"
	"sort"
)

// FilterCondition represents a single filter condition for a query
type FilterCondition struct {
	Field    string
	Operator string
	Value    interface{}
}

// Query represents a database query with builder pattern
type Query struct {
	table         *Table
	db            *HTDB
	limitCount    int
	sortField     string
	sortAscending bool
	conditions    []FilterCondition
}

// Select creates a new query for the specified table
func (tm *TableManager) Select(table *Table) *Query {
	return &Query{
		table:         table,
		db:            tm.db,
		limitCount:    -1, // No limit by default
		sortField:     "", // No sorting by default
		sortAscending: true,
		conditions:    []FilterCondition{}, // No conditions by default
	}
}

// Sort specifies the field to sort by and the sort direction
// If ascending is true, sort in ascending order, otherwise sort in descending order
func (q *Query) Sort(field string, ascending bool) *Query {
	q.sortField = field
	q.sortAscending = ascending
	return q
}

// Limit restricts the number of results returned from the query
func (q *Query) Limit(count int) *Query {
	q.limitCount = count
	return q
}

// Where adds a filter condition to the query
// Supported operators: "=", "!=", ">", ">=", "<", "<="
func (q *Query) Where(field string, operator string, value interface{}) *Query {
	q.conditions = append(q.conditions, FilterCondition{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
	return q
}

// GetAll executes the query and returns all matching records
// applying any filtering, sorting, and limits that were set
func (q *Query) GetAll() ([]*Record, error) {
	// Get all records from the table
	records, err := q.table.GetAllRecords()
	if err != nil {
		return nil, err
	}

	// Filter to current records only
	var currentRecords []*Record
	for _, record := range records {
		if record.Metadata.IsCurrent && !record.Metadata.IsDeleted {
			currentRecords = append(currentRecords, record)
		}
	}

	// Apply where conditions if any
	if len(q.conditions) > 0 {
		var filteredRecords []*Record
		for _, record := range currentRecords {
			if matchesConditions(record, q.conditions) {
				filteredRecords = append(filteredRecords, record)
			}
		}
		currentRecords = filteredRecords
	}

	// Apply sorting if a sort field is specified
	if q.sortField != "" {
		// Sort the records based on the specified field and direction
		sortRecords(currentRecords, q.sortField, q.sortAscending)
	}

	// Apply limit if set
	if q.limitCount > 0 && len(currentRecords) > q.limitCount {
		return currentRecords[:q.limitCount], nil
	}

	return currentRecords, nil
}

// matchesConditions checks if a record matches all the filter conditions
func matchesConditions(record *Record, conditions []FilterCondition) bool {
	for _, condition := range conditions {
		fieldValue, exists := record.FieldsData[condition.Field]
		if !exists {
			return false // Field doesn't exist in the record
		}

		// Compare based on the operator and types
		switch condition.Operator {
		case "=":
			if !equals(fieldValue, condition.Value) {
				return false
			}
		case "!=":
			if equals(fieldValue, condition.Value) {
				return false
			}
		case ">":
			if !greaterThan(fieldValue, condition.Value) {
				return false
			}
		case ">=":
			if !greaterThanOrEqual(fieldValue, condition.Value) {
				return false
			}
		case "<":
			if !lessThan(fieldValue, condition.Value) {
				return false
			}
		case "<=":
			if !lessThanOrEqual(fieldValue, condition.Value) {
				return false
			}
		default:
			return false // Unsupported operator
		}
	}
	return true // All conditions matched
}

// equals checks if two values are equal
func equals(a, b interface{}) bool {
	switch aVal := a.(type) {
	case string:
		if bVal, ok := b.(string); ok {
			return aVal == bVal
		}
	case int:
		switch bVal := b.(type) {
		case int:
			return aVal == bVal
		case float64:
			return float64(aVal) == bVal
		}
	case float64:
		switch bVal := b.(type) {
		case float64:
			return aVal == bVal
		case int:
			return aVal == float64(bVal)
		}
	case bool:
		if bVal, ok := b.(bool); ok {
			return aVal == bVal
		}
	}
	return false
}

// greaterThan checks if a > b
func greaterThan(a, b interface{}) bool {
	switch aVal := a.(type) {
	case string:
		if bVal, ok := b.(string); ok {
			return aVal > bVal
		}
	case int:
		switch bVal := b.(type) {
		case int:
			return aVal > bVal
		case float64:
			return float64(aVal) > bVal
		}
	case float64:
		switch bVal := b.(type) {
		case float64:
			return aVal > bVal
		case int:
			return aVal > float64(bVal)
		}
	}
	return false
}

// greaterThanOrEqual checks if a >= b
func greaterThanOrEqual(a, b interface{}) bool {
	return greaterThan(a, b) || equals(a, b)
}

// lessThan checks if a < b
func lessThan(a, b interface{}) bool {
	switch aVal := a.(type) {
	case string:
		if bVal, ok := b.(string); ok {
			return aVal < bVal
		}
	case int:
		switch bVal := b.(type) {
		case int:
			return aVal < bVal
		case float64:
			return float64(aVal) < bVal
		}
	case float64:
		switch bVal := b.(type) {
		case float64:
			return aVal < bVal
		case int:
			return aVal < float64(bVal)
		}
	}
	return false
}

// lessThanOrEqual checks if a <= b
func lessThanOrEqual(a, b interface{}) bool {
	return lessThan(a, b) || equals(a, b)
}

// sortRecords sorts the records by the specified field in the specified direction
func sortRecords(records []*Record, field string, ascending bool) {
	// Define a less function that compares records based on the field
	less := func(i, j int) bool {
		// Get the values to compare
		valI, okI := records[i].FieldsData[field]
		valJ, okJ := records[j].FieldsData[field]

		// If either value doesn't exist, put records with missing values at the end
		if !okI && !okJ {
			return false
		}
		if !okI {
			return false
		}
		if !okJ {
			return true
		}

		// Compare based on type
		var result bool
		switch valI.(type) {
		case string:
			// String comparison
			strI, _ := valI.(string)
			strJ, _ := valJ.(string)
			result = strI < strJ
		case int:
			// Integer comparison
			intI, _ := valI.(int)
			intJ, _ := valJ.(int)
			result = intI < intJ
		case float64:
			// Float comparison
			floatI, _ := valI.(float64)
			floatJ, _ := valJ.(float64)
			result = floatI < floatJ
		default:
			// Default to string comparison for other types
			result = fmt.Sprintf("%v", valI) < fmt.Sprintf("%v", valJ)
		}

		// Invert result if descending order
		if !ascending {
			return !result
		}
		return result
	}

	// Sort the records
	sort.Slice(records, less)
}
