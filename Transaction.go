// Transaction.go
// Description: Transaction system for the HTDB library
// Implements transaction management for the append-only database
// Author: harto.dev

package hartoDb_go

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Transaction represents a database transaction
type Transaction struct {
	ID            uint64               // Unique transaction ID
	StartTime     time.Time            // When the transaction started
	Status        TransactionStatus    // Current status of the transaction
	LockedRecords map[string]int64     // Map of tableName:recordID for locked records
	StagedRecords map[string][]*Record // Map of tableName:records for staged changes
	db            *HTDB                // Reference to the database
	mu            sync.Mutex           // Mutex for concurrent access
}

// TransactionStatus represents the status of a transaction
type TransactionStatus int

const (
	TransactionActive TransactionStatus = iota
	TransactionCommitted
	TransactionRolledBack
)

// Global transaction counter for generating unique IDs
var transactionCounter uint64 = 0

// NewTransaction creates a new transaction
func NewTransaction(db *HTDB) *Transaction {
	return &Transaction{
		ID:            atomic.AddUint64(&transactionCounter, 1),
		StartTime:     time.Now(),
		Status:        TransactionActive,
		LockedRecords: make(map[string]int64),
		StagedRecords: make(map[string][]*Record),
		db:            db,
	}
}

// LockRecord locks a record for this transaction
func (tx *Transaction) LockRecord(table *Table, record *Record) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	return tx.lockRecordInternal(table, record)
}

// lockRecordInternal locks a record without acquiring the transaction mutex
// This is used internally by methods that already hold the transaction mutex
func (tx *Transaction) lockRecordInternal(table *Table, record *Record) error {
	if tx.Status != TransactionActive {
		return fmt.Errorf("transaction is not active")
	}

	// Try to lock the record
	err := record.Lock(tx.ID)
	if err != nil {
		return err
	}

	// Add to locked records
	key := fmt.Sprintf("%s:%d", table.TableName, record.ID)
	tx.LockedRecords[key] = record.ID

	return nil
}

// StageUpdate stages an update to a record
func (tx *Transaction) StageUpdate(table *Table, record *Record, updates map[string]interface{}) (*Record, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TransactionActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	// Lock the record if not already locked
	key := fmt.Sprintf("%s:%d", table.TableName, record.ID)
	if _, exists := tx.LockedRecords[key]; !exists {
		err := tx.lockRecordInternal(table, record)
		if err != nil {
			return nil, err
		}
	}

	// Create a staging copy
	staging, err := record.Clone(tx.ID)
	if err != nil {
		return nil, err
	}

	// Apply updates
	for field, value := range updates {
		// Check if field exists in the table schema
		fieldExists := false
		var fieldDef Field
		for _, f := range table.Fields {
			if f.Name == field {
				fieldExists = true
				fieldDef = f
				break
			}
		}

		if !fieldExists {
			return nil, fmt.Errorf("field '%s' does not exist in table '%s'", field, table.TableName)
		}

		// Handle ref fields specially
		if fieldDef.Type == "ref" {
			if value == nil {
				staging.FieldsMeta[field] = FieldMetadata{IsNull: true}
				delete(staging.FieldsData, field)
				delete(staging.RefOffsets, field)
			} else {
				strValue, ok := value.(string)
				if !ok {
					return nil, fmt.Errorf("field '%s' requires a string value", field)
				}

				// Store the value in the ref file
				err := staging.WriteRefData(table.SchemaPath, table.TableName, field, strValue)
				if err != nil {
					return nil, err
				}

				staging.FieldsData[field] = strValue
				staging.FieldsMeta[field] = FieldMetadata{IsNull: false}
			}
		} else {
			// Regular field
			if value == nil {
				staging.FieldsMeta[field] = FieldMetadata{IsNull: true}
				delete(staging.FieldsData, field)
			} else {
				staging.FieldsData[field] = value
				staging.FieldsMeta[field] = FieldMetadata{IsNull: false}
			}
		}
	}

	// Add to staged records
	if _, exists := tx.StagedRecords[table.TableName]; !exists {
		tx.StagedRecords[table.TableName] = []*Record{}
	}
	tx.StagedRecords[table.TableName] = append(tx.StagedRecords[table.TableName], staging)

	return staging, nil
}

// StageDelete stages a delete operation for a record
func (tx *Transaction) StageDelete(table *Table, record *Record) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TransactionActive {
		return fmt.Errorf("transaction is not active")
	}

	// Lock the record if not already locked
	key := fmt.Sprintf("%s:%d", table.TableName, record.ID)
	if _, exists := tx.LockedRecords[key]; !exists {
		err := tx.lockRecordInternal(table, record)
		if err != nil {
			return err
		}
	}

	// Create a staging copy
	staging, err := record.Clone(tx.ID)
	if err != nil {
		return err
	}

	// Mark as deleted
	staging.Metadata.IsDeleted = true

	// Add to staged records
	if _, exists := tx.StagedRecords[table.TableName]; !exists {
		tx.StagedRecords[table.TableName] = []*Record{}
	}
	tx.StagedRecords[table.TableName] = append(tx.StagedRecords[table.TableName], staging)

	return nil
}

// Global counter for generating unique IDs
var recordIDCounter int64 = 0

// StageInsert stages a new record for insertion
func (tx *Transaction) StageInsert(table *Table, data map[string]interface{}) (*Record, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TransactionActive {
		return nil, fmt.Errorf("transaction is not active")
	}

	// Generate a new ID with a counter to ensure uniqueness
	id := time.Now().UnixNano() + atomic.AddInt64(&recordIDCounter, 1)

	// Create a new record
	record := NewRecord(id, data)
	record.Metadata.IsLocked = true
	record.Metadata.TransactionID = tx.ID

	// Handle ref fields
	for _, field := range table.Fields {
		if field.Type == "ref" {
			value, exists := data[field.Name]
			if !exists || value == nil {
				continue
			}

			strValue, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("field '%s' requires a string value", field.Name)
			}

			// Store the value in the ref file
			err := record.WriteRefData(table.SchemaPath, table.TableName, field.Name, strValue)
			if err != nil {
				return nil, err
			}
		}
	}

	// Add to staged records
	if _, exists := tx.StagedRecords[table.TableName]; !exists {
		tx.StagedRecords[table.TableName] = []*Record{}
	}
	tx.StagedRecords[table.TableName] = append(tx.StagedRecords[table.TableName], record)

	return record, nil
}

// Commit commits the transaction
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TransactionActive {
		return fmt.Errorf("transaction is not active")
	}

	// Process each table's staged records
	for tableName, records := range tx.StagedRecords {
		// Get the table
		table, err := GetTable(tableName, tx.db.GetMainPath())
		if err != nil {
			fmt.Println(err)
			return fmt.Errorf("failed to get table '%s': %v", tableName, err)
		}

		// Get existing records to update their is_current flag
		existingRecords, err := table.GetAllRecords()
		if err != nil {
			return fmt.Errorf("failed to get existing records for table '%s': %v", tableName, err)
		}

		// Update existing records' is_current flag
		for _, staged := range records {
			for _, existing := range existingRecords {
				// If this is an update to an existing record (not a new insert)
				if existing.FieldsData["id"] == staged.FieldsData["id"] && !staged.Metadata.IsDeleted {
					existing.Metadata.IsCurrent = false
				}
			}
		}

		// Mark staged records as current and not locked
		for _, record := range records {
			record.Metadata.IsCurrent = true
			record.Metadata.IsLocked = false
			record.Metadata.TransactionID = 0
		}

		// Append all records (existing and staged) to the table file
		err = table.WriteRecords(append(existingRecords, records...))
		if err != nil {
			return fmt.Errorf("failed to write records to table '%s': %v", tableName, err)
		}
	}

	// Update transaction status
	tx.Status = TransactionCommitted

	return nil
}

// Rollback rolls back the transaction
func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TransactionActive {
		return fmt.Errorf("transaction is not active")
	}

	// No need to do anything with staged records, they will be ignored
	// Just unlock any locked records
	for tableName, _ := range tx.StagedRecords {
		// Get the table
		table, err := GetTable(tableName, tx.db.GetMainPath())
		if err != nil {
			return fmt.Errorf("failed to get table '%s': %v", tableName, err)
		}

		// Get existing records to unlock them
		existingRecords, err := table.GetAllRecords()
		if err != nil {
			return fmt.Errorf("failed to get existing records for table '%s': %v", tableName, err)
		}

		// Unlock records
		for _, existing := range existingRecords {
			if existing.Metadata.IsLocked && existing.Metadata.TransactionID == tx.ID {
				existing.Metadata.IsLocked = false
				existing.Metadata.TransactionID = 0
			}
		}

		// Write the updated records back to the table
		err = table.WriteRecords(existingRecords)
		if err != nil {
			return fmt.Errorf("failed to write records to table '%s': %v", tableName, err)
		}
	}

	// Update transaction status
	tx.Status = TransactionRolledBack

	return nil
}

// Note: The actual implementations of GetTable, WriteRecords, and GetAllRecords
// are in the Table.go file.
