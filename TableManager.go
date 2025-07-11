// TableManager.go
// Description: TableManager for the HTDB library
// Provides a clean API for managing tables, transactions, and records
// Author: harto.dev

package hartoDb_go

import (
	"fmt"
	"sync"
	"time"
)

// TableManager manages tables, transactions, and records in the database
type TableManager struct {
	db             *HTDB
	cleanupWorker  *CleanupWorker
	transactions   map[uint64]*Transaction
	transactionsMu sync.Mutex
}

// NewTableManager creates a new table manager
func NewTableManager(db *HTDB) *TableManager {
	return &TableManager{
		db:           db,
		transactions: make(map[uint64]*Transaction),
	}
}

// StartCleanupWorker starts the background cleanup worker
func (tm *TableManager) StartCleanupWorker(interval time.Duration) error {
	if tm.cleanupWorker != nil {
		return fmt.Errorf("cleanup worker is already running")
	}

	tm.cleanupWorker = NewCleanupWorker(tm.db, interval)
	return tm.cleanupWorker.Start()
}

// StopCleanupWorker stops the background cleanup worker
func (tm *TableManager) StopCleanupWorker() error {
	if tm.cleanupWorker == nil {
		return fmt.Errorf("cleanup worker is not running")
	}

	err := tm.cleanupWorker.Stop()
	if err != nil {
		return err
	}

	tm.cleanupWorker = nil
	return nil
}

// BeginTransaction begins a new transaction
func (tm *TableManager) BeginTransaction() *Transaction {
	tm.transactionsMu.Lock()
	defer tm.transactionsMu.Unlock()

	tx := NewTransaction(tm.db)
	tm.transactions[tx.ID] = tx
	return tx
}

// CommitTransaction commits a transaction
func (tm *TableManager) CommitTransaction(tx *Transaction) error {
	tm.transactionsMu.Lock()
	defer tm.transactionsMu.Unlock()

	if _, exists := tm.transactions[tx.ID]; !exists {
		return fmt.Errorf("transaction not found")
	}

	err := tx.Commit()
	if err != nil {
		return err
	}

	delete(tm.transactions, tx.ID)
	return nil
}

// RollbackTransaction rolls back a transaction
func (tm *TableManager) RollbackTransaction(tx *Transaction) error {
	tm.transactionsMu.Lock()
	defer tm.transactionsMu.Unlock()

	if _, exists := tm.transactions[tx.ID]; !exists {
		return fmt.Errorf("transaction not found")
	}

	err := tx.Rollback()
	if err != nil {
		return err
	}

	delete(tm.transactions, tx.ID)
	return nil
}

// CreateTable creates a new table
func (tm *TableManager) CreateTable(schemaName, tableName string, fields []Field) (*Table, error) {
	// Get the schema
	schema, err := tm.db.Schema(schemaName)
	if err != nil {
		return nil, err
	}

	// Create the table
	resp := schema.CreateTable(tableName, fields)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf(resp.Message)
	}

	// Get the table
	table, err := GetTable(schemaName+":"+tableName, tm.db.GetMainPath())
	if err != nil {
		return nil, err
	}

	return table, nil
}

// GetTable gets a table by name
func (tm *TableManager) GetTable(schemaName, tableName string) (*Table, error) {
	return GetTable(schemaName+":"+tableName, tm.db.GetMainPath())
}

// InsertRecord inserts a new record into a table
func (tm *TableManager) InsertRecord(table *Table, data map[string]interface{}) (*Record, error) {
	// Begin a transaction
	tx := tm.BeginTransaction()

	// Stage the insert
	record, err := tx.StageInsert(table, data)
	if err != nil {
		tm.RollbackTransaction(tx)
		return nil, err
	}

	// Commit the transaction
	err = tm.CommitTransaction(tx)
	if err != nil {
		return nil, err
	}

	return record, nil
}

// UpdateRecord updates an existing record in a table
func (tm *TableManager) UpdateRecord(table *Table, record *Record, updates map[string]interface{}) (*Record, error) {
	// Begin a transaction
	tx := tm.BeginTransaction()

	// Stage the update
	updatedRecord, err := tx.StageUpdate(table, record, updates)
	if err != nil {
		tm.RollbackTransaction(tx)
		return nil, err
	}

	// Commit the transaction
	err = tm.CommitTransaction(tx)
	if err != nil {
		return nil, err
	}

	return updatedRecord, nil
}

// DeleteRecord deletes a record from a table
func (tm *TableManager) DeleteRecord(table *Table, record *Record) error {
	// Begin a transaction
	tx := tm.BeginTransaction()

	// Stage the delete
	err := tx.StageDelete(table, record)
	if err != nil {
		tm.RollbackTransaction(tx)
		return err
	}

	// Commit the transaction
	err = tm.CommitTransaction(tx)
	if err != nil {
		return err
	}

	return nil
}

// GetAllRecords gets all records from a table
func (tm *TableManager) GetAllRecords(table *Table) ([]*Record, error) {
	return table.GetAllRecords()
}

// GetCurrentRecords gets all current (not deleted) records from a table
func (tm *TableManager) GetCurrentRecords(table *Table) ([]*Record, error) {
	records, err := table.GetAllRecords()
	if err != nil {
		return nil, err
	}

	var currentRecords []*Record
	for _, record := range records {
		if record.Metadata.IsCurrent && !record.Metadata.IsDeleted {
			currentRecords = append(currentRecords, record)
		}
	}

	return currentRecords, nil
}

// GetRecordByID gets a record by ID
func (tm *TableManager) GetRecordByID(table *Table, id int64) (*Record, error) {
	records, err := table.GetAllRecords()
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		if record.ID == id && record.Metadata.IsCurrent {
			return record, nil
		}
	}

	return nil, fmt.Errorf("record not found")
}
