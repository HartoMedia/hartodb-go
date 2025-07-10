// Cleanup.go
// Description: Background cleanup worker for the HTDB library
// Implements periodic cleanup of outdated and deleted records
// Author: harto.dev

package library

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CleanupWorker represents a background worker that periodically cleans up the database
type CleanupWorker struct {
	db        *HTDB
	interval  time.Duration
	stopChan  chan struct{}
	wg        sync.WaitGroup
	isRunning bool
	mu        sync.Mutex
}

// NewCleanupWorker creates a new cleanup worker
func NewCleanupWorker(db *HTDB, interval time.Duration) *CleanupWorker {
	return &CleanupWorker{
		db:        db,
		interval:  interval,
		stopChan:  make(chan struct{}),
		isRunning: false,
	}
}

// Start starts the cleanup worker
func (w *CleanupWorker) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isRunning {
		return fmt.Errorf("cleanup worker is already running")
	}

	w.isRunning = true
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				w.performCleanup()
			case <-w.stopChan:
				return
			}
		}
	}()

	return nil
}

// Stop stops the cleanup worker
func (w *CleanupWorker) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isRunning {
		return fmt.Errorf("cleanup worker is not running")
	}

	close(w.stopChan)
	w.wg.Wait()
	w.isRunning = false

	return nil
}

// performCleanup performs the actual cleanup operation
func (w *CleanupWorker) performCleanup() {
	// Get all schemas
	schemas, err := w.getSchemas()
	if err != nil {
		fmt.Printf("Error getting schemas: %v\n", err)
		return
	}

	// Process each schema
	for _, schema := range schemas {
		// Get all tables in the schema
		tables, err := w.getTables(schema)
		if err != nil {
			fmt.Printf("Error getting tables for schema %s: %v\n", schema, err)
			continue
		}

		// Process each table
		for _, table := range tables {
			err := w.cleanupTable(schema, table)
			if err != nil {
				fmt.Printf("Error cleaning up table %s in schema %s: %v\n", table, schema, err)
			}
		}
	}
}

// getSchemas returns all schemas in the database
func (w *CleanupWorker) getSchemas() ([]string, error) {
	// Get all directories in the main path
	entries, err := os.ReadDir(w.db.mainPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read main directory: %v", err)
	}

	var schemas []string
	for _, entry := range entries {
		if entry.IsDir() {
			schemas = append(schemas, entry.Name())
		}
	}

	return schemas, nil
}

// getTables returns all tables in a schema
func (w *CleanupWorker) getTables(schema string) ([]string, error) {
	schemaPath := filepath.Join(w.db.mainPath, schema)

	// Get all files in the schema directory
	entries, err := os.ReadDir(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema directory: %v", err)
	}

	var tables []string
	for _, entry := range entries {
		if !entry.IsDir() {
			name := entry.Name()
			// Check if it's a table file (not a config or data file)
			if filepath.Ext(name) == fileEnding &&
				!strings.HasSuffix(name, ".conf"+fileEnding) &&
				!strings.HasSuffix(name, ".data"+fileEnding) {
				// Remove the extension
				tableName := name[:len(name)-len(fileEnding)]
				tables = append(tables, tableName)
			}
		}
	}

	return tables, nil
}

// cleanupTable cleans up a table by removing outdated and deleted records
func (w *CleanupWorker) cleanupTable(schema, tableName string) error {
	// Get the table
	tableConfPath := filepath.Join(w.db.mainPath, schema, tableName+".conf"+fileEnding)
	tableDataPath := filepath.Join(w.db.mainPath, schema, tableName+fileEnding)

	// Read the table configuration
	tableConf, err := os.ReadFile(tableConfPath)
	if err != nil {
		return fmt.Errorf("failed to read table configuration: %v", err)
	}

	var table Table
	err = json.Unmarshal(tableConf, &table)
	if err != nil {
		return fmt.Errorf("failed to parse table configuration: %v", err)
	}

	// Set the schema path
	table.SchemaPath = filepath.Join(w.db.mainPath, schema)

	// Read all records from the table
	records, err := table.GetAllRecords()
	if err != nil {
		return fmt.Errorf("failed to read records: %v", err)
	}

	// Filter out outdated and deleted records
	var currentRecords []*Record
	for _, record := range records {
		if record.Metadata.IsCurrent && !record.Metadata.IsDeleted {
			currentRecords = append(currentRecords, record)
		}
	}

	// If no records were filtered out, no cleanup needed
	if len(currentRecords) == len(records) {
		return nil
	}

	// Create a temporary file for the new table data
	tempDataPath := tableDataPath + ".temp"
	tempFile, err := os.Create(tempDataPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tempFile.Close()

	// Write current records to the temporary file
	for _, record := range currentRecords {
		data, err := record.Serialize(table.Fields)
		if err != nil {
			return fmt.Errorf("failed to serialize record: %v", err)
		}
		_, err = tempFile.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write record to temporary file: %v", err)
		}
	}

	// Close the temporary file
	tempFile.Close()

	// Replace the old file with the new one
	err = os.Rename(tempDataPath, tableDataPath)
	if err != nil {
		return fmt.Errorf("failed to replace table file: %v", err)
	}

	// Clean up ref field files
	for _, field := range table.Fields {
		if field.Type == "ref" {
			err := w.cleanupRefField(schema, tableName, field.Name, currentRecords)
			if err != nil {
				fmt.Printf("Error cleaning up ref field %s: %v\n", field.Name, err)
			}
		}
	}

	return nil
}

// cleanupRefField cleans up a ref field file by removing unused data
func (w *CleanupWorker) cleanupRefField(schema, tableName, fieldName string, records []*Record) error {
	refFilePath := filepath.Join(w.db.mainPath, schema, tableName+"."+fieldName+".data"+fileEnding)

	// Check if the ref file exists
	if _, err := os.Stat(refFilePath); os.IsNotExist(err) {
		return nil // Nothing to clean up
	}

	// Read the current ref file
	refData, err := os.ReadFile(refFilePath)
	if err != nil {
		return fmt.Errorf("failed to read ref field file: %v", err)
	}

	// Create a map of used offsets
	usedRanges := make(map[[2]int64]bool)
	for _, record := range records {
		if offsets, exists := record.RefOffsets[fieldName]; exists {
			usedRanges[offsets] = true
		}
	}

	// If all offsets are used, no cleanup needed
	if len(usedRanges) == 0 {
		return nil
	}

	// Create a temporary file for the new ref data
	tempRefPath := refFilePath + ".temp"
	tempFile, err := os.Create(tempRefPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary ref file: %v", err)
	}
	defer tempFile.Close()

	// Create a map to track new offsets
	offsetMap := make(map[[2]int64][2]int64)
	currentOffset := int64(0)

	// Write used data to the temporary file and update offsets
	for _, record := range records {
		if offsets, exists := record.RefOffsets[fieldName]; exists {
			// Check if we've already processed this range
			if newOffsets, processed := offsetMap[offsets]; processed {
				record.RefOffsets[fieldName] = newOffsets
				continue
			}

			// Extract the data
			start, end := offsets[0], offsets[1]
			if start < 0 || end > int64(len(refData)) || start > end {
				continue // Skip invalid offsets
			}

			data := refData[start:end]

			// Write to the temporary file
			newStart := currentOffset
			_, err := tempFile.Write(data)
			if err != nil {
				return fmt.Errorf("failed to write ref data to temporary file: %v", err)
			}

			newEnd := newStart + int64(len(data))

			// Update the record's offsets
			newOffsets := [2]int64{newStart, newEnd}
			record.RefOffsets[fieldName] = newOffsets

			// Store the mapping for other records that might use the same range
			offsetMap[offsets] = newOffsets

			currentOffset = newEnd
		}
	}

	// Close the temporary file
	tempFile.Close()

	// Replace the old file with the new one
	err = os.Rename(tempRefPath, refFilePath)
	if err != nil {
		return fmt.Errorf("failed to replace ref field file: %v", err)
	}

	return nil
}
