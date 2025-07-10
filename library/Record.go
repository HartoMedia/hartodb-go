// Record.go
// Description: Record struct for the HTDB library
// Implements record metadata, append-only handling, and transaction support
// Author: harto.dev

package library

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

// RecordMetadata contains the metadata for a record
type RecordMetadata struct {
	IsCurrent     bool   `json:"is_current"`     // true if this record is the latest version
	IsDeleted     bool   `json:"is_deleted"`     // true if the record was explicitly deleted
	IsLocked      bool   `json:"is_locked"`      // true if the record is locked by a transaction
	TransactionID uint64 `json:"transaction_id"` // The transaction ID currently owning this record
}

// FieldMetadata contains the metadata for a field
type FieldMetadata struct {
	IsNull bool `json:"is_null"` // true if the field is null
}

// Record represents a record in a table
type Record struct {
	ID         int64                    `json:"id"`          // Primary key (timeID)
	Metadata   RecordMetadata           `json:"metadata"`    // Record metadata
	FieldsData map[string]interface{}   `json:"fields_data"` // Field values
	FieldsMeta map[string]FieldMetadata `json:"fields_meta"` // Field metadata
	RefOffsets map[string][2]int64      `json:"ref_offsets"` // Offsets for ref fields [start, end]
	mu         sync.Mutex               // Mutex for concurrent access
}

// NewRecord creates a new record with default metadata
func NewRecord(id int64, data map[string]interface{}) *Record {
	record := &Record{
		ID: id,
		Metadata: RecordMetadata{
			IsCurrent:     true,
			IsDeleted:     false,
			IsLocked:      false,
			TransactionID: 0,
		},
		FieldsData: make(map[string]interface{}),
		FieldsMeta: make(map[string]FieldMetadata),
		RefOffsets: make(map[string][2]int64),
	}

	// Add ID to FieldsData
	record.FieldsData["id"] = id
	record.FieldsMeta["id"] = FieldMetadata{IsNull: false}

	// Copy data to fields
	for k, v := range data {
		record.FieldsData[k] = v
		record.FieldsMeta[k] = FieldMetadata{
			IsNull: v == nil,
		}
	}

	return record
}

// Lock locks the record for a transaction
func (r *Record) Lock(transactionID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Metadata.IsLocked && r.Metadata.TransactionID != transactionID {
		return fmt.Errorf("record is locked by another transaction: %d", r.Metadata.TransactionID)
	}

	r.Metadata.IsLocked = true
	r.Metadata.TransactionID = transactionID
	return nil
}

// Unlock unlocks the record
func (r *Record) Unlock() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Metadata.IsLocked = false
	r.Metadata.TransactionID = 0
}

// MarkDeleted marks the record as deleted
func (r *Record) MarkDeleted(transactionID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Metadata.IsLocked && r.Metadata.TransactionID != transactionID {
		return fmt.Errorf("record is locked by another transaction: %d", r.Metadata.TransactionID)
	}

	r.Metadata.IsDeleted = true
	return nil
}

// Clone creates a staging copy of the record for updates
func (r *Record) Clone(transactionID uint64) (*Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Metadata.IsLocked && r.Metadata.TransactionID != transactionID {
		return nil, fmt.Errorf("record is locked by another transaction: %d", r.Metadata.TransactionID)
	}

	// Create a new record with a new ID but same data
	newID := time.Now().UnixNano() // New timestamp ID
	clone := &Record{
		ID: newID,
		Metadata: RecordMetadata{
			IsCurrent:     false, // Not current until committed
			IsDeleted:     r.Metadata.IsDeleted,
			IsLocked:      true,
			TransactionID: transactionID,
		},
		FieldsData: make(map[string]interface{}),
		FieldsMeta: make(map[string]FieldMetadata),
		RefOffsets: make(map[string][2]int64),
	}

	// Copy data
	for k, v := range r.FieldsData {
		clone.FieldsData[k] = v
	}

	// Update ID in FieldsData to match the new ID
	clone.FieldsData["id"] = newID

	// Copy metadata
	for k, v := range r.FieldsMeta {
		clone.FieldsMeta[k] = v
	}

	// Ensure ID metadata is set
	clone.FieldsMeta["id"] = FieldMetadata{IsNull: false}

	// Copy ref offsets
	for k, v := range r.RefOffsets {
		clone.RefOffsets[k] = v
	}

	return clone, nil
}

// Serialize serializes the record to binary format
func (r *Record) Serialize(fields []Field) ([]byte, error) {
	// Calculate the size of the record
	recordSize := 8 // ID (int64)
	recordSize += 4 // Metadata (4 bytes for booleans and transaction ID)

	// Add field sizes
	for _, field := range fields {
		if field.Name == "id" {
			continue // Already counted
		}
		recordSize += int(field.Length)
		recordSize += 1 // Field metadata (1 byte for isNull)
	}

	// Create the binary data
	data := make([]byte, recordSize)
	offset := 0

	// Write ID
	binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(r.ID))
	offset += 8

	// Write metadata
	metaByte := byte(0)
	if r.Metadata.IsCurrent {
		metaByte |= 1
	}
	if r.Metadata.IsDeleted {
		metaByte |= 2
	}
	if r.Metadata.IsLocked {
		metaByte |= 4
	}
	data[offset] = metaByte
	offset++

	// Write transaction ID (3 bytes)
	binary.LittleEndian.PutUint16(data[offset:offset+2], uint16(r.Metadata.TransactionID))
	offset += 2
	data[offset] = byte(r.Metadata.TransactionID >> 16)
	offset++

	// Write fields
	for _, field := range fields {
		if field.Name == "id" {
			continue // Already written
		}

		// Write field metadata
		fieldMeta, exists := r.FieldsMeta[field.Name]
		if !exists {
			fieldMeta = FieldMetadata{IsNull: true}
		}
		if fieldMeta.IsNull {
			data[offset] = 1
		} else {
			data[offset] = 0
		}
		offset++

		// Write field data
		value, exists := r.FieldsData[field.Name]
		if !exists || fieldMeta.IsNull {
			// Write zeros for null fields
			offset += int(field.Length)
			continue
		}

		switch field.Type {
		case TimeID:
			v, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("field '%s' requires an int64 value", field.Name)
			}
			binary.LittleEndian.PutUint64(data[offset:offset+int(field.Length)], uint64(v))
		case Int:
			// Handle both int and int64 types
			var intValue int64
			if v, ok := value.(int); ok {
				intValue = int64(v)
			} else if v, ok := value.(int64); ok {
				intValue = v
			} else {
				return nil, fmt.Errorf("field '%s' requires an int or int64 value", field.Name)
			}
			binary.LittleEndian.PutUint64(data[offset:offset+int(field.Length)], uint64(intValue))
		case Float:
			v, ok := value.(float64)
			if !ok {
				return nil, fmt.Errorf("field '%s' requires a float64 value", field.Name)
			}
			binary.LittleEndian.PutUint64(data[offset:offset+int(field.Length)], uint64(v))
		case String:
			v, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("field '%s' requires a string value", field.Name)
			}
			copy(data[offset:offset+int(field.Length)], v)
		case "ref":
			// For ref fields, we store the offsets
			offsets, ok := r.RefOffsets[field.Name]
			if !ok {
				return nil, fmt.Errorf("missing ref offsets for field '%s'", field.Name)
			}
			binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(offsets[0]))
			binary.LittleEndian.PutUint64(data[offset+8:offset+16], uint64(offsets[1]))
		default:
			return nil, fmt.Errorf("unsupported field type '%s'", field.Type)
		}

		offset += int(field.Length)
	}

	return data, nil
}

// Deserialize deserializes binary data into a record
func DeserializeRecord(data []byte, fields []Field) (*Record, error) {
	if len(data) < 12 { // Minimum size: 8 (ID) + 4 (metadata)
		return nil, fmt.Errorf("data too short to be a valid record")
	}

	record := &Record{
		FieldsData: make(map[string]interface{}),
		FieldsMeta: make(map[string]FieldMetadata),
		RefOffsets: make(map[string][2]int64),
	}

	offset := 0

	// Read ID
	record.ID = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// Read metadata
	metaByte := data[offset]
	record.Metadata.IsCurrent = (metaByte & 1) != 0
	record.Metadata.IsDeleted = (metaByte & 2) != 0
	record.Metadata.IsLocked = (metaByte & 4) != 0
	offset++

	// Read transaction ID (3 bytes)
	txID := uint64(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2
	txID |= uint64(data[offset]) << 16
	record.Metadata.TransactionID = txID
	offset++

	// Read fields
	for _, field := range fields {
		if field.Name == "id" {
			record.FieldsData["id"] = record.ID
			record.FieldsMeta["id"] = FieldMetadata{IsNull: false}
			continue
		}

		// Read field metadata
		isNull := data[offset] == 1
		record.FieldsMeta[field.Name] = FieldMetadata{IsNull: isNull}
		offset++

		if isNull {
			// Skip null fields
			offset += int(field.Length)
			continue
		}

		// Read field data
		switch field.Type {
		case TimeID, Int:
			value := int64(binary.LittleEndian.Uint64(data[offset : offset+int(field.Length)]))
			record.FieldsData[field.Name] = value
		case Float:
			bits := binary.LittleEndian.Uint64(data[offset : offset+int(field.Length)])
			record.FieldsData[field.Name] = float64(bits)
		case String:
			str := string(data[offset : offset+int(field.Length)])
			// Trim null bytes
			record.FieldsData[field.Name] = string([]byte(str))
		case "ref":
			start := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			end := int64(binary.LittleEndian.Uint64(data[offset+8 : offset+16]))
			record.RefOffsets[field.Name] = [2]int64{start, end}
		}

		offset += int(field.Length)
	}

	return record, nil
}

// WriteRefData writes data for a ref field to the appropriate file
func (r *Record) WriteRefData(schema, tableName, fieldName string, value string) error {
	refFilePath := fmt.Sprintf("%s/%s.%s.data%s", schema, tableName, fieldName, fileEnding)

	refFile, err := os.OpenFile(refFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open ref field file: %v", err)
	}
	defer refFile.Close()

	// Get current file size as start offset
	stat, err := refFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %v", err)
	}

	start := stat.Size()

	// Write the data
	_, err = refFile.Write([]byte(value))
	if err != nil {
		return fmt.Errorf("failed to write to ref field file: %v", err)
	}

	// Store the offsets
	r.RefOffsets[fieldName] = [2]int64{start, start + int64(len(value))}

	return nil
}

// ReadRefData reads data for a ref field from the appropriate file
func (r *Record) ReadRefData(schema, tableName, fieldName string) (string, error) {
	offsets, exists := r.RefOffsets[fieldName]
	if !exists {
		return "", fmt.Errorf("no ref offsets found for field '%s'", fieldName)
	}

	refFilePath := fmt.Sprintf("%s/%s.%s.data%s", schema, tableName, fieldName, fileEnding)

	// Read the file
	data, err := os.ReadFile(refFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read ref field file: %v", err)
	}

	// Check bounds
	if offsets[0] < 0 || offsets[1] > int64(len(data)) || offsets[0] > offsets[1] {
		return "", fmt.Errorf("invalid ref offsets for field '%s'", fieldName)
	}

	// Extract the data
	return string(data[offsets[0]:offsets[1]]), nil
}
