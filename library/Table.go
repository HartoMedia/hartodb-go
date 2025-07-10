// Table.go
// Description: Table struct for the HTDB library
// Jej, Tables got its own file
// Author: harto.dev
package library

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Table struct {
	TableName  string  `json:"tableName"`
	Fields     []Field `json:"fields"`
	SchemaPath string  `json:"schemaPath"`
}

type Field struct {
	Name        string       `json:"name"`
	Type        FieldTypes   `json:"type"`
	Length      uint         `json:"length,omitempty"`
	Constraints []Constraint `json:"constraints"`
}

type FieldTypes string

const (
	String FieldTypes = "string"
	Int    FieldTypes = "int"
	Float  FieldTypes = "float"
	Bool   FieldTypes = "bool"
	TimeID FieldTypes = "timeID"
	// unsure -- Arrays or List will work similar to the reference type
)

type Constraint string

const (
	PrimaryKey Constraint = "primary_key"
	NotNull    Constraint = "not_null"
	Unique     Constraint = "unique"
)

func NewTable(name string, fields []Field) Table {
	return Table{
		TableName: name,
		Fields:    fields,
	}
}

// Function to create a database table
func (s *Schema) CreateTable(name string, fields []Field) Response {
	// Prepend the timePKField to fields
	fields = append([]Field{timePKField}, fields...)

	// Set the path for the schema and table
	var pathTable = s.schemaPath + "/" + name + fileEnding
	var pathConf = s.schemaPath + "/" + name + ".conf" + fileEnding

	// Check schema
	if _, err := os.Stat(s.schemaPath); os.IsNotExist(err) {
		// Return error if schema does not exist
		var errorMessage = "Schema " + s.name + " does not exist"
		return Response{time.Now().String(), 406, errorMessage}
	}

	// Check if table exists
	if _, err := os.Stat(pathTable); !os.IsNotExist(err) {
		// Return error if table file already exists
		var errorMessage = "Table " + name + " already exists"
		return Response{time.Now().String(), 406, errorMessage}
	}

	// Check table name
	if len(name) == 0 {
		return Response{time.Now().String(), 406, "You have to give the table a name"}
	}

	if strings.HasPrefix(name, ".") {
		return Response{time.Now().String(), 406, "Can't name a Table like that, sowwy"}
	}

	if name == "index" {
		return Response{time.Now().String(), 406, "Can't name a Table \"index\", sowwy"}
	}

	// Validate field lengths
	if err := validateFieldLengths(fields); err != nil {
		return Response{time.Now().String(), 406, err.Error()}
	}

	// Create the file for the table
	file, err := os.Create(pathTable)
	defer file.Close() // Close the file after function ends
	if err != nil {
		// Return error if file creation fails
		return Response{time.Now().String(), 500, "Failed to create table file: " + err.Error()}
	}

	// Create a separate data file for each ref field
	for _, field := range fields {
		if field.Type == "ref" {
			refFilePath := s.schemaPath + "/" + name + "." + field.Name + ".data" + fileEnding
			refFile, err := os.Create(refFilePath)
			if err != nil {
				return Response{time.Now().String(), 500, "Failed to create ref field file: " + err.Error()}
			}
			refFile.Close()
		}
	}

	confFile, err := os.Create(pathConf)
	if err != nil {
		return Response{time.Now().String(), 500, fmt.Sprint(err)}
	}
	defer confFile.Close()

	// Create the configuration file
	newTable := Table{
		TableName:  name,
		Fields:     fields,
		SchemaPath: s.schemaPath,
	}

	// Serialize the table to JSON
	tableJSON, err := json.MarshalIndent(newTable, "", "  ")
	if err != nil {
		return Response{time.Now().String(), 500, "Failed to serialize table to JSON: " + err.Error()}
	}

	// Write JSON to configuration file
	err = os.WriteFile(pathConf, tableJSON, 0644)
	if err != nil {
		return Response{time.Now().String(), 500, "Failed to write JSON to configuration file: " + err.Error()}
	}

	// Log success message
	return Response{time.Now().String(), 200, "Table created successfully"}
}

func validateFieldLengths(fields []Field) error {
	for _, f := range fields {
		if f.Type == "ref" && f.Length != 128 {
			return fmt.Errorf("field '%s' of type 'ref' must have a length of %d bytes", f.Name, 128)
		}
		if f.Type == "timeID" && f.Length != 8 {
			return fmt.Errorf("field '%s' of type 'timeID' must have a length of 8 bytes", f.Name)
		}
	}
	return nil
}

// GetTable returns a table by name from a schema
func GetTable(tableName string, mainPath string) (*Table, error) {
	// Split the tableName into schema and table parts if it contains a colon
	parts := strings.Split(tableName, ":")
	var schemaName, tableNameOnly string

	if len(parts) > 1 {
		schemaName = parts[0]
		tableNameOnly = parts[1]
	} else {
		// Default schema
		schemaName = "testSchema" // or any default schema you want to use
		tableNameOnly = tableName
	}

	// Construct paths
	schemaPath := mainPath + "/" + schemaName
	tableConfPath := schemaPath + "/" + tableNameOnly + ".conf" + fileEnding

	// Check if the schema exists
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("schema '%s' does not exist", schemaName)
	}

	// Check if the table configuration exists
	if _, err := os.Stat(tableConfPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("table '%s' does not exist in schema '%s'", tableNameOnly, schemaName)
	}

	// Read the table configuration
	tableConf, err := os.ReadFile(tableConfPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read table configuration: %v", err)
	}

	var table Table
	err = json.Unmarshal(tableConf, &table)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table configuration: %v", err)
	}

	// Set the schema path
	table.SchemaPath = schemaPath

	return &table, nil
}

// WriteRecords writes records to the table file
func (t *Table) WriteRecords(records []*Record) error {
	// Construct the table file path
	tablePath := t.SchemaPath + "/" + t.TableName + fileEnding

	// Create a temporary file
	tempPath := tablePath + ".temp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tempFile.Close()

	// Write each record to the temporary file
	for _, record := range records {
		data, err := record.Serialize(t.Fields)
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
	err = os.Rename(tempPath, tablePath)
	if err != nil {
		return fmt.Errorf("failed to replace table file: %v", err)
	}

	return nil
}

// GetAllRecords reads all records from the table file
func (t *Table) GetAllRecords() ([]*Record, error) {
	// Construct the table file path
	tablePath := t.SchemaPath + "/" + t.TableName + fileEnding

	// Check if the table file exists
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		return []*Record{}, nil // Return empty slice if file doesn't exist
	}

	// Read the table file
	data, err := os.ReadFile(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read table file: %v", err)
	}

	// Calculate record size
	recordSize := 0
	for _, field := range t.Fields {
		if field.Name == "id" {
			continue // ID is handled separately
		}
		recordSize += int(field.Length)
		recordSize += 1 // Field metadata (1 byte for isNull)
	}

	// Add metadata size
	recordSize += 12 // 8 bytes for ID, 4 bytes for metadata

	// Parse records
	var records []*Record
	for i := 0; i < len(data); i += recordSize {
		if i+recordSize > len(data) {
			break // Partial record, skip
		}

		recordData := data[i : i+recordSize]
		record, err := DeserializeRecord(recordData, t.Fields)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize record: %v", err)
		}

		records = append(records, record)
	}

	return records, nil
}
