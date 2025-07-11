// Package htdb / main.go
// Description: Main file for the HTDB library
// This file contains the main struct for the HTDB library
// and the main functions to interact with the library
// Author: harto.dev

// ChatGPT url: https://chatgpt.com/c/67ec35f1-fddc-8000-88ae-864091d5ede7
// didnt do the last step about the responses
package hartoDb_go

type HTDB struct {
	mainPath      string
	lastTimestamp int64
	tableManager  *TableManager
}

// --- Field Presets ---
var timePKField = Field{
	Name:        "id",
	Type:        "timeID",
	Length:      8, // 64 bits - 8 bytes (uint64) stored for Nanoseconds since Unix epoch +- 584 years
	Constraints: []Constraint{PrimaryKey, NotNull, Unique},
}

//var stringField = Field{
//	Type:        "string",
//	Length:      255,
//	Constraints: []Constraint{NotNull},
//}
//
//var intField = Field{
//	Type:        "int",
//	Constraints: []Constraint{NotNull},
//	Length:      8, // 64 bits - 8 bytes (int64)
//}

const fileEnding string = ".htdb"

// Constructor
func NewHTDB(mainPath string) *HTDB {
	db := &HTDB{
		mainPath: mainPath,
	}
	db.tableManager = NewTableManager(db)
	return db
}

func (db *HTDB) GetMainPath() string {
	return db.mainPath
}

func (db *HTDB) SetMainPath(path string) {
	db.mainPath = path
}

func (db *HTDB) GetLastTimestamp() int64 {
	return db.lastTimestamp
}

func (db *HTDB) SetLastTimestamp(timestamp int64) {
	db.lastTimestamp = timestamp
}

func (db *HTDB) GetTableManager() *TableManager {
	return db.tableManager
}

func (db *HTDB) SetTableManager(tm *TableManager) {
	db.tableManager = tm
}
