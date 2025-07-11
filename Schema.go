// Schema.go
// Description: Every thing Schema and Table related
// May change in near future that Tables get its own File
// Author: harto.dev

package hartoDb_go

import (
	"fmt"
	"os"
)

type Schema struct {
	name       string
	schemaPath string
	db         *HTDB
}

func (db *HTDB) Schema(name string) (*Schema, error) {
	var pathSchema = db.mainPath + "/" + name
	// check if folder at pathSchema exists
	if _, err := os.Stat(pathSchema); err == nil {
		return &Schema{
			name:       name,
			schemaPath: pathSchema,
			db:         db,
		}, nil
	}
	return nil, NewResponse(StatusSchenaDoesntExist, "Schema "+name+" does not exist")
}

func (db *HTDB) CreateSchema(name string) (*Schema, error) {
	pathSchema := db.mainPath + "/" + name

	if _, err := os.Stat(pathSchema); os.IsNotExist(err) {
		err := os.Mkdir(pathSchema, 0777)
		if err != nil {
			return nil, NewResponse(StatusDbError, fmt.Sprint(err))
		}

		_, err = os.Create(pathSchema + "/index.conf" + fileEnding)
		if err != nil {
			return nil, NewResponse(StatusDbError, fmt.Sprint(err))
		}

		return &Schema{
			name:       name,
			schemaPath: pathSchema,
			db:         db,
		}, nil

	} else {
		return nil, NewResponse(StatusSchenaAlreadyExists, "Schema "+name+" already exists")
	}
}
