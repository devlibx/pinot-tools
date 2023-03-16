package main

import (
	schema "github.com/devlibx/pinot-tools/cmd/pinot/backup"
	"os"
)

func main() {

	schemaList, err := schema.FetchSchemas()
	if err != nil {
		panic(err)
	}

	for _, s := range schemaList {
		if curl, err := schema.FetchSchemaCurl(s); err == nil {
			if err := os.WriteFile(schema.OutDir+"/schema_file_"+s+".sh", []byte(curl), 0644); err != nil {
				panic("failed to write schema file: schema=" + s)
			}
		}
	}

	tableList, err := schema.FetchTables()
	if err != nil {
		panic(err)
	}
	for _, table := range tableList {
		if curl, err := schema.FetchTableCurl(table); err == nil {
			if err := os.WriteFile(schema.OutDir+"/table_file_"+table+".sh", []byte(curl), 0644); err != nil {
				panic("failed to write schema file: table=" + table)
			}
		}
	}
}
