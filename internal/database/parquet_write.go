package database

import (
	"database/sql"
	// "errors"
	"fmt"
	// "os"
	// "path/filepath"
	"reflect"
	"strings"
	"time"
)

func goTypeToDuckDBType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Int, reflect.Int64:
		return "BIGINT"
	case reflect.Float64:
		return "DOUBLE"
	case reflect.String:
		return "TEXT"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP"
		}
	}
	return "TEXT"
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
	query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`
	var count int
	err := db.QueryRow(query, tableName).Scan(&count)
	return count > 0, err
}

func createTableFromStruct[T any](db *sql.DB, tableName string, exampleStruct T) error {
	val := reflect.TypeOf(exampleStruct)
	var cols []string

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		colName := field.Name
		colType := goTypeToDuckDBType(field.Type)
		cols = append(cols, fmt.Sprintf("%s %s", colName, colType))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(cols, ", "))
	_, err := db.Exec(query)
	return err
}

func appendToTable[T any](db *sql.DB, tableName string, data []T) error {
	exists, err := tableExists(db, tableName)
	if err != nil {
		return err
	}
	if !exists {
		err := createTableFromStruct(db, tableName, data[0])
		if err != nil {
			return err
		}
	}

	// Generate CSV in memory
	// var csvBuilder strings.Builder
	// typ := records[0].Type()

	// for _, row := range records {
	// 	var values []string
	// 	for i := 0; i < typ.NumField(); i++ {
	// 		field := row.Field(i)
	// 		valStr := fmt.Sprintf("%v", field.Interface())
	// 		// Escape any quotes
	// 		valStr = strings.ReplaceAll(valStr, `"`, `""`)
	// 		values = append(values, fmt.Sprintf(`"%s"`, valStr))
	// 	}
	// 	csvBuilder.WriteString(strings.Join(values, ",") + "\n")
	// }

	// Create temp in-memory table
	// colNames := []string{}
	// for i := 0; i < typ.NumField(); i++ {
	// 	colNames = append(colNames, typ.Field(i).Name)
	// }
	tempTableName := "temp_insert_" + tableName
	_, err = db.Exec(fmt.Sprintf(`CREATE TEMP TABLE %s AS SELECT * FROM %s LIMIT 0`, tempTableName, tableName))
	if err != nil {
		return err
	}

	// Copy from CSV
	// copyCmd := fmt.Sprintf(`COPY %s(%s) FROM STDIN (FORMAT CSV, HEADER FALSE)`, tempTableName, strings.Join(colNames, ","))
	// _, err = db.Exec(copyCmd, csvBuilder.String())
	// if err != nil {
	// 	return err
	// }

	// Insert into main table
	insertCmd := fmt.Sprintf(`INSERT INTO %s SELECT * FROM %s`, tableName, tempTableName)
	_, err = db.Exec(insertCmd)
	return err
}
