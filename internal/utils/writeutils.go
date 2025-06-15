package utils

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/marcboeker/go-duckdb"
)

func WriteStructsToCSV[T any](filename string, data []T) error {
	// Get the reflect Value of the slice
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice || v.Len() == 0 {
		return fmt.Errorf("data must be a non-empty slice of structs")
	}

	// Get the element type (should be struct)
	elem := v.Index(0)
	elemType := elem.Type()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("slice elements must be structs")
	}

	// Open file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	var headers []string
	for i := range elem.NumField() {
		headers = append(headers, elemType.Field(i).Name)
	}
	writer.Write(headers)

	// Write data rows
	for i := range v.Len() {
		var row []string
		rowElem := v.Index(i)
		for j := range rowElem.NumField() {
			field := rowElem.Field(j)
			var value string
			switch field.Kind() {
			case reflect.String:
				value = field.String()
			case reflect.Int, reflect.Int64:
				value = strconv.FormatInt(field.Int(), 10)
			case reflect.Float64, reflect.Float32:
				value = strconv.FormatFloat(field.Float(), 'f', 2, 64)
			case reflect.Bool:
				value = strconv.FormatBool(field.Bool())
			default:
				value = fmt.Sprintf("%v", field.Interface())
			}
			row = append(row, value)
		}
		writer.Write(row)
	}

	return nil
}

func WriteStructsToDuckDB[T any](con *duckdb.Conn, schema, table string, data []T) error {
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice || v.Len() == 0 {
		return fmt.Errorf("data must be a non-empty slice of structs")
	}

	elem := v.Index(0)
	elemType := elem.Type()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("slice elements must be structs")
	}

	// Build CREATE TABLE statement
	var columns []string
	for i := range elemType.NumField() {
		field := elemType.Field(i)
		duckType := mapGoTypeToDuckDBType(field.Type)
		if duckType == "" {
			return fmt.Errorf("unsupported type: %s", field.Type.Kind())
		}
		columns = append(columns, fmt.Sprintf("%s %s", field.Name, duckType))
	}

	fqTable := fmt.Sprintf(`"%s"."%s"`, schema, table)
	createSQL := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"; CREATE TABLE IF NOT EXISTS %s (%s);`,
		schema, fqTable, strings.Join(columns, ", "))

	if _, err := con.ExecContext(context.Background(), createSQL, nil); err != nil {
		return fmt.Errorf("create table error: %w", err)
	}

	defer con.Close()

	// Use DuckDB Appender

	appender, err := duckdb.NewAppenderFromConn(con, schema, table)
	if err != nil {
		return err
	}
	defer appender.Close()

	for i := range v.Len() {
		row := v.Index(i)
		var values []driver.Value
		for j := range row.NumField() {
			val := row.Field(j).Interface()
			values = append(values, val)
		}
		if err := appender.AppendRow(values...); err != nil {
			return fmt.Errorf("append error on row %d: %w", i, err)
		}
	}

	return nil
}

func mapGoTypeToDuckDBType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "TEXT"
	case reflect.Int, reflect.Int64:
		return "BIGINT"
	case reflect.Int32:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "DOUBLE"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP"
		}
	}
	return "TIMESTAMP"
}
