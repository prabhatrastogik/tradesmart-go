package config

import (
	"context"

	duckdb "github.com/marcboeker/go-duckdb"
)

func GetDuckDBConnection() (*duckdb.Conn, error) {
	// Open a connection to the DuckDB database
	connector, err := duckdb.NewConnector("rough.duckdb", nil)
	if err != nil {
		return nil, err
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	// db, err := sql.Open("duckdb", "file:mydatabase.duckdb?mode=memory")
	// if err != nil {
	// 	return nil, err
	// }

	// Optionally, you can set connection parameters or check the connection
	// if err = db.Ping(); err != nil {
	// 	return nil, err
	// }

	return conn.(*duckdb.Conn), nil
}
