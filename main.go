package main

import (
	// "fmt"

	"github.com/prabhatrastogik/tradesmart-go/internal/app"
	"github.com/prabhatrastogik/tradesmart-go/internal/config"
	"github.com/prabhatrastogik/tradesmart-go/internal/utils"
)

func main() {
	instruments := app.GetAllInstruments()
	db, err := config.GetDuckDBConnection()
	if err != nil {
		utils.GetLogger("main").Fatalf("Failed to connect to DuckDB: %v", err)
	}
	err = utils.WriteStructsToDuckDB(db, "instruments", "instruments", instruments)
	if err != nil {
		utils.GetLogger("main").Fatalf("Failed to write to DuckDB: %v", err)
	}
}
