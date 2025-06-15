package app

import (
	"github.com/prabhatrastogik/tradesmart-go/internal/config"
	"github.com/prabhatrastogik/tradesmart-go/internal/utils"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

var (
	logger = utils.GetLogger("instruments")
)

// Function that use Zerodha kite to get all instruments
func GetAllInstruments() []kiteconnect.Instrument {
	kiteClient, err := GetZerodhaClient(config.GetZerodhaCredentials())
	if err != nil {
		logger.Fatalf("Failed to get Zerodha client: %v", err)
	}
	// Get all instruments from Zerodha kite
	instruments, err := kiteClient.GetInstruments()
	if err != nil {
		logger.Fatalf("Failed to get instruments: %v", err)
	}
	return instruments
}
