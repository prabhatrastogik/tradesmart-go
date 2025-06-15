package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Credentials struct {
	APIKey    string
	UserName  string
	Password  string
	TOTPKey   string
	APISecret string
}

const (
	TokenFile string = "access_token.enc"
)

func GetZerodhaCredentials() Credentials {
	requiredVars := []string{"ZERODHA_API_KEY", "ZERODHA_USERNAME", "ZERODHA_PASSWORD", "ZERODHA_TOTP_KEY", "ZERODHA_API_SECRET"}
	missing := false

	for _, key := range requiredVars {
		if os.Getenv(key) == "" {
			missing = true
			break
		}
	}

	if missing {
		err := godotenv.Load()
		if err != nil {
			log.Fatalf("Error loading .env file: %v", err)
		}
	}

	// Populate credentials struct from environment variables
	creds := Credentials{
		APIKey:    os.Getenv("ZERODHA_API_KEY"),
		UserName:  os.Getenv("ZERODHA_USERNAME"),
		Password:  os.Getenv("ZERODHA_PASSWORD"),
		TOTPKey:   os.Getenv("ZERODHA_TOTP_KEY"),
		APISecret: os.Getenv("ZERODHA_API_SECRET"),
	}
	return creds
}
