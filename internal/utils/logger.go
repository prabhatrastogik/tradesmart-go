package utils

import (
	"log"
	"os"
)

func GetLogger(prefix string) *log.Logger {
	file, err := os.OpenFile(prefix+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	return log.New(file, prefix, log.LstdFlags|log.Lshortfile)
}
