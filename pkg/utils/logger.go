package utils

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func SetupLogger(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
	if os.Getenv("LOG_FORMAT") != "json" {
		log.Logger = log.Output(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		})
	}
	log.Logger = log.With().
		Str("service", "ckic-manager").
		Timestamp().
		Logger()
}
