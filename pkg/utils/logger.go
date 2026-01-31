package utils

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func SetupLogger(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
	var output io.Writer = os.Stdout
	if os.Getenv("LOG_FORMAT") != "json" {
		output = zerolog.SyncWriter(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		})
	}
	log.Logger = log.Output(output).With().Str("service", "ckic-manager").Logger()
}

func NodeLogger(nodeName string) zerolog.Logger {
	return log.With().Str("component", "node").Str("node", nodeName).Logger()
}
