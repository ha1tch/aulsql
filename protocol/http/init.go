package http

import (
	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/protocol"
)

func init() {
	protocol.RegisterHTTPFactory(func(cfg protocol.ListenerConfig, logger *log.Logger) (protocol.Listener, error) {
		return NewListener(cfg, logger)
	})
}
