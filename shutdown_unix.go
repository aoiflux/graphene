//go:build !windows

package graphene

import (
	"os"
	"syscall"
)

func defaultShutdownSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT}
}
