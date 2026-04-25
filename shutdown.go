package graphene

import (
	"os"
	"os/signal"
	"sync"
)

// HandleSignals registers a graceful shutdown hook that closes the graph when
// any of the provided signals is received. If no signals are provided, a
// platform-appropriate default set is used.
//
// The returned stop function unregisters the signal handler.
func (g *Graph) HandleSignals(signals ...os.Signal) func() {
	if len(signals) == 0 {
		signals = defaultShutdownSignals()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, signals...)
	return bindCloseOnSignal(g.Close, sigCh, func() { signal.Stop(sigCh) })
}

func bindCloseOnSignal(closeFn func() error, sigCh <-chan os.Signal, stopNotify func()) func() {
	done := make(chan struct{})
	var once sync.Once
	stop := func() {
		once.Do(func() {
			if stopNotify != nil {
				stopNotify()
			}
			close(done)
		})
	}

	go func() {
		select {
		case <-done:
			return
		case <-sigCh:
			select {
			case <-done:
				return
			default:
			}
			_ = closeFn()
			stop()
		}
	}()

	return stop
}
