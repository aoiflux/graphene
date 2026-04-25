package graphene

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestBindCloseOnSignal_ClosesOnceAndStopsNotify(t *testing.T) {
	sigCh := make(chan os.Signal, 1)
	closed := make(chan struct{}, 2)
	stopped := make(chan struct{}, 2)

	stop := bindCloseOnSignal(
		func() error {
			closed <- struct{}{}
			return nil
		},
		sigCh,
		func() {
			stopped <- struct{}{}
		},
	)

	sigCh <- syscall.SIGTERM

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected close function to be called after signal")
	}

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("expected stop notify to be called once")
	}

	stop()
	stop()

	select {
	case <-stopped:
		t.Fatal("stop notify called more than once")
	default:
	}

	select {
	case <-closed:
		t.Fatal("close function called more than once")
	default:
	}
}

func TestBindCloseOnSignal_StopPreventsClose(t *testing.T) {
	sigCh := make(chan os.Signal, 1)
	closed := make(chan struct{}, 1)

	stop := bindCloseOnSignal(
		func() error {
			closed <- struct{}{}
			return nil
		},
		sigCh,
		nil,
	)

	stop()
	sigCh <- syscall.SIGTERM

	select {
	case <-closed:
		t.Fatal("close function should not be called after stop")
	case <-time.After(100 * time.Millisecond):
	}
}
