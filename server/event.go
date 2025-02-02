package server

import (
	"context"
	"sync"
)

type Event struct {
	once sync.Once
	C    chan struct{}
}

func NewEvent() *Event {
	return &Event{
		C: make(chan struct{}),
	}
}

func (e *Event) Set() {
	e.once.Do(func() {
		close(e.C)
	})
}

func (e *Event) Wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-e.C:
	}
}
