package main

import (
	"log"
	"sync"
)

type WatcherCounter struct {
	counter int
	mx      sync.Mutex
}

func newWatcherCounter() *WatcherCounter {
	return &WatcherCounter{counter: 0}
}

func (w *WatcherCounter) Increment() {
	w.mx.Lock()
	defer w.mx.Unlock()

	w.counter++
	log.Printf("incrementing counter to %d", w.counter)
}

func (w *WatcherCounter) Decrement() {
	w.mx.Lock()
	defer w.mx.Unlock()

	w.counter--
	log.Printf("decrementing counter to %d", w.counter)
}

func (w *WatcherCounter) Get() int {
	w.mx.Lock()
	defer w.mx.Unlock()

	return w.counter
}
