package main

import "log"

type WatcherController struct {
	counter int
}

func newWatcherController() *WatcherController {
	return &WatcherController{counter: 0}
}

func (w *WatcherController) Increment() {
	w.counter++
	log.Printf("incrementing counter to %d", w.counter)
}

func (w *WatcherController) Decrement() {
	w.counter--
	log.Printf("decrementing counter to %d", w.counter)
}

func (w *WatcherController) Get() int {
	return w.counter
}
