package internal

import (
	"errors"
	"sync"
)

type Value struct {
	mu        sync.Mutex
	v         string
	version   Version
	watchers  map[*watcher]struct{}
	isRemoved bool
}

func NewValue(vv string, version Version) *Value {
	var v Value
	v.set(vv, version)
	return &v
}

func (v *Value) Get() (string, Version, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.isRemoved {
		return "", 0, ErrValueRemoved
	}
	return v.v, v.version, nil
}

func (v *Value) AddWatcher() (Watcher, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.isRemoved {
		return Watcher{}, ErrValueRemoved
	}
	watcher1 := new(watcher).Init()
	if v.watchers == nil {
		v.watchers = make(map[*watcher]struct{})
	}
	v.watchers[watcher1] = struct{}{}
	wrappedWatcher := Watcher{watcher1}
	return wrappedWatcher, nil
}

func (v *Value) RemoveWatcher(wrappedWatcher Watcher, remover ValueRemover) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.isRemoved {
		return ErrValueRemoved
	}
	watcher := wrappedWatcher.w
	if _, ok := v.watchers[watcher]; !ok {
		return nil
	}
	delete(v.watchers, watcher)
	if len(v.watchers) >= 1 {
		return nil
	}
	v.watchers = nil
	if v.version == 0 {
		v.remove(remover)
	}
	return nil
}

func (v *Value) CheckAndSet(callback func(Version) (string, Version, bool)) (bool, error) {
	mu := &v.mu
	mu.Lock()
	defer func() {
		if mu != nil {
			mu.Unlock()
		}
	}()
	if v.isRemoved {
		return false, ErrValueRemoved
	}
	vv, version, ok := callback(v.version)
	if !ok {
		return false, nil
	}
	v.set(vv, version)
	watchers := v.watchers
	v.watchers = nil
	mu.Unlock()
	mu = nil
	for watcher := range watchers {
		eventArgs := EventArgs{
			Value:   vv,
			Version: version,
		}
		watcher.FireEvent(eventArgs)
	}
	return true, nil
}

func (v *Value) set(vv string, version Version) {
	if version == 0 {
		panic("set value to zero version")
	}
	v.v = vv
	v.version = version
}

func (v *Value) Clear(version Version, remover ValueRemover) (bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.isRemoved {
		return false, ErrValueRemoved
	}
	if v.version == 0 {
		return false, nil
	}
	if version != 0 && v.version != version {
		return false, nil
	}
	v.v = ""
	v.version = 0
	if v.watchers == nil {
		v.remove(remover)
	}
	return true, nil
}

func (v *Value) remove(remover ValueRemover) {
	remover()
	v.isRemoved = true
}

type Version uint64

type Watcher struct{ w *watcher }

func (w Watcher) Event() <-chan struct{} { return w.w.Event() }
func (w Watcher) EventArgs() EventArgs   { return w.w.EventArgs() }

type EventArgs struct {
	Value   string
	Version Version
}

type ValueRemover func()

var ErrValueRemoved error = errors.New("internal: value removed")

type watcher struct {
	event     chan struct{}
	eventArgs EventArgs
}

func (w *watcher) Init() *watcher {
	w.event = make(chan struct{})
	return w
}

func (w *watcher) FireEvent(eventArgs EventArgs) {
	w.eventArgs = eventArgs
	close(w.event)
}

func (w *watcher) Event() <-chan struct{} {
	return w.event
}

func (w *watcher) EventArgs() EventArgs {
	return w.eventArgs
}
