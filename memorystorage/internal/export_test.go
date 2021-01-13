package internal

func (v *Value) Remove() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.remove(func() {})
}

func (v *Value) Set(vv string, version Version) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.set(vv, version)
}

type ValueDetails struct {
	V                string
	Version          Version
	NumberOfWatchers int
	IsRemoved        bool
}

func (v *Value) Inspect() ValueDetails {
	v.mu.Lock()
	defer v.mu.Unlock()
	return ValueDetails{
		V:                v.v,
		Version:          v.version,
		NumberOfWatchers: len(v.watchers),
		IsRemoved:        v.isRemoved,
	}
}
