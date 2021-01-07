package internal

func (v *Value) Remove()                        { v.remove(func() {}) }
func (v *Value) Set(vv string, version Version) { v.set(vv, version) }
