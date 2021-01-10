package memorystorage

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/go-tk/versionedkv"
	"github.com/go-tk/versionedkv/memorystorage/internal"
)

// New creates a new memory storage.
func New() versionedkv.Storage {
	var ms memoryStorage
	ms.closure = make(chan struct{})
	return &ms
}

type memoryStorage struct {
	values    sync.Map
	version   internal.Version
	isClosed1 int32
	closure   chan struct{}
}

func (ms *memoryStorage) GetValue(_ context.Context, key string) (string, versionedkv.Version, error) {
	for {
		val, version, err, done := ms.tryGetValue(key)
		if !done {
			continue
		}
		return val, version2OpaqueVersion(version), err
	}
}

func (ms *memoryStorage) tryGetValue(key string) (string, internal.Version, error, bool) {
	if ms.isClosed() {
		return "", 0, versionedkv.ErrStorageClosed, true
	}
	opaqueValue, ok := ms.values.Load(key)
	if !ok {
		return "", 0, nil, true
	}
	value := opaqueValue.(*internal.Value)
	val, version, err := value.Get()
	if err != nil {
		if err != internal.ErrValueRemoved {
			panic("unreachable")
		}
		return "", 0, nil, false
	}
	return val, version, nil, true
}

func (ms *memoryStorage) WaitForValue(ctx context.Context, key string,
	opaqueOldVersion versionedkv.Version) (string, versionedkv.Version, error) {
	oldVersion := opaqueVersion2Version(opaqueOldVersion)
	for {
		val, newVersion, err, done := ms.tryWaitForValue(ctx, key, oldVersion)
		if !done {
			continue
		}
		return val, version2OpaqueVersion(newVersion), err
	}
}

func (ms *memoryStorage) tryWaitForValue(ctx context.Context, key string,
	oldVersion internal.Version) (string, internal.Version, error, bool) {
	if ms.isClosed() {
		return "", 0, versionedkv.ErrStorageClosed, true
	}
	opaqueValue, ok := ms.values.Load(key)
	if !ok {
		opaqueValue, _ = ms.values.LoadOrStore(key, &internal.Value{})
	}
	value := opaqueValue.(*internal.Value)
	watcher, err := value.AddWatcher()
	if err != nil {
		if err != internal.ErrValueRemoved {
			panic("unreachable")
		}
		return "", 0, nil, false
	}
	val, version, err := value.Get()
	if err != nil {
		if err != internal.ErrValueRemoved {
			panic("unreachable")
		}
		return "", 0, nil, false
	}
	defer func() {
		if watcher != (internal.Watcher{}) {
			value.RemoveWatcher(watcher, func() { ms.values.Delete(key) })
		}
	}()
	if version != 0 && (oldVersion == 0 || version != oldVersion) {
		return val, version, nil, true
	}
	select {
	case <-watcher.Event():
		eventArgs := watcher.EventArgs()
		watcher = internal.Watcher{}
		return eventArgs.Value, eventArgs.Version, nil, true
	case <-ms.closure:
		watcher = internal.Watcher{}
		return "", 0, versionedkv.ErrStorageClosed, true
	case <-ctx.Done():
		return "", 0, ctx.Err(), true
	}
}

func (ms *memoryStorage) CreateValue(_ context.Context, key, val string) (versionedkv.Version, error) {
	for {
		version, err, ok := ms.tryCreateValue(key, val)
		if !ok {
			continue
		}
		return version2OpaqueVersion(version), err
	}
}

func (ms *memoryStorage) tryCreateValue(key, val string) (internal.Version, error, bool) {
	if ms.isClosed() {
		return 0, versionedkv.ErrStorageClosed, true
	}
	version := ms.nextVersion()
	value := internal.NewValue(val, version)
	opaqueValue, valueExists := ms.values.LoadOrStore(key, value)
	if !valueExists {
		return version, nil, true
	}
	value = opaqueValue.(*internal.Value)
	ok, err := value.CheckAndSet(func(currentVersion internal.Version) (string, internal.Version, bool) {
		if currentVersion != 0 {
			return "", 0, false
		}
		return val, version, true
	})
	if err != nil {
		if err != internal.ErrValueRemoved {
			panic("unreachable")
		}
		return 0, nil, false
	}
	if !ok {
		return 0, nil, true
	}
	return version, nil, true
}

func (ms *memoryStorage) UpdateValue(_ context.Context, key, val string,
	opaqueOldVersion versionedkv.Version) (versionedkv.Version, error) {
	oldVersion := opaqueVersion2Version(opaqueOldVersion)
	for {
		newVersion, err, done := ms.tryUpdateValue(key, val, oldVersion)
		if !done {
			continue
		}
		return version2OpaqueVersion(newVersion), err
	}
}

func (ms *memoryStorage) tryUpdateValue(key, val string, oldVersion internal.Version) (internal.Version, error, bool) {
	if ms.isClosed() {
		return 0, versionedkv.ErrStorageClosed, true
	}
	opaqueValue, ok := ms.values.Load(key)
	if !ok {
		return 0, nil, true
	}
	value := opaqueValue.(*internal.Value)
	var newVersion internal.Version
	ok, err := value.CheckAndSet(func(currentVersion internal.Version) (string, internal.Version, bool) {
		if currentVersion == 0 {
			return "", 0, false
		}
		if oldVersion != 0 && currentVersion != oldVersion {
			return "", 0, false
		}
		newVersion = ms.nextVersion()
		return val, newVersion, true
	})
	if err != nil {
		if err != internal.ErrValueRemoved {
			panic("unreachable")
		}
		return 0, nil, false
	}
	if !ok {
		return 0, nil, true
	}
	return newVersion, nil, true
}

func (ms *memoryStorage) CreateOrUpdateValue(_ context.Context, key, val string,
	opaqueOldVersion versionedkv.Version) (versionedkv.Version, error) {
	oldVersion := opaqueVersion2Version(opaqueOldVersion)
	for {
		newVersion, err, done := ms.tryCreateOrUpdateValue(key, val, oldVersion)
		if !done {
			continue
		}
		return version2OpaqueVersion(newVersion), err
	}
}

func (ms *memoryStorage) tryCreateOrUpdateValue(key, val string, oldVersion internal.Version) (internal.Version, error, bool) {
	if ms.isClosed() {
		return 0, versionedkv.ErrStorageClosed, true
	}
	version := ms.nextVersion()
	value := internal.NewValue(val, version)
	opaqueValue, valueExists := ms.values.LoadOrStore(key, value)
	if !valueExists {
		return version, nil, true
	}
	value = opaqueValue.(*internal.Value)
	var newVersion internal.Version
	ok, err := value.CheckAndSet(func(currentVersion internal.Version) (string, internal.Version, bool) {
		if currentVersion == 0 {
			return val, version, true
		}
		if oldVersion != 0 && currentVersion != oldVersion {
			return "", 0, false
		}
		newVersion = ms.nextVersion()
		return val, newVersion, true
	})
	if err != nil {
		if err != internal.ErrValueRemoved {
			panic("unreachable")
		}
		return 0, nil, false
	}
	if !ok {
		return 0, nil, true
	}
	if newVersion == 0 {
		return version, nil, true
	}
	return newVersion, nil, true
}

func (ms *memoryStorage) DeleteValue(_ context.Context, key string, opaqueVersion versionedkv.Version) (bool, error) {
	oldVersion := opaqueVersion2Version(opaqueVersion)
	for {
		ok, err, done := ms.tryDeleteValue(key, oldVersion)
		if !done {
			continue
		}
		return ok, err
	}
}

func (ms *memoryStorage) tryDeleteValue(key string, version internal.Version) (bool, error, bool) {
	if ms.isClosed() {
		return false, versionedkv.ErrStorageClosed, true
	}
	opaqueValue, ok := ms.values.Load(key)
	if !ok {
		return false, nil, true
	}
	value := opaqueValue.(*internal.Value)
	ok, err := value.Clear(version, func() { ms.values.Delete(key) })
	if err != nil {
		if err != internal.ErrValueRemoved {
			panic("unreachable")
		}
		return false, nil, false
	}
	return ok, nil, true
}

func (ms *memoryStorage) Close() error {
	if atomic.SwapInt32(&ms.isClosed1, 1) != 0 {
		return versionedkv.ErrStorageClosed
	}
	close(ms.closure)
	return nil
}

func (ms *memoryStorage) Inspect() versionedkv.StorageDetails {
	if ms.isClosed() {
		return versionedkv.StorageDetails{
			IsClosed: true,
		}
	}
	var valueDetails map[string]versionedkv.ValueDetails
	ms.values.Range(func(opaqueKey, opaqueValue interface{}) bool {
		if valueDetails == nil {
			valueDetails = make(map[string]versionedkv.ValueDetails)
		}
		key := opaqueKey.(string)
		value := opaqueValue.(*internal.Value)
		val, version, _ := value.Get()
		valueDetails[key] = versionedkv.ValueDetails{
			V:       val,
			Version: version,
		}
		return true
	})
	return versionedkv.StorageDetails{
		Values: valueDetails,
	}
}

func (ms *memoryStorage) isClosed() bool {
	return atomic.LoadInt32(&ms.isClosed1) != 0
}

func (ms *memoryStorage) nextVersion() internal.Version {
	return internal.Version(atomic.AddUint64((*uint64)(&ms.version), 1))
}

func version2OpaqueVersion(version internal.Version) versionedkv.Version {
	if version == 0 {
		return nil
	}
	return version
}

func opaqueVersion2Version(opaqueVersion versionedkv.Version) internal.Version {
	if opaqueVersion == nil {
		return 0
	}
	return opaqueVersion.(internal.Version)
}
