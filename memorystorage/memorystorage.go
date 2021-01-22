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
		val, version, err := ms.doGetValue(key)
		if err == internal.ErrValueRemoved {
			continue
		}
		return val, version2OpaqueVersion(version), err
	}
}

func (ms *memoryStorage) doGetValue(key string) (string, internal.Version, error) {
	if ms.isClosed() {
		return "", 0, versionedkv.ErrStorageClosed
	}
	opaqueValue, ok := ms.values.Load(key)
	if !ok {
		return "", 0, nil
	}
	value := opaqueValue.(*internal.Value)
	val, version, err := value.Get()
	if err != nil {
		return "", 0, err
	}
	return val, version, nil
}

func (ms *memoryStorage) WaitForValue(ctx context.Context, key string,
	opaqueOldVersion versionedkv.Version) (string, versionedkv.Version, error) {
	oldVersion := opaqueVersion2Version(opaqueOldVersion)
	for {
		val, newVersion, err := ms.doWaitForValue(ctx, key, oldVersion)
		if err == internal.ErrValueRemoved {
			continue
		}
		return val, version2OpaqueVersion(newVersion), err
	}
}

func (ms *memoryStorage) doWaitForValue(ctx context.Context, key string,
	oldVersion internal.Version) (string, internal.Version, error) {
	if ms.isClosed() {
		return "", 0, versionedkv.ErrStorageClosed
	}
	opaqueValue, ok := ms.values.Load(key)
	if !ok {
		opaqueValue, _ = ms.values.LoadOrStore(key, &internal.Value{})
	}
	value := opaqueValue.(*internal.Value)
	watcher, err := value.AddWatcher()
	if err != nil {
		return "", 0, err
	}
	val, version, err := value.Get()
	if err != nil {
		return "", 0, err
	}
	defer func() {
		if watcher != (internal.Watcher{}) {
			value.RemoveWatcher(watcher, func() { ms.values.Delete(key) })
		}
	}()
	if version != 0 && (oldVersion == 0 || version != oldVersion) {
		return val, version, nil
	}
	select {
	case <-watcher.Event():
		eventArgs := watcher.EventArgs()
		watcher = internal.Watcher{}
		return eventArgs.Value, eventArgs.Version, nil
	case <-ms.closure:
		watcher = internal.Watcher{}
		return "", 0, versionedkv.ErrStorageClosed
	case <-ctx.Done():
		return "", 0, ctx.Err()
	}
}

func (ms *memoryStorage) CreateValue(_ context.Context, key, val string) (versionedkv.Version, error) {
	for {
		version, err := ms.doCreateValue(key, val)
		if err == internal.ErrValueRemoved {
			continue
		}
		return version2OpaqueVersion(version), err
	}
}

func (ms *memoryStorage) doCreateValue(key, val string) (internal.Version, error) {
	if ms.isClosed() {
		return 0, versionedkv.ErrStorageClosed
	}
	version := ms.nextVersion()
	value := internal.NewValue(val, version)
	opaqueValue, valueExists := ms.values.LoadOrStore(key, value)
	if !valueExists {
		return version, nil
	}
	value = opaqueValue.(*internal.Value)
	ok, err := value.CheckAndSet(func(currentVersion internal.Version) (string, internal.Version, bool) {
		if currentVersion != 0 {
			return "", 0, false
		}
		return val, version, true
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return version, nil
}

func (ms *memoryStorage) UpdateValue(_ context.Context, key, val string,
	opaqueOldVersion versionedkv.Version) (versionedkv.Version, error) {
	oldVersion := opaqueVersion2Version(opaqueOldVersion)
	for {
		newVersion, err := ms.doUpdateValue(key, val, oldVersion)
		if err == internal.ErrValueRemoved {
			continue
		}
		return version2OpaqueVersion(newVersion), err
	}
}

func (ms *memoryStorage) doUpdateValue(key, val string, oldVersion internal.Version) (internal.Version, error) {
	if ms.isClosed() {
		return 0, versionedkv.ErrStorageClosed
	}
	opaqueValue, ok := ms.values.Load(key)
	if !ok {
		return 0, nil
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
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return newVersion, nil
}

func (ms *memoryStorage) CreateOrUpdateValue(_ context.Context, key, val string,
	opaqueOldVersion versionedkv.Version) (versionedkv.Version, error) {
	oldVersion := opaqueVersion2Version(opaqueOldVersion)
	for {
		newVersion, err := ms.doCreateOrUpdateValue(key, val, oldVersion)
		if err == internal.ErrValueRemoved {
			continue
		}
		return version2OpaqueVersion(newVersion), err
	}
}

func (ms *memoryStorage) doCreateOrUpdateValue(key, val string, oldVersion internal.Version) (internal.Version, error) {
	if ms.isClosed() {
		return 0, versionedkv.ErrStorageClosed
	}
	version := ms.nextVersion()
	value := internal.NewValue(val, version)
	opaqueValue, valueExists := ms.values.LoadOrStore(key, value)
	if !valueExists {
		return version, nil
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
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	if newVersion == 0 {
		return version, nil
	}
	return newVersion, nil
}

func (ms *memoryStorage) DeleteValue(_ context.Context, key string, opaqueVersion versionedkv.Version) (bool, error) {
	oldVersion := opaqueVersion2Version(opaqueVersion)
	for {
		ok, err := ms.doDeleteValue(key, oldVersion)
		if err == internal.ErrValueRemoved {
			continue
		}
		return ok, err
	}
}

func (ms *memoryStorage) doDeleteValue(key string, version internal.Version) (bool, error) {
	if ms.isClosed() {
		return false, versionedkv.ErrStorageClosed
	}
	opaqueValue, ok := ms.values.Load(key)
	if !ok {
		return false, nil
	}
	value := opaqueValue.(*internal.Value)
	ok, err := value.Clear(version, func() { ms.values.Delete(key) })
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (ms *memoryStorage) Close() error {
	if atomic.SwapInt32(&ms.isClosed1, 1) != 0 {
		return versionedkv.ErrStorageClosed
	}
	close(ms.closure)
	return nil
}

func (ms *memoryStorage) Inspect(_ context.Context) (versionedkv.StorageDetails, error) {
	if ms.isClosed() {
		return versionedkv.StorageDetails{IsClosed: true}, nil
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
	}, nil
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
