package versionedkv

import (
	"context"
	"errors"
)

// Storage represents a versioned key/value storage.
type Storage interface {
	// GetValue retrieves the value for the given key.
	//
	// If the value does not exist, a nil version is returned.
	GetValue(ctx context.Context, key string) (value string, version Version, err error)

	// WaitForValue waits for the creation, update, deletion of the value for the given key.
	//
	// a) If the value does not exist and the old-version is not given, it blocks until
	// the value has been created;
	// b) If the value does not exist and the old-version is given, a nil new-version
	// is returned right away.
	// c) If the value exists and the old-version is given and the current version of
	// the value is equal to the old-version, it blocks until the value has been
	// updated to a new version or deleted (a nil new-version is returned);
	// d) Otherwise the value is returned right away.
	WaitForValue(ctx context.Context, key string, oldVersion Version) (value string, newVersion Version, err error)

	// Create creates the value for the given key.
	//
	// a) If the value does not exist, it creates the value as the given value;
	// b) Otherwise a nil version is returned.
	CreateValue(ctx context.Context, key string, value string) (version Version, err error)

	// Update updates the value for the given key.
	//
	// a) If the value exists and the old-version is not given, it updates the value to
	// a new version as the given value;
	// b) If the value exists and the old-version is given and the current version of the
	// value is equal to the old-version, it updates the value to a new version as the
	// given value;
	// c) Otherwise a nil new-version is returned.
	UpdateValue(ctx context.Context, key, value string, oldVersion Version) (newVersion Version, err error)

	// CreateOrUpdateValue performs CreateValue or UpdateValue as an atomic operation.
	//
	// a) If the value does not exist, it creates the value as the given value;
	// b) If the value exists and the old-version is not given, it updates the value to
	// a new version as the given value;
	// c) If the value exists and the old-version is given and the current version of the
	// value is equal to the old-version, it updates the value to a new version as the
	// given value;
	// d) Otherwise a nil new-version is returned.
	CreateOrUpdateValue(ctx context.Context, key, value string, oldVersion Version) (newVersion Version, err error)

	// DeleteValue deletes the value for the given key.
	//
	// a) If the value exists and the version is not given, it deletes the value;
	// b) If the value exists and the version is given and the current version of the
	// value is equal to the version, it deletes the value;
	// c) Otherwise false is returned.
	DeleteValue(ctx context.Context, key string, version Version) (ok bool, err error)

	// Close releases resources associated.
	Close() (err error)

	// Inspect returns detailed information for testing and debugging purposes.
	Inspect(ctx context.Context) (details StorageDetails, err error)
}

// Version represents a specific version of a value in a storage.
type Version interface{}

// StorageDetails represents the detailed information of a storage.
type StorageDetails struct {
	Values   map[string]ValueDetails
	IsClosed bool
}

// ValueDetails represents the detailed information of a value in a storage.
type ValueDetails struct {
	V       string
	Version Version
}

// ErrStorageClosed is returned when operating on a storage that has already been closed.
var ErrStorageClosed error = errors.New("versionedkv: storage closed")
