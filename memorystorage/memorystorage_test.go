package memorystorage_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-tk/versionedkv"
	. "github.com/go-tk/versionedkv/memorystorage"
	"github.com/stretchr/testify/assert"
)

func TestMemoryStorage(t *testing.T) {
	versionedkv.DoTestStorage(t, func() (versionedkv.Storage, error) {
		return New(), nil
	})
}

func TestMemoryStorage_Close(t *testing.T) {
	s := New()
	time.AfterFunc(100*time.Millisecond, func() {
		s.Close() // WaitForValue should fail with error ErrStorageClosed
	})
	_, _, err := s.WaitForValue(context.Background(), "foo", nil)
	assert.Equal(t, err, versionedkv.ErrStorageClosed)
}
