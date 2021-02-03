package memorystorage_test

import (
	"testing"

	"github.com/go-tk/versionedkv"
	. "github.com/go-tk/versionedkv/memorystorage"
)

func TestMemoryStorage(t *testing.T) {
	versionedkv.DoTestStorage(t, func() (versionedkv.Storage, error) {
		return New(), nil
	})
}
