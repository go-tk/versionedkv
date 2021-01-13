package versionedkv

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// StorageFactory is the type of the function creating storages.
type StorageFactory func() (storage Storage)

// DoTestStorage test storages created by the given storage factory.
func DoTestStorage(t *testing.T, sf StorageFactory) {
	t.Run("GetValue", func(t *testing.T) {
		t.Parallel()
		doTestStorageGetValue(t, sf)
	})
	t.Run("WaitForValue", func(t *testing.T) {
		t.Parallel()
		doTestStorageWaitForValue(t, sf)
	})
	t.Run("CreateValue", func(t *testing.T) {
		t.Parallel()
		doTestStorageCreateValue(t, sf)
	})
	t.Run("UpdateValue", func(t *testing.T) {
		t.Parallel()
		doTestStorageUpdateValue(t, sf)
	})
	t.Run("CreateOrUpdateValue", func(t *testing.T) {
		t.Parallel()
		doTestStorageCreateOrUpdateValue(t, sf)
	})
	t.Run("DeleteValue", func(t *testing.T) {
		t.Parallel()
		doTestStorageDeleteValue(t, sf)
	})
	t.Run("Close", func(t *testing.T) {
		t.Parallel()
		doTestStorageClose(t, sf)
	})
	t.Run("RaceCondition", func(t *testing.T) {
		t.Parallel()
		doTestStorageRaceCondition(t, sf)
	})
}

func doTestStorageGetValue(t *testing.T, sf StorageFactory) {
	type Input struct {
		Ctx context.Context
		Key string
	}
	type Output struct {
		Value   string
		Version Version
		Err     error
	}
	type State = StorageDetails
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t *testing.T
		s Storage
	}
	testCases := []TestCase{
		{
			Given: "storage closed",
			Then:  "should fail with error ErrStorageClosed",
			Setup: func(tc *TestCase) {
				err := tc.s.Close()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Err: ErrStorageClosed,
			},
			State: State{
				IsClosed: true,
			},
		},
		{
			When: "value for given key does not exist",
			Then: "should return nil version",
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key does not exist",
			Then:  "should return nil version",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				vd := tc.State.Values["bar"]
				vd.Version = version
				tc.State.Values["bar"] = vd
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "123",
					},
				},
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists",
			Then:  "should return corresponding value and version",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.Output.Version = version
				vd := tc.State.Values["bar"]
				vd.Version = version
				tc.State.Values["bar"] = vd
			},
			Output: Output{
				Value: "123",
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "bar",
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "123",
					},
				},
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			s := sf()
			defer s.Close()
			tc.s = s

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			value, version, err := s.GetValue(tc.Input.Ctx, tc.Input.Key)

			var output Output
			output.Value = value
			output.Version = version
			for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
			}
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				f(&tc)
			}

			state := s.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func doTestStorageWaitForValue(t *testing.T, sf StorageFactory) {
	type Input struct {
		Ctx        context.Context
		Key        string
		OldVersion Version
	}
	type Output struct {
		Value      string
		NewVersion Version
		Err        error
	}
	type State = StorageDetails
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t  *testing.T
		s  Storage
		wg *sync.WaitGroup
	}
	testCases := []TestCase{
		{
			Given: "storage closed",
			Then:  "should fail with error ErrStorageClosed",
			Setup: func(tc *TestCase) {
				err := tc.s.Close()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Err: ErrStorageClosed,
			},
			State: State{
				IsClosed: true,
			},
		},
		{
			When: "value for given key does not exist",
			Then: "should block until value has been created",
			Setup: func(tc *TestCase) {
				var wg sync.WaitGroup
				wg.Add(1)
				tc.wg = &wg
				time.AfterFunc(100*time.Millisecond, func() {
					defer wg.Done()
					version, err := tc.s.UpdateValue(context.Background(), "foo", "123abc", nil)
					if !assert.NoError(tc.t, err) {
						return
					}
					if !assert.Nil(tc.t, version) {
						return
					}
					version, err = tc.s.CreateValue(context.Background(), "foo", "123abc")
					if !assert.NoError(tc.t, err) {
						return
					}
					if !assert.NotNil(tc.t, version) {
						return
					}
					tc.Output.NewVersion = version
					vd := tc.State.Values["foo"]
					vd.Version = version
					tc.State.Values["foo"] = vd
				})
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Value: "123abc",
			},
			State: State{
				Values: map[string]ValueDetails{
					"foo": {
						V: "123abc",
					},
				},
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists and given old-version is equal to current version of value",
			Then:  "should block until value has been updated",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.Input.OldVersion = version

				var wg sync.WaitGroup
				wg.Add(1)
				tc.wg = &wg
				time.AfterFunc(100*time.Millisecond, func() {
					defer wg.Done()
					version, err := tc.s.UpdateValue(context.Background(), "foo", "123abc", nil)
					if !assert.NoError(tc.t, err) {
						return
					}
					if !assert.NotNil(tc.t, version) {
						return
					}
					tc.Output.NewVersion = version
					vd := tc.State.Values["foo"]
					vd.Version = version
					tc.State.Values["foo"] = vd
				})
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Value: "123abc",
			},
			State: State{
				Values: map[string]ValueDetails{
					"foo": {
						V: "123abc",
					},
				},
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists and given old-version is equal to current version of value",
			Then:  "should block until value has been recreated",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.Input.OldVersion = version

				var wg sync.WaitGroup
				wg.Add(1)
				tc.wg = &wg
				time.AfterFunc(100*time.Millisecond, func() {
					defer wg.Done()
					ok, err := tc.s.DeleteValue(context.Background(), "foo", tc.Input.OldVersion)
					if !assert.NoError(tc.t, err) {
						return
					}
					if !assert.True(tc.t, ok) {
						return
					}
					version, err = tc.s.CreateOrUpdateValue(context.Background(), "foo", "123abc", nil)
					if !assert.NoError(tc.t, err) {
						return
					}
					if !assert.NotNil(tc.t, version) {
						return
					}
					tc.Output.NewVersion = version
					vd := tc.State.Values["foo"]
					vd.Version = version
					tc.State.Values["foo"] = vd
				})
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Value: "123abc",
			},
			State: State{
				Values: map[string]ValueDetails{
					"foo": {
						V: "123abc",
					},
				},
			},
		},
		{
			Given: "storage with value",
			Then:  "should return corresponding value and version (1)",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.Output.NewVersion = version
				vd := tc.State.Values["foo"]
				vd.Version = version
				tc.State.Values["foo"] = vd
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Value: "123",
			},
			State: State{
				Values: map[string]ValueDetails{
					"foo": {
						V: "123",
					},
				},
			},
		},
		{
			Given: "storage with value",
			Then:  "should return corresponding value and Version (2)",
			Setup: func(tc *TestCase) {
				oldVersion, err := tc.s.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, oldVersion) {
					tc.t.FailNow()
				}
				newVersion, err := tc.s.UpdateValue(context.Background(), "foo", "123abc", oldVersion)
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, newVersion) {
					tc.t.FailNow()
				}
				tc.Input.OldVersion = oldVersion
				tc.Output.NewVersion = newVersion
				vd := tc.State.Values["foo"]
				vd.Version = newVersion
				tc.State.Values["foo"] = vd
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Value: "123abc",
			},
			State: State{
				Values: map[string]ValueDetails{
					"foo": {
						V: "123abc",
					},
				},
			},
		},
		{
			When: "ctx timed out",
			Then: "should fail with error DeadlineExceeded",
			Setup: func(tc *TestCase) {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_ = cancel
				tc.Input.Ctx = ctx
			},
			Input: Input{
				Key: "foo",
			},
			Output: Output{
				Err: context.DeadlineExceeded,
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			s := sf()
			defer s.Close()
			tc.s = s

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			value, newVersion, err := s.WaitForValue(tc.Input.Ctx, tc.Input.Key, tc.Input.OldVersion)

			if wg := tc.wg; wg != nil {
				wg.Wait()
			}

			var output Output
			output.Value = value
			output.NewVersion = newVersion
			for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
			}
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				f(&tc)
			}

			state := s.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func doTestStorageCreateValue(t *testing.T, sf StorageFactory) {
	type Input struct {
		Ctx   context.Context
		Key   string
		Value string
	}
	type Output struct {
		VersionIsNotNil bool
		Err             error
	}
	type State = StorageDetails
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t                *testing.T
		s                Storage
		outputtedVersion Version
	}
	testCases := []TestCase{
		{
			Given: "storage closed",
			Then:  "should fail with error ErrStorageClosed",
			Setup: func(tc *TestCase) {
				err := tc.s.Close()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Err: ErrStorageClosed,
			},
			State: State{
				IsClosed: true,
			},
		},
		{
			When: "value for given key does not exist",
			Then: "should create value and return non-nil version",
			Input: Input{
				Ctx:   context.Background(),
				Key:   "foo",
				Value: "123",
			},
			Output: Output{
				VersionIsNotNil: true,
			},
			Teardown: func(tc *TestCase) {
				vd := tc.State.Values["foo"]
				vd.Version = tc.outputtedVersion
				tc.State.Values["foo"] = vd
			},
			State: State{
				Values: map[string]ValueDetails{
					"foo": {
						V: "123",
					},
				},
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists",
			Then:  "should not create value and return nil version",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				vd := tc.State.Values["foo"]
				vd.Version = version
				tc.State.Values["foo"] = vd
			},
			Input: Input{
				Ctx:   context.Background(),
				Key:   "foo",
				Value: "123abc",
			},
			State: State{
				Values: map[string]ValueDetails{
					"foo": {
						V: "123",
					},
				},
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			s := sf()
			defer s.Close()
			tc.s = s

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			version, err := s.CreateValue(tc.Input.Ctx, tc.Input.Key, tc.Input.Value)

			var output Output
			output.VersionIsNotNil = version != nil
			for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
			}
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				tc.outputtedVersion = version
				f(&tc)
			}

			state := s.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func doTestStorageUpdateValue(t *testing.T, sf StorageFactory) {
	type Input struct {
		Ctx        context.Context
		Key        string
		Value      string
		OldVersion Version
	}
	type Output struct {
		NewVersionIsNotNil bool
		Err                error
	}
	type State = StorageDetails
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t                   *testing.T
		s                   Storage
		oldVersion          Version
		outputtedNewVersion Version
	}
	testCases := []TestCase{
		{
			Given: "storage closed",
			Then:  "should fail with error ErrStorageClosed",
			Setup: func(tc *TestCase) {
				err := tc.s.Close()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Err: ErrStorageClosed,
			},
			State: State{
				IsClosed: true,
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists",
			Then:  "should update value to new version",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.oldVersion = version
			},
			Input: Input{
				Ctx:   context.Background(),
				Key:   "bar",
				Value: "test",
			},
			Output: Output{
				NewVersionIsNotNil: true,
			},
			Teardown: func(tc *TestCase) {
				assert.NotEqual(tc.t, tc.oldVersion, tc.outputtedNewVersion)
				vd := tc.State.Values["bar"]
				vd.Version = tc.outputtedNewVersion
				tc.State.Values["bar"] = vd
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "test",
					},
				},
			},
		},
		{
			When: "value for given key does not exist",
			Then: "should return nil version",
			Input: Input{
				Ctx:   context.Background(),
				Key:   "bar",
				Value: "test",
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists and given old-version is equal to current version of value",
			Then:  "should update value to new version",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.Input.OldVersion = version
			},
			Input: Input{
				Ctx:   context.Background(),
				Key:   "bar",
				Value: "test",
			},
			Output: Output{
				NewVersionIsNotNil: true,
			},
			Teardown: func(tc *TestCase) {
				assert.NotEqual(tc.t, tc.Input.OldVersion, tc.outputtedNewVersion)
				vd := tc.State.Values["bar"]
				vd.Version = tc.outputtedNewVersion
				tc.State.Values["bar"] = vd
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "test",
					},
				},
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists and given old-version is not equal to current version of value",
			Then:  "should not update value and return nil version",
			Setup: func(tc *TestCase) {
				oldVersion, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, oldVersion) {
					tc.t.FailNow()
				}
				newVersion, err := tc.s.UpdateValue(context.Background(), "bar", "456", oldVersion)
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, newVersion) {
					tc.t.FailNow()
				}
				tc.Input.OldVersion = oldVersion
				vd := tc.State.Values["bar"]
				vd.Version = newVersion
				tc.State.Values["bar"] = vd
			},
			Input: Input{
				Ctx:   context.Background(),
				Key:   "bar",
				Value: "test",
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "456",
					},
				},
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			s := sf()
			defer s.Close()
			tc.s = s

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			newVersion, err := s.UpdateValue(tc.Input.Ctx, tc.Input.Key, tc.Input.Value, tc.Input.OldVersion)

			var output Output
			output.NewVersionIsNotNil = newVersion != nil
			for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
			}
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				tc.outputtedNewVersion = newVersion
				f(&tc)
			}

			state := s.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func doTestStorageCreateOrUpdateValue(t *testing.T, sf StorageFactory) {
	type Input struct {
		Ctx        context.Context
		Key        string
		Value      string
		OldVersion Version
	}
	type Output struct {
		NewVersionIsNotNil bool
		Err                error
	}
	type State = StorageDetails
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t                   *testing.T
		s                   Storage
		oldVersion          Version
		outputtedNewVersion Version
	}
	testCases := []TestCase{
		{
			Given: "storage closed",
			Then:  "should fail with error ErrStorageClosed",
			Setup: func(tc *TestCase) {
				err := tc.s.Close()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Err: ErrStorageClosed,
			},
			State: State{
				IsClosed: true,
			},
		},
		{
			When: "value for given key does not exist",
			Then: "should create value and return non-nil version",
			Input: Input{
				Ctx:   context.Background(),
				Key:   "foo",
				Value: "123",
			},
			Output: Output{
				NewVersionIsNotNil: true,
			},
			Teardown: func(tc *TestCase) {
				vd := tc.State.Values["foo"]
				vd.Version = tc.outputtedNewVersion
				tc.State.Values["foo"] = vd
			},
			State: State{
				Values: map[string]ValueDetails{
					"foo": {
						V: "123",
					},
				},
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists",
			Then:  "should update value to new version",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.oldVersion = version
			},
			Input: Input{
				Ctx:   context.Background(),
				Key:   "bar",
				Value: "test",
			},
			Output: Output{
				NewVersionIsNotNil: true,
			},
			Teardown: func(tc *TestCase) {
				assert.NotEqual(tc.t, tc.oldVersion, tc.outputtedNewVersion)
				vd := tc.State.Values["bar"]
				vd.Version = tc.outputtedNewVersion
				tc.State.Values["bar"] = vd
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "test",
					},
				},
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists and given old-version is equal to current version of value",
			Then:  "should update value to new version",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.Input.OldVersion = version
			},
			Input: Input{
				Ctx:   context.Background(),
				Key:   "bar",
				Value: "test",
			},
			Output: Output{
				NewVersionIsNotNil: true,
			},
			Teardown: func(tc *TestCase) {
				assert.NotEqual(tc.t, tc.Input.OldVersion, tc.outputtedNewVersion)
				vd := tc.State.Values["bar"]
				vd.Version = tc.outputtedNewVersion
				tc.State.Values["bar"] = vd
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "test",
					},
				},
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists and given old-version is not equal to current version of value",
			Then:  "should not update value and return nil version",
			Setup: func(tc *TestCase) {
				oldVersion, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, oldVersion) {
					tc.t.FailNow()
				}
				newVersion, err := tc.s.UpdateValue(context.Background(), "bar", "456", oldVersion)
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, newVersion) {
					tc.t.FailNow()
				}
				tc.Input.OldVersion = oldVersion
				vd := tc.State.Values["bar"]
				vd.Version = newVersion
				tc.State.Values["bar"] = vd
			},
			Input: Input{
				Ctx:   context.Background(),
				Key:   "bar",
				Value: "test",
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "456",
					},
				},
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			s := sf()
			defer s.Close()
			tc.s = s

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			newVersion, err := s.CreateOrUpdateValue(tc.Input.Ctx, tc.Input.Key, tc.Input.Value, tc.Input.OldVersion)

			var output Output
			output.NewVersionIsNotNil = newVersion != nil
			for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
			}
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				tc.outputtedNewVersion = newVersion
				f(&tc)
			}

			state := s.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func doTestStorageDeleteValue(t *testing.T, sf StorageFactory) {
	type Input struct {
		Ctx     context.Context
		Key     string
		Version Version
	}
	type Output struct {
		OK  bool
		Err error
	}
	type State = StorageDetails
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t *testing.T
		s Storage
	}
	testCases := []TestCase{
		{
			Given: "storage closed",
			Then:  "should fail with error ErrStorageClosed",
			Setup: func(tc *TestCase) {
				err := tc.s.Close()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "foo",
			},
			Output: Output{
				Err: ErrStorageClosed,
			},
			State: State{
				IsClosed: true,
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists",
			Then:  "should succeed",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "bar",
			},
			Output: Output{
				OK: true,
			},
		},
		{
			When: "value for given key does not exist",
			Then: "should fail",
			Input: Input{
				Ctx: context.Background(),
				Key: "bar",
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists and given version is equal to current version of value",
			Then:  "should succeed",
			Setup: func(tc *TestCase) {
				version, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, version) {
					tc.t.FailNow()
				}
				tc.Input.Version = version
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "bar",
			},
			Output: Output{
				OK: true,
			},
		},
		{
			Given: "storage with value",
			When:  "value for given key exists and given version is not equal to current version of value",
			Then:  "should fail",
			Setup: func(tc *TestCase) {
				oldVersion, err := tc.s.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, oldVersion) {
					tc.t.FailNow()
				}
				newVersion, err := tc.s.UpdateValue(context.Background(), "bar", "123abc", oldVersion)
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				if !assert.NotNil(tc.t, newVersion) {
					tc.t.FailNow()
				}
				tc.Input.Version = oldVersion
				vd := tc.State.Values["bar"]
				vd.Version = newVersion
				tc.State.Values["bar"] = vd
			},
			Input: Input{
				Ctx: context.Background(),
				Key: "bar",
			},
			State: State{
				Values: map[string]ValueDetails{
					"bar": {
						V: "123abc",
					},
				},
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			s := sf()
			defer s.Close()
			tc.s = s

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			ok, err := s.DeleteValue(tc.Input.Ctx, tc.Input.Key, tc.Input.Version)

			var output Output
			output.OK = ok
			for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
			}
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				f(&tc)
			}

			state := s.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func doTestStorageClose(t *testing.T, sf StorageFactory) {
	s := sf()
	err := s.Close()
	assert.NoError(t, err)
	err = s.Close()
	for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
	}
	assert.Equal(t, ErrStorageClosed, err)
}

func doTestStorageRaceCondition(t *testing.T, sf StorageFactory) {
	const N = 10
	s := sf()
	defer s.Close()
	worker := func(key string) {
		const (
			actionGetValue = iota
			actionWaitForValue
			actionCreateValue
			actionUpdateValue
			actionCreateOrUpdateValue
			actionDeleteValue
			actionMax
		)
		type nextActions [actionMax]bool
		na := nextActions{
			actionGetValue: true,
		}
		var value string
		actions := make([]int, actionMax)
		var k int
		for version, prevVersion := Version(nil), Version(nil); ; prevVersion, version = version, nil {
			value += "1"
			actions = actions[:0]
			for a, v := range na {
				if v {
					actions = append(actions, a)
				}
			}
			switch actions[rand.Intn(len(actions))] {
			case actionGetValue:
				if prevVersion != nil {
					panic("unreachable")
				}
				var err error
				value, version, err = s.GetValue(context.Background(), key)
				if !assert.NoError(t, err) {
					return
				}
				if version == nil {
					na = nextActions{
						actionCreateValue:  true,
						actionWaitForValue: true,
					}
				} else {
					na = nextActions{
						actionWaitForValue:        true,
						actionUpdateValue:         true,
						actionCreateOrUpdateValue: true,
						actionDeleteValue:         true,
					}
				}
			case actionWaitForValue:
				d := time.Duration(100+rand.Intn(101)) * time.Millisecond
				ctx, cancel := context.WithTimeout(context.Background(), d)
				_ = cancel
				var err error
				value, version, err = s.WaitForValue(ctx, key, prevVersion)
				if err == context.DeadlineExceeded {
					err = nil
				}
				if !assert.NoError(t, err) {
					return
				}
				k++
				if version == nil {
					if prevVersion == nil {
						na = nextActions{
							actionCreateValue: true,
						}
					} else {
						version = prevVersion
						na = nextActions{
							actionUpdateValue:         true,
							actionCreateOrUpdateValue: true,
							actionDeleteValue:         true,
						}
					}
				} else {
					na = nextActions{
						actionUpdateValue:         true,
						actionCreateOrUpdateValue: true,
						actionDeleteValue:         true,
					}
				}
			case actionCreateValue:
				if prevVersion != nil {
					panic("unreachable")
				}
				var err error
				version, err = s.CreateValue(context.Background(), key, value)
				if !assert.NoError(t, err) {
					return
				}
				if version == nil {
					na = nextActions{
						actionGetValue:    true,
						actionDeleteValue: true,
					}
				} else {
					na = nextActions{
						actionWaitForValue:        true,
						actionUpdateValue:         true,
						actionCreateOrUpdateValue: true,
						actionDeleteValue:         true,
					}
				}
			case actionUpdateValue:
				if prevVersion == nil {
					panic("unreachable")
				}
				var err error
				version, err = s.UpdateValue(context.Background(), key, value, prevVersion)
				if !assert.NoError(t, err) {
					return
				}
				if version == nil {
					na = nextActions{
						actionGetValue:    true,
						actionDeleteValue: true,
					}
				} else {
					k++
					na = nextActions{
						actionWaitForValue:        true,
						actionUpdateValue:         true,
						actionCreateOrUpdateValue: true,
						actionDeleteValue:         true,
					}
				}
			case actionCreateOrUpdateValue:
				if prevVersion == nil {
					panic("unreachable")
				}
				var err error
				version, err = s.CreateOrUpdateValue(context.Background(), key, value, prevVersion)
				if !assert.NoError(t, err) {
					return
				}
				if version == nil {
					na = nextActions{
						actionGetValue:    true,
						actionDeleteValue: true,
					}
				} else {
					k++
					na = nextActions{
						actionWaitForValue:        true,
						actionUpdateValue:         true,
						actionCreateOrUpdateValue: true,
						actionDeleteValue:         true,
					}
				}
			case actionDeleteValue:
				var ok bool
				var err error
				ok, err = s.DeleteValue(context.Background(), key, prevVersion)
				if !assert.NoError(t, err) {
					return
				}
				if ok {
					if prevVersion != nil {
						k++
					}
					na = nextActions{
						actionCreateValue: true,
					}
				} else {
					if prevVersion == nil {
						na = nextActions{
							actionCreateValue: true,
						}
					} else {
						na = nextActions{
							actionGetValue: true,
						}
					}
				}
			}
			if k == N*N {
				return
			}
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key%d", i+1)
		for j := 0; j < N; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker(key)
			}()
		}
	}
	wg.Wait()
}
