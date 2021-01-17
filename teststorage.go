package versionedkv

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-tk/testcase"
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
	type Context struct {
		S Storage

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Input: Input{
				Ctx: context.Background(),
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.S = sf()
	}).Run(func(t *testing.T, c *Context) {
		value, version, err := c.S.GetValue(c.Input.Ctx, c.Input.Key)
		var output Output
		output.Value = value
		output.Version = version
		for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
		}
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.S.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	}).Teardown(func(t *testing.T, c *Context) {
		if c.ExpectedOutput.Err != ErrStorageClosed {
			err := c.S.Close()
			assert.NoError(t, err)
		}
	})
	testcase.RunListParallel(t, []testcase.TestCase{
		tc.Copy().
			Given("storage closed").
			Then("should fail with error ErrStorageClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.S.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedOutput.Err = ErrStorageClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			When("value for given key does not exist").
			Then("should return nil version").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.Key = "foo"
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key does not exist").
			Then("should return nil version").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "123",
						Version: version,
					},
				}
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists").
			Then("should return corresponding value and version").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "bar"
				c.ExpectedOutput.Value = "123"
				c.ExpectedOutput.Version = version
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "123",
						Version: version,
					},
				}
			}),
	})
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
	type Context struct {
		S  Storage
		WG *sync.WaitGroup

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Input: Input{
				Ctx: context.Background(),
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.S = sf()
	}).Run(func(t *testing.T, c *Context) {
		value, newVersion, err := c.S.WaitForValue(c.Input.Ctx, c.Input.Key, c.Input.OldVersion)
		if wg := c.WG; wg != nil {
			wg.Wait()
		}
		var output Output
		output.Value = value
		output.NewVersion = newVersion
		for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
		}
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.S.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	}).Teardown(func(t *testing.T, c *Context) {
		if c.ExpectedOutput.Err != ErrStorageClosed {
			err := c.S.Close()
			assert.NoError(t, err)
		}
	})
	testcase.RunListParallel(t, []testcase.TestCase{
		tc.Copy().
			Given("storage closed").
			Then("should fail with error ErrStorageClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.S.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedOutput.Err = ErrStorageClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			When("value for given key does not exist").
			Then("should block until value has been created").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.Key = "foo"
				var wg sync.WaitGroup
				wg.Add(1)
				c.WG = &wg
				time.AfterFunc(100*time.Millisecond, func() {
					defer wg.Done()
					version, err := c.S.UpdateValue(context.Background(), "foo", "123abc", nil)
					if !assert.NoError(t, err) {
						return
					}
					if !assert.Nil(t, version) {
						return
					}
					version, err = c.S.CreateValue(context.Background(), "foo", "123abc")
					if !assert.NoError(t, err) {
						return
					}
					if !assert.NotNil(t, version) {
						return
					}
					c.ExpectedOutput.Value = "123abc"
					c.ExpectedOutput.NewVersion = version
					c.ExpectedState.Values = map[string]ValueDetails{
						"foo": {
							V:       "123abc",
							Version: version,
						},
					}
				})
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists and given old-version is equal to current version of value").
			Then("should block until value has been updated").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.Input.OldVersion = version
				var wg sync.WaitGroup
				wg.Add(1)
				c.WG = &wg
				time.AfterFunc(100*time.Millisecond, func() {
					defer wg.Done()
					version, err := c.S.UpdateValue(context.Background(), "foo", "123abc", nil)
					if !assert.NoError(t, err) {
						return
					}
					if !assert.NotNil(t, version) {
						return
					}
					c.ExpectedOutput.Value = "123abc"
					c.ExpectedOutput.NewVersion = version
					c.ExpectedState.Values = map[string]ValueDetails{
						"foo": {
							V:       "123abc",
							Version: version,
						},
					}
				})
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists and given old-version is equal to current version of value").
			Then("should block until value has been recreated").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.Input.OldVersion = version
				var wg sync.WaitGroup
				wg.Add(1)
				c.WG = &wg
				time.AfterFunc(100*time.Millisecond, func() {
					defer wg.Done()
					ok, err := c.S.DeleteValue(context.Background(), "foo", c.Input.OldVersion)
					if !assert.NoError(t, err) {
						return
					}
					if !assert.True(t, ok) {
						return
					}
					version, err = c.S.CreateOrUpdateValue(context.Background(), "foo", "123abc", nil)
					if !assert.NoError(t, err) {
						return
					}
					if !assert.NotNil(t, version) {
						return
					}
					c.ExpectedOutput.Value = "123abc"
					c.ExpectedOutput.NewVersion = version
					c.ExpectedState.Values = map[string]ValueDetails{
						"foo": {
							V:       "123abc",
							Version: version,
						},
					}
				})
			}),
		tc.Copy().
			Given("storage with value").
			Then("should return corresponding value and version (1)").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedOutput.Value = "123"
				c.ExpectedOutput.NewVersion = version
				c.ExpectedState.Values = map[string]ValueDetails{
					"foo": {
						V:       "123",
						Version: version,
					},
				}
			}),
		tc.Copy().
			Given("storage with value").
			Then("should return corresponding value and Version (2)").
			PreRun(func(t *testing.T, c *Context) {
				oldVersion, err := c.S.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, oldVersion) {
					t.FailNow()
				}
				newVersion, err := c.S.UpdateValue(context.Background(), "foo", "123abc", oldVersion)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, newVersion) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.Input.OldVersion = oldVersion
				c.ExpectedOutput.Value = "123abc"
				c.ExpectedOutput.NewVersion = newVersion
				c.ExpectedState.Values = map[string]ValueDetails{
					"foo": {
						V:       "123abc",
						Version: newVersion,
					},
				}
			}),
		tc.Copy().
			Given("ctx timed out").
			Then("should fail with error DeadlineExceeded").
			PreRun(func(t *testing.T, c *Context) {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_ = cancel
				c.Input.Ctx = ctx
				c.Input.Key = "foo"
				c.ExpectedOutput.Err = context.DeadlineExceeded
			}),
	})
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
	type Context struct {
		S             Storage
		OutputVersion Version

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Input: Input{
				Ctx: context.Background(),
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.S = sf()
	}).Run(func(t *testing.T, c *Context) {
		version, err := c.S.CreateValue(c.Input.Ctx, c.Input.Key, c.Input.Value)
		c.OutputVersion = version
		var output Output
		output.VersionIsNotNil = version != nil
		for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
		}
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
	}).Teardown(func(t *testing.T, c *Context) {
		state := c.S.Inspect()
		assert.Equal(t, c.ExpectedState, state)
		if c.ExpectedOutput.Err != ErrStorageClosed {
			err := c.S.Close()
			assert.NoError(t, err)
		}
	})
	testcase.RunListParallel(t, []testcase.TestCase{
		tc.Copy().
			Given("storage closed").
			Then("should fail with error ErrStorageClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.S.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedOutput.Err = ErrStorageClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			When("value for given key does not exist").
			Then("should create value and return non-nil version").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.Key = "foo"
				c.Input.Value = "123"
				c.ExpectedOutput.VersionIsNotNil = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				c.ExpectedState.Values = map[string]ValueDetails{
					"foo": {
						V:       "123",
						Version: c.OutputVersion,
					},
				}
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists").
			Then("should not create value and return nil version").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "foo", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedState.Values = map[string]ValueDetails{
					"foo": {
						V:       "123",
						Version: version,
					},
				}
			}),
	})
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
	type Context struct {
		S                Storage
		OldVersion       Version
		OutputNewVersion Version

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Input: Input{
				Ctx: context.Background(),
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.S = sf()
	}).Run(func(t *testing.T, c *Context) {
		version, err := c.S.UpdateValue(c.Input.Ctx, c.Input.Key, c.Input.Value, c.Input.OldVersion)
		c.OutputNewVersion = version
		var output Output
		output.NewVersionIsNotNil = version != nil
		for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
		}
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
	}).Teardown(func(t *testing.T, c *Context) {
		state := c.S.Inspect()
		assert.Equal(t, c.ExpectedState, state)
		if c.ExpectedOutput.Err != ErrStorageClosed {
			err := c.S.Close()
			assert.NoError(t, err)
		}
	})
	testcase.RunListParallel(t, []testcase.TestCase{
		tc.Copy().
			Given("storage closed").
			Then("should fail with error ErrStorageClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.S.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedOutput.Err = ErrStorageClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists").
			Then("should update value to new version").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.OldVersion = version
				c.Input.Key = "bar"
				c.Input.Value = "test"
				c.ExpectedOutput.NewVersionIsNotNil = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				assert.NotEqual(t, c.OldVersion, c.OutputNewVersion)
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "test",
						Version: c.OutputNewVersion,
					},
				}
			}),
		tc.Copy().
			When("value for given key does not exist").
			Then("should return nil version").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.Key = "bar"
				c.Input.Value = "test"
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists and given old-version is equal to current version of value").
			Then("should update value to new version").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "bar"
				c.Input.Value = "test"
				c.Input.OldVersion = version
				c.ExpectedOutput.NewVersionIsNotNil = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				assert.NotEqual(t, c.Input.OldVersion, c.OutputNewVersion)
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "test",
						Version: c.OutputNewVersion,
					},
				}
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists and given old-version is not equal to current version of value").
			Then("should not update value and return nil version").
			PreRun(func(t *testing.T, c *Context) {
				oldVersion, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, oldVersion) {
					t.FailNow()
				}
				newVersion, err := c.S.UpdateValue(context.Background(), "bar", "456", oldVersion)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, newVersion) {
					t.FailNow()
				}
				c.Input.Key = "bar"
				c.Input.Value = "test"
				c.Input.OldVersion = oldVersion
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "456",
						Version: newVersion,
					},
				}
			}),
	})
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
	type Context struct {
		S                Storage
		OldVersion       Version
		OutputNewVersion Version

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Input: Input{
				Ctx: context.Background(),
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.S = sf()
	}).Run(func(t *testing.T, c *Context) {
		version, err := c.S.CreateOrUpdateValue(c.Input.Ctx, c.Input.Key, c.Input.Value, c.Input.OldVersion)
		c.OutputNewVersion = version
		var output Output
		output.NewVersionIsNotNil = version != nil
		for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
		}
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
	}).Teardown(func(t *testing.T, c *Context) {
		state := c.S.Inspect()
		assert.Equal(t, c.ExpectedState, state)
		if c.ExpectedOutput.Err != ErrStorageClosed {
			err := c.S.Close()
			assert.NoError(t, err)
		}
	})
	testcase.RunListParallel(t, []testcase.TestCase{
		tc.Copy().
			Given("storage closed").
			Then("should fail with error ErrStorageClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.S.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedOutput.Err = ErrStorageClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			When("value for given key does not exist").
			Then("should create value and return non-nil version").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.Key = "foo"
				c.Input.Value = "123"
				c.ExpectedOutput.NewVersionIsNotNil = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				c.ExpectedState.Values = map[string]ValueDetails{
					"foo": {
						V:       "123",
						Version: c.OutputNewVersion,
					},
				}
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists").
			Then("should update value to new version").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.OldVersion = version
				c.Input.Key = "bar"
				c.Input.Value = "test"
				c.ExpectedOutput.NewVersionIsNotNil = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				assert.NotEqual(t, c.OldVersion, c.OutputNewVersion)
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "test",
						Version: c.OutputNewVersion,
					},
				}
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists and given old-version is equal to current version of value").
			Then("should update value to new version").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "bar"
				c.Input.Value = "test"
				c.Input.OldVersion = version
				c.ExpectedOutput.NewVersionIsNotNil = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				assert.NotEqual(t, c.Input.OldVersion, c.OutputNewVersion)
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "test",
						Version: c.OutputNewVersion,
					},
				}
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists and given old-version is not equal to current version of value").
			Then("should not update value and return nil version").
			PreRun(func(t *testing.T, c *Context) {
				oldVersion, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, oldVersion) {
					t.FailNow()
				}
				newVersion, err := c.S.UpdateValue(context.Background(), "bar", "456", oldVersion)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, newVersion) {
					t.FailNow()
				}
				c.Input.Key = "bar"
				c.Input.Value = "test"
				c.Input.OldVersion = oldVersion
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "456",
						Version: newVersion,
					},
				}
			}),
	})
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
	type Context struct {
		S Storage

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Input: Input{
				Ctx: context.Background(),
			},
		}
	}).Setup(func(t *testing.T, c *Context) {
		c.S = sf()
	}).Run(func(t *testing.T, c *Context) {
		ok, err := c.S.DeleteValue(c.Input.Ctx, c.Input.Key, c.Input.Version)
		var output Output
		output.OK = ok
		for err2 := errors.Unwrap(err); err2 != nil; err, err2 = err2, errors.Unwrap(err2) {
		}
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.S.Inspect()
		assert.Equal(t, c.ExpectedState, state)
		c.S.Close()
	})
	testcase.RunListParallel(t, []testcase.TestCase{
		tc.Copy().
			Given("storage closed").
			Then("should fail with error ErrStorageClosed").
			PreRun(func(t *testing.T, c *Context) {
				err := c.S.Close()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Key = "foo"
				c.ExpectedOutput.Err = ErrStorageClosed
				c.ExpectedState.IsClosed = true
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists").
			Then("should succeed").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "bar"
				c.ExpectedOutput.OK = true
			}),
		tc.Copy().
			When("value for given key does not exist").
			Then("should fail").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.Key = "bar"
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists and given version is equal to current version of value").
			Then("should succeed").
			PreRun(func(t *testing.T, c *Context) {
				version, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, version) {
					t.FailNow()
				}
				c.Input.Key = "bar"
				c.Input.Version = version
				c.ExpectedOutput.OK = true
			}),
		tc.Copy().
			Given("storage with value").
			When("value for given key exists and given version is not equal to current version of value").
			Then("should fail").
			PreRun(func(t *testing.T, c *Context) {
				oldVersion, err := c.S.CreateValue(context.Background(), "bar", "123")
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, oldVersion) {
					t.FailNow()
				}
				newVersion, err := c.S.UpdateValue(context.Background(), "bar", "123abc", oldVersion)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.NotNil(t, newVersion) {
					t.FailNow()
				}
				c.Input.Key = "bar"
				c.Input.Version = oldVersion
				c.ExpectedState.Values = map[string]ValueDetails{
					"bar": {
						V:       "123abc",
						Version: newVersion,
					},
				}
			}),
	})
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
