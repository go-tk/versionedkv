package internal_test

import (
	"testing"

	"github.com/go-tk/testcase"
	. "github.com/go-tk/versionedkv/memorystorage/internal"
	"github.com/stretchr/testify/assert"
)

func TestValue_Get(t *testing.T) {
	type Output struct {
		V       string
		Version Version
		Err     error
	}
	type State = ValueDetails
	type Context struct {
		V Value

		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{}
	}).Run(func(t *testing.T, c *Context) {
		v, version, err := c.V.Get()
		var output Output
		output.V = v
		output.Version = version
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.V.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("value removed").
			Then("should fail with error ErrValueRemoved").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Remove()
				c.ExpectedOutput.Err = ErrValueRemoved
				c.ExpectedState.IsRemoved = true
			}),
		tc.Copy().
			Given("value not set").
			Then("should return zero value and version"),
		tc.Copy().
			Given("value set").
			Then("should return corresponding value and version").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("foo", 1)
				c.ExpectedOutput.V = "foo"
				c.ExpectedOutput.Version = 1
				c.ExpectedState.V = "foo"
				c.ExpectedState.Version = 1
			}),
	)
}

func TestValue_AddWatcher(t *testing.T) {
	type Output struct {
		Err error
	}
	type State = ValueDetails
	type Context struct {
		V Value

		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{}
	}).Run(func(t *testing.T, c *Context) {
		_, err := c.V.AddWatcher()
		var output Output
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.V.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("value removed").
			Then("should fail with error ErrValueRemoved").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Remove()
				c.ExpectedOutput.Err = ErrValueRemoved
				c.ExpectedState.IsRemoved = true
			}),
		tc.Copy().
			Then("should succeed").
			PreRun(func(t *testing.T, c *Context) {
				c.ExpectedState.NumberOfWatchers = 1
			}),
	)
}

func TestValue_RemoveWatcher(t *testing.T) {
	type Input struct {
		Watcher Watcher
		Remover ValueRemover
	}
	type Output struct {
		Err error
	}
	type State = ValueDetails
	type Context struct {
		V Value

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Input: Input{
				Remover: func() {},
			},
		}
	}).Run(func(t *testing.T, c *Context) {
		err := c.V.RemoveWatcher(c.Input.Watcher, c.Input.Remover)
		var output Output
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.V.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("value removed").
			Then("should fail with error ErrValueRemoved").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Remove()
				c.ExpectedOutput.Err = ErrValueRemoved
				c.ExpectedState.IsRemoved = true
			}),
		tc.Copy().
			Given("value set and watcher already removed").
			Then("should succeed").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("abc", 100)
				w, err := c.V.AddWatcher()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.V.RemoveWatcher(w, nil)
				c.Input.Watcher = w
				c.ExpectedState.V = "abc"
				c.ExpectedState.Version = 100
			}),
		tc.Copy().
			Given("value not set and watcher added").
			Then("should succeed and remove value").
			PreRun(func(t *testing.T, c *Context) {
				w, err := c.V.AddWatcher()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Watcher = w
				c.Input.Remover = func() {
					c.Input.Remover = nil
				}
				c.ExpectedState.IsRemoved = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				assert.Nil(t, c.Input.Remover)
			}),
		tc.Copy().
			Given("value not set and multiple watchers added").
			Then("should succeed and do not remove value").
			PreRun(func(t *testing.T, c *Context) {
				_, err := c.V.AddWatcher()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				w, err := c.V.AddWatcher()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Watcher = w
				c.Input.Remover = func() {
					c.Input.Remover = nil
				}
				c.ExpectedState.NumberOfWatchers = 1
			}).
			PostRun(func(t *testing.T, c *Context) {
				assert.NotNil(t, c.Input.Remover)
			}),
		tc.Copy().
			Given("value set and watcher added").
			Then("should succeed but do not remove value").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("bar", 88)
				w, err := c.V.AddWatcher()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.Input.Watcher = w
				c.Input.Remover = func() {
					c.Input.Remover = nil
				}
				c.ExpectedState.V = "bar"
				c.ExpectedState.Version = 88
			}).
			PostRun(func(t *testing.T, c *Context) {
				assert.NotNil(t, c.Input.Remover)
			}),
	)
}

func TestValue_CheckAndSet(t *testing.T) {
	type Input struct {
		Callback func(Version) (string, Version, bool)
	}
	type Output struct {
		OK  bool
		Err error
	}
	type State = ValueDetails
	type Context struct {
		V Value
		W Watcher

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{}
	}).Run(func(t *testing.T, c *Context) {
		ok, err := c.V.CheckAndSet(c.Input.Callback)
		var output Output
		output.OK = ok
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.V.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("value removed").
			Then("should fail with error ErrValueRemoved").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Remove()
				c.ExpectedOutput.Err = ErrValueRemoved
				c.ExpectedState.IsRemoved = true
			}),
		tc.Copy().
			When("callback function failed").
			Then("should fail (1)").
			PreRun(func(t *testing.T, c *Context) {
				c.Input.Callback = func(currentVersion Version) (string, Version, bool) {
					if currentVersion == 0 {
						return "", 0, false
					}
					return "foo", 99, true
				}
			}),
		tc.Copy().
			Then("should succeed (1)").
			PreRun(func(t *testing.T, c *Context) {
				w, err := c.V.AddWatcher()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.W = w
				c.Input.Callback = func(currentVersion Version) (string, Version, bool) {
					if currentVersion != 0 {
						return "", 0, false
					}
					return "foo", 99, true
				}
				c.ExpectedOutput.OK = true
				c.ExpectedState.V = "foo"
				c.ExpectedState.Version = 99
			}).
			PostRun(func(t *testing.T, c *Context) {
				e := c.W.Event()
				select {
				case <-e:
				default:
					t.Fatal("event not fired")
				}
				ea := EventArgs{
					Value:   "foo",
					Version: 99,
				}
				assert.Equal(t, ea, c.W.EventArgs())
			}),
		tc.Copy().
			Given("value set").
			When("callback function failed").
			Then("should fail (2)").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("foo", 99)
				c.Input.Callback = func(currentVersion Version) (string, Version, bool) {
					if currentVersion == 99 {
						return "", 0, false
					}
					return "bar", 100, true
				}
				c.ExpectedState.V = "foo"
				c.ExpectedState.Version = 99
			}),
		tc.Copy().
			Given("value set").
			Then("should succeed (2)").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("foo", 99)
				w, err := c.V.AddWatcher()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.W = w
				c.Input.Callback = func(currentVersion Version) (string, Version, bool) {
					if currentVersion != 99 {
						return "", 0, false
					}
					return "bar", 100, true
				}
				c.ExpectedOutput.OK = true
				c.ExpectedState.V = "bar"
				c.ExpectedState.Version = 100
			}).
			PostRun(func(t *testing.T, c *Context) {
				e := c.W.Event()
				select {
				case <-e:
				default:
					t.Fatal("event not fired")
				}
				ea := EventArgs{
					Value:   "bar",
					Version: 100,
				}
				assert.Equal(t, ea, c.W.EventArgs())
			}),
	)
}

func TestValue_Clear(t *testing.T) {
	type Input struct {
		Version Version
		Remover ValueRemover
	}
	type Output struct {
		OK  bool
		Err error
	}
	type State = ValueDetails
	type Context struct {
		V Value
		W Watcher

		Input          Input
		ExpectedOutput Output
		ExpectedState  State
	}
	tc := testcase.New(func(t *testing.T) *Context {
		return &Context{
			Input: Input{
				Remover: func() {},
			},
		}
	}).Run(func(t *testing.T, c *Context) {
		ok, err := c.V.Clear(c.Input.Version, c.Input.Remover)
		var output Output
		output.OK = ok
		output.Err = err
		assert.Equal(t, c.ExpectedOutput, output)
		state := c.V.Inspect()
		assert.Equal(t, c.ExpectedState, state)
	})
	testcase.RunListParallel(t,
		tc.Copy().
			Given("value removed").
			Then("should fail with error ErrValueRemoved").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Remove()
				c.ExpectedOutput.Err = ErrValueRemoved
				c.ExpectedState.IsRemoved = true
			}),
		tc.Copy().
			Given("value not set").
			Then("should fail"),
		tc.Copy().
			Given("value set").
			When("given version is not equal to current version").
			Then("should fail").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("abc", 99)
				c.Input.Version = 100
				c.ExpectedState.V = "abc"
				c.ExpectedState.Version = 99
			}),
		tc.Copy().
			Given("value set").
			When("given version is equal to current version").
			Then("should succeed").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("abc", 100)
				c.Input.Version = 100
				c.ExpectedOutput.OK = true
				c.ExpectedState.IsRemoved = true
			}),
		tc.Copy().
			Given("value set and no watcher added").
			Then("should succeed and remove value").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("abc", 99)
				c.Input.Remover = func() {
					c.Input.Remover = nil
				}
				c.ExpectedOutput.OK = true
				c.ExpectedState.IsRemoved = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				assert.Nil(t, c.Input.Remover)
			}),
		tc.Copy().
			Given("value set and watcher added").
			Then("should succeed and remove value").
			PreRun(func(t *testing.T, c *Context) {
				c.V.Set("abc", 99)
				w, err := c.V.AddWatcher()
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				c.W = w
				c.Input.Remover = func() {
					c.Input.Remover = nil
				}
				c.ExpectedOutput.OK = true
				c.ExpectedState.IsRemoved = true
			}).
			PostRun(func(t *testing.T, c *Context) {
				e := c.W.Event()
				select {
				case <-e:
				default:
					t.Fatal("event not fired")
				}
				var ea EventArgs
				assert.Equal(t, ea, c.W.EventArgs())
				assert.Nil(t, c.Input.Remover)
			}),
	)
}

func TestValue_NewValue(t *testing.T) {
	v := NewValue("abc", 999)
	vv, version, err := v.Get()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	assert.Equal(t, "abc", vv)
	assert.Equal(t, Version(999), version)
}
