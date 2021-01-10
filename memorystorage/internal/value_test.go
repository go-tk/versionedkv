package internal_test

import (
	"strconv"
	"testing"

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
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Output            Output
		State             State

		t *testing.T
		v *Value
	}
	testCases := []TestCase{
		{
			Given: "value removed",
			Then:  "should fail with error ErrValueRemoved",
			Setup: func(tc *TestCase) {
				tc.v.Remove()
			},
			Output: Output{
				Err: ErrValueRemoved,
			},
			State: State{
				IsRemoved: true,
			},
		},
		{
			Given: "value not set",
			Then:  "should return zero value and version",
		},
		{
			Given: "value set",
			Then:  "should return corresponding value and version",
			Setup: func(tc *TestCase) {
				tc.v.Set("foo", 1)
			},
			Output: Output{
				V:       "foo",
				Version: 1,
			},
			State: State{
				V:       "foo",
				Version: 1,
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			var v Value
			tc.v = &v

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			vv, version, err := v.Get()

			var output Output
			output.V = vv
			output.Version = version
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				f(&tc)
			}

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
}

func TestValue_AddWatcher(t *testing.T) {
	type Output struct {
		Err error
	}
	type State = ValueDetails
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Output            Output
		State             State

		t *testing.T
		v *Value
	}
	testCases := []TestCase{
		{
			Given: "value removed",
			Then:  "should fail with error ErrValueRemoved",
			Setup: func(tc *TestCase) {
				tc.v.Remove()
			},
			Output: Output{
				Err: ErrValueRemoved,
			},
			State: State{
				IsRemoved: true,
			},
		},
		{
			Then: "should succeed",
			State: State{
				NumberOfWatchers: 1,
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			var v Value
			tc.v = &v

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			_, err := v.AddWatcher()

			var output Output
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				f(&tc)
			}

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
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
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t *testing.T
		v *Value
	}
	testCases := []TestCase{
		{
			Given: "value removed",
			Then:  "should fail with error ErrValueRemoved",
			Setup: func(tc *TestCase) {
				tc.v.Remove()
			},
			Output: Output{
				Err: ErrValueRemoved,
			},
			State: State{
				IsRemoved: true,
			},
		},
		{
			Then: "should succeed",
		},
		{
			Given: "value set and watcher already removed",
			Then:  "should succeed",
			Setup: func(tc *TestCase) {
				tc.v.Set("abc", 100)
				w, err := tc.v.AddWatcher()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				tc.v.RemoveWatcher(w, nil)
				tc.Input.Watcher = w
			},
			State: State{
				V:       "abc",
				Version: 100,
			},
		},
		{
			Given: "value not set and watcher added",
			Then:  "should succeed and remove value",
			Setup: func(tc *TestCase) {
				w, err := tc.v.AddWatcher()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				tc.Input.Watcher = w
				tc.Input.Remover = func() {
					tc.Input.Remover = nil
				}
			},
			Teardown: func(tc *TestCase) {
				assert.Nil(tc.t, tc.Input.Remover)
			},
			State: State{
				IsRemoved: true,
			},
		},
		{
			Given: "value not set and multiple watchers added",
			Then:  "should succeed and do not remove value",
			Setup: func(tc *TestCase) {
				_, err := tc.v.AddWatcher()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				w, err := tc.v.AddWatcher()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				tc.Input.Watcher = w
				tc.Input.Remover = func() {
					tc.Input.Remover = nil
				}
			},
			Teardown: func(tc *TestCase) {
				assert.NotNil(tc.t, tc.Input.Remover)
			},
			State: State{
				NumberOfWatchers: 1,
			},
		},
		{
			Given: "value set and watcher added",
			Then:  "should succeed but do not remove value",
			Setup: func(tc *TestCase) {
				tc.v.Set("bar", 88)
				w, err := tc.v.AddWatcher()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				tc.Input.Watcher = w
				tc.Input.Remover = func() {
					tc.Input.Remover = nil
				}
			},
			Teardown: func(tc *TestCase) {
				assert.NotNil(tc.t, tc.Input.Remover)
			},
			State: State{
				V:       "bar",
				Version: 88,
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			var v Value
			tc.v = &v

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			err := v.RemoveWatcher(tc.Input.Watcher, tc.Input.Remover)

			var output Output
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				f(&tc)
			}

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
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
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t *testing.T
		v *Value
		w Watcher
	}
	testCases := []TestCase{
		{
			Given: "value removed",
			Then:  "should fail with error ErrValueRemoved",
			Setup: func(tc *TestCase) {
				tc.v.Remove()
			},
			Output: Output{
				Err: ErrValueRemoved,
			},
			State: State{
				IsRemoved: true,
			},
		},
		{
			When: "callback function failed",
			Then: "should fail (1)",
			Input: Input{
				Callback: func(currentVersion Version) (string, Version, bool) {
					if currentVersion == 0 {
						return "", 0, false
					}
					return "foo", 99, true
				},
			},
		},
		{
			Then: "should succeed (1)",
			Setup: func(tc *TestCase) {
				w, err := tc.v.AddWatcher()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				tc.w = w
			},
			Input: Input{
				Callback: func(currentVersion Version) (string, Version, bool) {
					if currentVersion != 0 {
						return "", 0, false
					}
					return "foo", 99, true
				},
			},
			Output: Output{
				OK: true,
			},
			Teardown: func(tc *TestCase) {
				e := tc.w.Event()
				select {
				case <-e:
				default:
					tc.t.Fail()
				}
				ea := EventArgs{
					Value:   "foo",
					Version: 99,
				}
				assert.Equal(tc.t, ea, tc.w.EventArgs())

			},
			State: State{
				V:       "foo",
				Version: 99,
			},
		},
		{
			Given: "value set",
			When:  "callback function failed",
			Then:  "should fail (2)",
			Setup: func(tc *TestCase) {
				tc.v.Set("foo", 99)
			},
			Input: Input{
				Callback: func(currentVersion Version) (string, Version, bool) {
					if currentVersion == 99 {
						return "", 0, false
					}
					return "bar", 100, true
				},
			},
			State: State{
				V:       "foo",
				Version: 99,
			},
		},
		{
			Given: "value set",
			Then:  "should succeed (2)",
			Setup: func(tc *TestCase) {
				tc.v.Set("foo", 99)
				w, err := tc.v.AddWatcher()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
				tc.w = w
			},
			Input: Input{
				Callback: func(currentVersion Version) (string, Version, bool) {
					if currentVersion != 99 {
						return "", 0, false
					}
					return "bar", 100, true
				},
			},
			Output: Output{
				OK: true,
			},
			Teardown: func(tc *TestCase) {
				e := tc.w.Event()
				select {
				case <-e:
				default:
					tc.t.Fail()
				}
				ea := EventArgs{
					Value:   "bar",
					Version: 100,
				}
				assert.Equal(tc.t, ea, tc.w.EventArgs())

			},
			State: State{
				V:       "bar",
				Version: 100,
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			var v Value
			tc.v = &v

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			ok, err := v.CheckAndSet(tc.Input.Callback)

			var output Output
			output.OK = ok
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				f(&tc)
			}

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
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
	type TestCase struct {
		Given, When, Then string
		Setup, Teardown   func(*TestCase)
		Input             Input
		Output            Output
		State             State

		t *testing.T
		v *Value
		w Watcher
	}
	testCases := []TestCase{
		{
			Given: "value removed",
			Then:  "should fail with error ErrValueRemoved",
			Setup: func(tc *TestCase) {
				tc.v.Remove()
			},
			Output: Output{
				Err: ErrValueRemoved,
			},
			State: State{
				IsRemoved: true,
			},
		},
		{
			Given: "value not set",
			Then:  "should fail",
		},
		{
			Given: "value set",
			When:  "given version is not equal to current version",
			Then:  "should fail",
			Setup: func(tc *TestCase) {
				tc.v.Set("abc", 99)
			},
			Input: Input{
				Version: 100,
			},
			State: State{
				V:       "abc",
				Version: 99,
			},
		},
		{
			Given: "value set",
			When:  "given version is equal to current version",
			Then:  "should succeed",
			Setup: func(tc *TestCase) {
				tc.v.Set("abc", 100)
			},
			Input: Input{
				Version: 100,
				Remover: func() {},
			},
			Output: Output{
				OK: true,
			},
			State: State{
				IsRemoved: true,
			},
		},
		{
			Given: "value set and no watcher added",
			Then:  "should succeed and remove value",
			Setup: func(tc *TestCase) {
				tc.v.Set("abc", 99)
				tc.Input.Remover = func() {
					tc.Input.Remover = nil
				}
			},
			Output: Output{
				OK: true,
			},
			Teardown: func(tc *TestCase) {
				assert.Nil(tc.t, tc.Input.Remover)
			},
			State: State{
				IsRemoved: true,
			},
		},
		{
			Given: "value set and watcher added",
			Then:  "should succeed and do not remove value",
			Setup: func(tc *TestCase) {
				tc.v.Set("abc", 99)
				tc.Input.Remover = func() {
					tc.Input.Remover = nil
				}
				_, err := tc.v.AddWatcher()
				if !assert.NoError(tc.t, err) {
					tc.t.FailNow()
				}
			},
			Output: Output{
				OK: true,
			},
			Teardown: func(tc *TestCase) {
				assert.NotNil(tc.t, tc.Input.Remover)
			},
			State: State{
				NumberOfWatchers: 1,
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			t.Logf("\nGIVEN: %s\nWHEN: %s\nTHEN: %s", tc.Given, tc.When, tc.Then)
			tc.t = t

			var v Value
			tc.v = &v

			if f := tc.Setup; f != nil {
				f(&tc)
			}

			ok, err := v.Clear(tc.Input.Version, tc.Input.Remover)

			var output Output
			output.OK = ok
			output.Err = err
			assert.Equal(t, tc.Output, output)

			if f := tc.Teardown; f != nil {
				f(&tc)
			}

			state := v.Inspect()
			assert.Equal(t, tc.State, state)
		})
	}
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
