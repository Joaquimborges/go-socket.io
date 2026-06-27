package socketio

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventHandler(t *testing.T) {
	tests := []struct {
		f        interface{}
		ok       bool
		argTypes []interface{}
	}{
		{1, false, []interface{}{}},
		{func() {}, true, []interface{}{}},
		{func(int) {}, true, []interface{}{1}},
		{func(int) error { return nil }, true, []interface{}{1}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%#v", test.argTypes), func(t *testing.T) {
			should := assert.New(t)
			must := require.New(t)

			defer func() {
				r := recover()
				must.Equal(test.ok, r == nil)
			}()

			h := newEventHandler(test.f)
			must.Equal(len(test.argTypes), len(h.argTypes))

			for i := range h.argTypes {
				should.Equal(reflect.TypeOf(test.argTypes[i]), h.argTypes[i])
			}
		})
	}
}

func TestEventHandlerCall(t *testing.T) {
	tests := []struct {
		f    interface{}
		args []interface{}
		ok   bool
	}{
		{func() {}, nil, true},
		{func(int) {}, []interface{}{1}, true},
		{func() int { return 1 }, nil, true},
		{func(int) int { return 1 }, []interface{}{1}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%#v", test.f), func(t *testing.T) {
			must := require.New(t)

			h := newEventHandler(test.f)

			args := make([]reflect.Value, len(test.args))
			for i := range args {
				args[i] = reflect.ValueOf(test.args[i])
			}

			err := h.Call(args)
			must.Equal(test.ok, err == nil)
		})
	}
}
