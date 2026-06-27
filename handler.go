package socketio

import (
	"fmt"
	"reflect"
)

type eventHandler struct {
	argTypes []reflect.Type
	f        reflect.Value
}

func (h *eventHandler) Call(args []reflect.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("event call error: %s", r)
			}
		}
	}()

	h.f.Call(args)

	return nil
}

func newEventHandler(f interface{}) *eventHandler {
	fv := reflect.ValueOf(f)

	if fv.Kind() != reflect.Func {
		panic("event handler must be a func")
	}

	ft := fv.Type()
	argTypes := make([]reflect.Type, ft.NumIn())

	for i := range argTypes {
		argTypes[i] = ft.In(i)
	}

	if len(argTypes) == 0 {
		argTypes = nil
	}

	return &eventHandler{
		argTypes: argTypes,
		f:        fv,
	}
}
