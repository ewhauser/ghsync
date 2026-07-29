// Package observer provides shared observer-composition helpers.
package observer

import "reflect"

// FanOut invokes notify for every non-nil observer in declaration order.
func FanOut[T any](observers []T, notify func(T)) {
	for _, candidate := range observers {
		value := reflect.ValueOf(candidate)
		if !value.IsValid() || nilable(value.Kind()) && value.IsNil() {
			continue
		}
		notify(candidate)
	}
}

func nilable(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return true
	default:
		return false
	}
}
