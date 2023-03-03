package etltool

import (
	"context"
	"reflect"
	"sync"
)

type consumer struct {
	lock    sync.Mutex
	inPipes []reflect.SelectCase
}

func (c *consumer) addProvider(ch <-chan any) {
	c.lock.Lock()
	defer c.lock.Unlock()

	sc := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	c.inPipes = append(c.inPipes, sc)
}

func (c *consumer) consume(aborted context.Context) (interface{}, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	done := aborted.Done()
	sc := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)}

	for len(c.inPipes) > 0 {
		idx, value, ok := reflect.Select(append([]reflect.SelectCase{sc}, c.inPipes...))
		if idx == 0 {
			return nil, false
		}
		if ok {
			return value.Interface(), ok
		}

		if idx == len(c.inPipes) {
			c.inPipes = c.inPipes[:idx-1]
		} else {
			c.inPipes = append(c.inPipes[:idx-1], c.inPipes[idx:]...)
		}
	}

	return nil, false
}
