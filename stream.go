package etltool

import (
	"context"
	"sync"
)

type upstream struct {
	outstreams OutStreamSet
}

func (s *upstream) deliver(msg any) {
	s.outstreams.Publish(msg)
}

func (s *upstream) stop() {
	s.outstreams.Term()
}

func (s *upstream) addDownStream(d OutStream) {
	s.outstreams.Register(d)
}

type downstream struct {
	lock      sync.Mutex
	instreams InStreamSet
}

func (c *downstream) addUpStream(s InStream) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.instreams.Subscribe(s)
}

func (c *downstream) consume(aborted context.Context) (any, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.instreams.Fetch(aborted)
}
