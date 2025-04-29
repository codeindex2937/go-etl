package etltool

import (
	"sync"
)

type Stage[K any, V any] struct {
	downstream
	upstream
	Id      string
	wg      sync.WaitGroup
	handler func(input K) []V
	m       *Manager
}

func NewStage[K any, V any](m *Manager, id string, replicas int, handler func(input K) []V) *Stage[K, V] {
	s := &Stage[K, V]{
		Id: id,
		downstream: downstream{
			instreams: m.m.NewInStreamSet(),
		},
		upstream: upstream{outstreams: m.m.NewOutStreamSet()},
		handler:  handler,
		m:        m,
	}
	m.replicas = append(m.replicas, replica{
		replicas: replicas,
		run:      s.run,
	})
	return s
}

func (s *Stage[K, V]) run() {
	defer s.m.wg.Done()

	for {
		msg, ok := s.consume(s.m.aborted)
		if !ok {
			s.wg.Wait()
			s.stop()
			return
		}
		s.m.AddFinished(1)

		resp := s.handler(msg.(K))
		s.wg.Add(1)
		go func() {
			for _, d := range resp {
				s.m.AddProcessing(s.outstreams.Size())
				s.deliver(d)
			}
			s.wg.Done()
		}()
	}
}

func (s *Stage[K, V]) AddInputStream(input InStream) {
	s.instreams.Subscribe(input)
}
