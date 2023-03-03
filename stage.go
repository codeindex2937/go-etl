package etltool

import "reflect"

type Stage[K any, V any] struct {
	consumer
	provider
	handler func(input K) []V
}

func NewStage[K any, V any](m *Manager, replicas int, handler func(input K) []V) *Stage[K, V] {
	s := &Stage[K, V]{
		consumer: consumer{
			inPipes: make([]reflect.SelectCase, 0),
		},
		provider: provider{
			consumers: make([]chan<- any, 0),
		},
		handler: handler,
	}
	m.replicas = append(m.replicas, replica{
		replicas: replicas,
		run:      s.run,
	})
	return s
}

func NewStageFor[K any, V any, U any](m *Manager, replicas int, handler func(input K) []V, u *Stage[U, K]) *Stage[K, V] {
	s := NewStage(m, replicas, handler)
	PipeTo(u, s)
	return s
}

func NewStageFrom[K any, V any](m *Manager, replicas int, handler func(input K) []V, u *Source[K]) *Stage[K, V] {
	s := NewStage(m, replicas, handler)
	s.ReceiveFrom(u)
	return s
}

func (s *Stage[K, V]) run(m *Manager) {
	defer m.wg.Done()

	for {
		msg, ok := s.consume(m.aborted)
		if !ok {
			s.stop()
			return
		}
		for _, d := range s.handler(msg.(K)) {
			s.deliver(d)
		}
	}
}

func (s *Stage[K, V]) ReceiveFrom(r *Source[K]) *Stage[K, V] {
	ch := make(chan any, 5000)
	s.addProvider(ch)
	r.addConsumer(ch)
	return s
}

func (s *Stage[K, V]) OutputTo(r *Output[V]) *Stage[K, V] {
	ch := make(chan any, 5000)
	r.addProvider(ch)
	s.addConsumer(ch)
	return s
}

func PipeTo[K, U, V any](s *Stage[K, U], r *Stage[U, V]) *Stage[U, V] {
	ch := make(chan any, 5000)
	r.addProvider(ch)
	s.addConsumer(ch)
	return r
}
