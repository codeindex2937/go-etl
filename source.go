package etltool

type Source[K any] struct {
	provider
	handler func() []K
}

func NewSource[K any](m *Manager, replicas int, handler func() []K) *Source[K] {
	s := &Source[K]{
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

func (r *Source[K]) run(m *Manager) {
	defer m.wg.Done()
	for _, d := range r.handler() {
		r.deliver(d)
	}
	r.stop()
}
