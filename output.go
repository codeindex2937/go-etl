package etltool

type Output[K any] struct {
	downstream
	handler func(input K)
}

func NewOutput[K any](m *Manager, replicas int, handler func(input K)) *Output[K] {
	s := &Output[K]{
		downstream: downstream{
			instreams: m.m.NewInStreamSet(),
		},
		handler: handler,
	}
	m.replicas = append(m.replicas, replica{
		replicas: replicas,
		run:      s.run,
	})
	return s
}

func (w *Output[K]) run(m *Manager) {
	defer m.wg.Done()

	for {
		msg, ok := w.consume(m.aborted)
		if !ok {
			return
		}
		w.handler(msg.(K))
	}
}
