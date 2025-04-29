package etltool

type Output[K any] struct {
	downstream
	handler func(input K)
	m       *Manager
}

func NewOutput[K any](m *Manager, replicas int, handler func(input K)) *Output[K] {
	s := &Output[K]{
		downstream: downstream{
			instreams: m.m.NewInStreamSet(),
		},
		handler: handler,
		m:       m,
	}
	m.replicas = append(m.replicas, replica{
		replicas: replicas,
		run:      s.run,
	})
	return s
}

func (d *Output[K]) run() {
	defer d.m.wg.Done()

	for {
		msg, ok := d.consume(d.m.aborted)
		if !ok {
			return
		}
		d.handler(msg.(K))
		d.m.AddFinished(1)
	}
}
