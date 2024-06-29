package etltool

type Upstream[K any] struct {
	upstream
	Id      string
	handler func() []K
}

func NewSource[K any](m *Manager, id string, replicas int, handler func() []K) *Upstream[K] {
	s := &Upstream[K]{
		Id:       id,
		upstream: upstream{outstreams: m.m.NewOutStreamSet()},
		handler:  handler,
	}
	m.replicas = append(m.replicas, replica{
		replicas: replicas,
		run:      s.run,
	})
	return s
}

func (r *Upstream[K]) run(m *Manager) {
	defer m.wg.Done()
	for _, d := range r.handler() {
		r.deliver(d)
	}
	r.stop()
}
