package etltool

type Upstream[K any] struct {
	upstream
	Id        string
	handler   func() []K
	m         *Manager
	autoClose bool
}

type SourceOption[K any] func(u *Upstream[K])

func WithNoAutoClose[K any]() SourceOption[K] {
	return func(u *Upstream[K]) {
		u.autoClose = false
	}
}

func NewSource[K any](m *Manager, id string, replicas int, handler func() []K, options ...SourceOption[K]) *Upstream[K] {
	u := &Upstream[K]{
		Id:        id,
		upstream:  upstream{outstreams: m.m.NewOutStreamSet()},
		handler:   handler,
		m:         m,
		autoClose: true,
	}
	for _, opt := range options {
		opt(u)
	}
	m.replicas = append(m.replicas, replica{
		replicas: replicas,
		run:      u.run,
	})
	return u
}

func (u *Upstream[K]) Stop() {
	u.stop()
	u.m.wg.Done()
}

func (u *Upstream[K]) Run() {
	for _, d := range u.handler() {
		u.deliver(d)
		u.m.addProcessing(u.outstreams.Size())
	}
}

func (u *Upstream[K]) run() {
	u.Run()
	if u.autoClose {
		u.Stop()
	}
}
