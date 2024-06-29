package etltool

import (
	"context"
	"sync"
)

type replica struct {
	replicas int
	run      func(*Manager)
}
type Manager struct {
	aborted  context.Context
	wg       sync.WaitGroup
	replicas []replica
	m        MessageSystem
}

func (m *Manager) Wait() {
	m.wg.Wait()
}

func (m *Manager) Start() {
	for _, s := range m.replicas {
		for i := 0; i < s.replicas; i++ {
			m.wg.Add(1)
			go s.run(m)
		}
	}
}

func HandleStage[K any, V any, U any](m *Manager, id string, replicas int, handler func(input K) []V, u *Stage[U, K]) *Stage[K, V] {
	s := NewStage(m, id, replicas, handler)
	Dispatch(m, u, s)
	return s
}

func HandleSource[K any, V any](m *Manager, id string, replicas int, handler func(input K) []V, u *Upstream[K]) *Stage[K, V] {
	s := NewStage(m, id, replicas, handler)
	SetSource(m, s, u)
	return s
}

func SetSource[K any, V any](m *Manager, s *Stage[K, V], r *Upstream[K]) *Stage[K, V] {
	u, d := m.m.NewPipe(r.Id)
	s.addUpStream(u)
	r.addDownStream(d)
	return s
}

func SetOutput[K any, V any](m *Manager, s *Stage[K, V], r *Output[V]) *Stage[K, V] {
	u, d := m.m.NewPipe(s.Id)
	r.addUpStream(u)
	s.addDownStream(d)
	return s
}

func Dispatch[K, U, V any](m *Manager, s *Stage[K, U], r *Stage[U, V]) *Stage[U, V] {
	u, d := m.m.NewPipe(s.Id)
	r.addUpStream(u)
	s.addDownStream(d)
	return r
}
