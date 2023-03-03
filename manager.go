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
