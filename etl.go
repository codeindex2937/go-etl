package etltool

import (
	"context"
)

func New(aborted context.Context, m MessageSystem) *Manager {
	return &Manager{
		aborted:  aborted,
		replicas: []replica{},
		m:        m,
	}
}
