package etltool

import (
	"context"
)

func New(aborted context.Context) *Manager {
	return &Manager{
		aborted:  aborted,
		replicas: []replica{},
	}
}
