package etltool

import "context"

type InStreamSet interface {
	Subscribe(InStream)
	Fetch(context.Context) (any, bool)
	Size() int
}

type InStream interface {
	Fetch(context.Context) (any, bool)
}

type OutStreamSet interface {
	Register(OutStream)
	Publish(any)
	Term()
	Size() int
}

type OutStream interface {
	Publish(any)
	Term()
}
