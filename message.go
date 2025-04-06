package etltool

import (
	"context"
	"reflect"
)

type MessageSystem interface {
	NewPipe(subject string) (InStream, OutStream)
	NewInStreamSet() InStreamSet
	NewOutStreamSet() OutStreamSet
}

type ChanMessageSystem struct{}

func (s ChanMessageSystem) NewPipe(string) (InStream, OutStream) {
	ch := make(chan any, 5000)
	return &ChanInStream{instream: ch}, &ChanOutStream{outStream: ch}
}

func (s ChanMessageSystem) NewInStreamSet() InStreamSet {
	return &ChanInStreamSet{instreams: make([]reflect.SelectCase, 0)}
}

func (s ChanMessageSystem) NewOutStreamSet() OutStreamSet {
	return &ChanOutStreamSet{outStreams: []OutStream{}}
}

type ChanOutStream struct {
	outStream chan<- any
}

func (s *ChanOutStream) Publish(msg any) {
	s.outStream <- msg
}

func (s *ChanOutStream) Term() {
	close(s.outStream)
}

type ChanOutStreamSet struct {
	outStreams []OutStream
}

func (s *ChanOutStreamSet) Size() int {
	return len(s.outStreams)
}

func (s *ChanOutStreamSet) Register(d OutStream) {
	s.outStreams = append(s.outStreams, d.(*ChanOutStream))
}

func (s ChanOutStreamSet) Publish(msg any) {
	for _, r := range s.outStreams {
		r.Publish(msg)
	}
}

func (s ChanOutStreamSet) Term() {
	for _, r := range s.outStreams {
		r.Term()
	}
}

type ChanInStream struct {
	instream <-chan any
}

func (s *ChanInStream) Fetch(aborted context.Context) (any, bool) {
	value, ok := <-s.instream
	return value, ok
}

type ChanInStreamSet struct {
	instreams []reflect.SelectCase
}

func (s *ChanInStreamSet) Size() int {
	return len(s.instreams)
}

func (s *ChanInStreamSet) Subscribe(i InStream) {
	chanIn, ok := i.(*ChanInStream)
	if !ok {
		return
	}

	sc := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(chanIn.instream)}
	s.instreams = append(s.instreams, sc)
}

func (s *ChanInStreamSet) Fetch(aborted context.Context) (any, bool) {
	done := aborted.Done()
	sc := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)}

	for len(s.instreams) > 0 {
		idx, value, ok := reflect.Select(append([]reflect.SelectCase{sc}, s.instreams...))
		if idx == 0 {
			return nil, false
		}
		if ok {
			return value.Interface(), ok
		}

		if idx == len(s.instreams) {
			s.instreams = s.instreams[:idx-1]
		} else {
			s.instreams = append(s.instreams[:idx-1], s.instreams[idx:]...)
		}
	}

	return nil, false
}
