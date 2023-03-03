package etltool

type provider struct {
	consumers []chan<- any
}

func (s *provider) deliver(msg interface{}) {
	for _, r := range s.consumers {
		r <- msg
	}
}

func (s *provider) stop() {
	for _, r := range s.consumers {
		close(r)
	}
}

func (s *provider) addConsumer(ch chan<- any) {
	s.consumers = append(s.consumers, ch)
}
