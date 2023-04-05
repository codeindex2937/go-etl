package etltool

import (
	"context"
	"testing"
	"time"

	"golang.org/x/exp/slices"
)

func TestRecursive(t *testing.T) {
	aborted, abortFn := context.WithCancel(context.Background())
	defer abortFn()

	data := &[]int{}
	m := New(aborted)
	source := NewSource(m, 1, func() []int {
		return []int{0}
	})
	s := NewStageFrom(m, 1, func(v int) []int {
		if v > 2 {
			return nil
		}

		v++
		r := []int{}
		for i := 0; i < v; i++ {
			r = append(r, v)
		}
		*data = append(*data, r...)
		return r
	}, source)
	PipeTo(s, s)

	go func() {
		time.Sleep(time.Second)
		s.stop()
	}()

	m.Start()
	m.Wait()

	if !slices.Equal(*data, []int{1, 2, 2, 3, 3, 3, 3, 3, 3}) {
		t.Errorf("unexpected output: %v", *data)
	}
}
