package tail

import (
	"context"
	"sync"
	"sync/atomic"
)

type Tail interface {
	// Append item
	Push(value interface{})
	Get(ctx context.Context, offset uint64, limit uint64) ([]interface{}, uint64)
	// Resize(size uint32)
}

type tail struct {
	sync.RWMutex
	watchers  uint32 // for atomic
	size      uint64
	changed   chan struct{} // close if new item added and if watchers > 0
	next      uint64
	fixedData [2][]interface{}
}

func New(size uint64) Tail {
	t := &tail{
		changed: make(chan struct{}),
		size:    size,
	}

	return t
}

func (t *tail) Push(value interface{}) {
	var oldChanged chan struct{}

	t.Lock()

	i := t.next % t.size

	if i == 0 {
		// recreate data
		t.fixedData = [2][]interface{}{
			make([]interface{}, t.size),
			t.fixedData[0],
		}
	}

	t.fixedData[0][i] = value

	t.next++

	w := atomic.LoadUint32(&t.watchers)
	if w > 0 {
		oldChanged = t.changed
		t.changed = make(chan struct{})
		atomic.StoreUint32(&t.watchers, 0)
	}

	t.Unlock()

	if oldChanged != nil {
		close(oldChanged)
	}
}

func min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func (t *tail) Get(ctx context.Context, offset uint64, limit uint64) ([]interface{}, uint64) {
GetLoop:
	for {
		t.RLock()
		cp := *t
		atomic.AddUint32(&t.watchers, 1)
		t.RUnlock()

		if offset >= cp.next {
			// wait
			select {
			case <-cp.changed:
				continue GetLoop
				// something changed
			case <-ctx.Done():
				// timeout or abort
				return nil, offset
			}
		}

		// first index in cp.fixedData[0]
		s0 := (cp.next - 1) - ((cp.next - 1) % cp.size)

		if offset >= s0 {
			if limit > 0 {
				return cp.fixedData[0][offset%cp.size : min((offset%cp.size)+limit, cp.next%cp.size)], min(offset+limit, cp.next)
			} else {
				return cp.fixedData[0][offset%cp.size : cp.next%cp.size], cp.next
			}
		}

		if offset < (s0 - cp.size) {
			offset = s0 - cp.size
		}

		if limit > 0 {
			return cp.fixedData[1][offset%cp.size : min((offset%cp.size)+limit, cp.size)], min(s0, offset+limit)
		} else {
			return cp.fixedData[1][offset%cp.size:], s0
		}
	}

	return nil, offset
}
