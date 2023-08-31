package commitqueue

import "sync"

type CommitQueue struct {
	mux      sync.RWMutex
	queueXID []uint64
}

func New(size uint64) *CommitQueue {
	return &CommitQueue{
		queueXID: make([]uint64, 0, size),
	}
}

func (q *CommitQueue) Enqueue(xid uint64) {
	q.mux.Lock()
	defer q.mux.Unlock()
	q.queueXID = append(q.queueXID, xid)
}

func (q *CommitQueue) Peek() uint64 {
	q.mux.RLock()
	defer q.mux.RUnlock()
	return q.queueXID[0]
}

func (q *CommitQueue) Dequeue() uint64 {
	q.mux.Lock()
	defer q.mux.Unlock()
	top := q.queueXID[0]
	q.queueXID = q.queueXID[1:]
	return top
}

func (q *CommitQueue) IsEmpty() bool {
	q.mux.RLock()
	defer q.mux.RUnlock()
	return len(q.queueXID) == 0
}
