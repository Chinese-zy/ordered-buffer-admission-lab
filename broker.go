package stream

import (
	"errors"
	"sync"
)

var (
	ErrFull       = errors.New("stream buffer full")
	ErrClosed     = errors.New("connection closed")
	ErrFrameLarge = errors.New("frame exceeds limit")
)

type Frame struct {
	Seq  uint64
	Conn string
	Data []byte
}

// Broker is a small in-memory receive broker. A caller attaches a connection,
// publishes complete frames, and receives them through the stable Receive API.
// Flow control is per connection: each connection has its own watermark,
// sequence counter, and queue, so one slow connection never stalls the others.
type Broker struct {
	mu          sync.Mutex
	maxBytes    int
	maxFrame    int
	conns       map[string]*connState
	failPublish error
}

type connState struct {
	closed   bool
	buffered int
	nextSeq  uint64
	frames   []Frame
}

func New(maxBytes, maxFrame int) *Broker {
	return &Broker{maxBytes: maxBytes, maxFrame: maxFrame, conns: make(map[string]*connState)}
}

func (b *Broker) Attach(conn string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cs := b.conns[conn]
	if cs == nil {
		cs = &connState{}
		b.conns[conn] = cs
	}
	cs.closed = false
}

func (b *Broker) Cancel(conn string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cs := b.conns[conn]
	if cs == nil {
		cs = &connState{}
		b.conns[conn] = cs
	}
	cs.closed = true
	cs.frames = nil
	cs.buffered = 0
}

func (b *Broker) InjectPublishFailure(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failPublish = err
}

func (b *Broker) Publish(conn string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	cs := b.conns[conn]
	if cs == nil {
		cs = &connState{}
		b.conns[conn] = cs
	}
	if cs.closed {
		return ErrClosed
	}
	if b.failPublish != nil {
		err := b.failPublish
		b.failPublish = nil
		return err
	}
	if len(data) > b.maxFrame {
		return ErrFrameLarge
	}
	if cs.buffered+len(data) > b.maxBytes {
		return ErrFull
	}
	cs.nextSeq++
	cp := append([]byte(nil), data...)
	cs.frames = append(cs.frames, Frame{Seq: cs.nextSeq, Conn: conn, Data: cp})
	cs.buffered += len(cp)
	return nil
}

func (b *Broker) Receive(conn string) (Frame, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cs := b.conns[conn]
	if cs == nil || cs.closed || len(cs.frames) == 0 {
		return Frame{}, false
	}
	f := cs.frames[0]
	cs.frames = cs.frames[1:]
	cs.buffered -= len(f.Data)
	return f, true
}

func (b *Broker) Buffered() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	for _, cs := range b.conns {
		total += cs.buffered
	}
	return total
}
