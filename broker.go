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
// Each connection owns an independent byte watermark and sequence counter;
// cancelling one connection never stalls or drops another connection's frames.
type Broker struct {
	mu          sync.Mutex
	maxBytes    int
	maxFrame    int
	buffered    int
	conns       map[string]*connState
	frames      []Frame
	failPublish error
}

type connState struct {
	closed   bool
	nextSeq  uint64
	buffered int
}

func New(maxBytes, maxFrame int) *Broker {
	return &Broker{maxBytes: maxBytes, maxFrame: maxFrame, conns: make(map[string]*connState)}
}

func (b *Broker) state(conn string) *connState {
	c, ok := b.conns[conn]
	if !ok {
		c = &connState{}
		b.conns[conn] = c
	}
	return c
}

func (b *Broker) Attach(conn string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.state(conn)
	if c.closed {
		*c = connState{}
	}
}

func (b *Broker) Cancel(conn string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.conns[conn]
	if !ok {
		b.conns[conn] = &connState{closed: true}
		return
	}
	c.closed = true
	kept := b.frames[:0]
	for _, f := range b.frames {
		if f.Conn == conn {
			b.buffered -= len(f.Data)
			c.buffered -= len(f.Data)
			continue
		}
		kept = append(kept, f)
	}
	for i := len(kept); i < len(b.frames); i++ {
		b.frames[i] = Frame{}
	}
	b.frames = kept
}

func (b *Broker) InjectPublishFailure(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failPublish = err
}

func (b *Broker) Publish(conn string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.state(conn)
	if c.closed {
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
	if c.buffered+len(data) > b.maxBytes {
		return ErrFull
	}
	c.nextSeq++
	cp := append([]byte(nil), data...)
	b.frames = append(b.frames, Frame{Seq: c.nextSeq, Conn: conn, Data: cp})
	b.buffered += len(cp)
	c.buffered += len(cp)
	return nil
}

func (b *Broker) Receive(conn string) (Frame, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.conns[conn]
	if ok && c.closed {
		return Frame{}, false
	}
	for i, f := range b.frames {
		if f.Conn != conn {
			continue
		}
		b.buffered -= len(f.Data)
		if c != nil {
			c.buffered -= len(f.Data)
		}
		b.frames = append(b.frames[:i], b.frames[i+1:]...)
		return f, true
	}
	return Frame{}, false
}

func (b *Broker) Buffered() int { b.mu.Lock(); defer b.mu.Unlock(); return b.buffered }
