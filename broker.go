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
// The initial implementation intentionally has one shared watermark; the
// feature task asks for independent connection flow control without changing
// these public method names.
type Broker struct {
	mu          sync.Mutex
	maxBytes    int
	maxFrame    int
	buffered    int
	nextSeq     uint64
	closed      map[string]bool
	frames      []Frame
	failPublish error
}

func New(maxBytes, maxFrame int) *Broker {
	return &Broker{maxBytes: maxBytes, maxFrame: maxFrame, closed: make(map[string]bool)}
}

func (b *Broker) Attach(conn string) { b.mu.Lock(); defer b.mu.Unlock(); delete(b.closed, conn) }

func (b *Broker) Cancel(conn string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed[conn] = true
	for i := 0; i < len(b.frames); {
		if b.frames[i].Conn == conn {
			b.buffered -= len(b.frames[i].Data)
			b.frames = append(b.frames[:i], b.frames[i+1:]...)
			continue
		}
		i++
	}
}

func (b *Broker) InjectPublishFailure(err error) { b.mu.Lock(); defer b.mu.Unlock(); b.failPublish = err }

func (b *Broker) Publish(conn string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed[conn] { return ErrClosed }
	if b.failPublish != nil { err := b.failPublish; b.failPublish = nil; return err }
	if len(data) > b.maxFrame { return ErrFrameLarge }
	if b.buffered+len(data) > b.maxBytes { return ErrFull }
	b.nextSeq++
	cp := append([]byte(nil), data...)
	b.frames = append(b.frames, Frame{Seq: b.nextSeq, Conn: conn, Data: cp})
	b.buffered += len(cp)
	return nil
}

func (b *Broker) Receive(conn string) (Frame, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed[conn] { return Frame{}, false }
	for i, f := range b.frames {
		if f.Conn != conn { continue }
		b.buffered -= len(f.Data)
		b.frames = append(b.frames[:i], b.frames[i+1:]...)
		return f, true
	}
	return Frame{}, false
}

func (b *Broker) Buffered() int { b.mu.Lock(); defer b.mu.Unlock(); return b.buffered }
