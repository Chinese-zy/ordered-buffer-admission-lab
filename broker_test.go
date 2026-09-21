package stream

import "testing"

func TestPublishReceive(t *testing.T) {
	b := New(8, 8)
	b.Attach("a")
	if err := b.Publish("a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	f, ok := b.Receive("a")
	if !ok || string(f.Data) != "one" || f.Seq != 1 {
		t.Fatalf("got %#v %v", f, ok)
	}
}

func TestPerConnectionWatermarkAndCancelIsolation(t *testing.T) {
	b := New(5, 5)
	b.Attach("slow")
	b.Attach("fast")
	if err := b.Publish("slow", []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("fast", []byte("xx")); err != nil {
		t.Fatalf("fast connection must keep flowing while slow is full: %v", err)
	}
	if err := b.Publish("slow", []byte("yy")); err != ErrFull {
		t.Fatalf("slow connection should be full, got %v", err)
	}
	f, ok := b.Receive("fast")
	if !ok || string(f.Data) != "xx" || f.Seq != 1 {
		t.Fatalf("fast connection lost its frame: %#v %v", f, ok)
	}
	b.Cancel("slow")
	if b.Buffered() != 0 {
		t.Fatalf("cancel did not release bytes: %d", b.Buffered())
	}
	if err := b.Publish("fast", []byte("zz")); err != nil {
		t.Fatalf("cancelling slow must not cancel fast: %v", err)
	}
}

func TestPerConnectionSeqDoesNotJump(t *testing.T) {
	b := New(20, 20)
	b.Attach("a")
	b.Attach("b")
	for _, p := range []struct {
		conn string
		data string
	}{{"a", "a1"}, {"b", "b1"}, {"b", "b2"}, {"a", "a2"}} {
		if err := b.Publish(p.conn, []byte(p.data)); err != nil {
			t.Fatal(err)
		}
	}
	want := map[string][]string{"a": {"a1", "a2"}, "b": {"b1", "b2"}}
	for conn, frames := range want {
		for i, data := range frames {
			f, ok := b.Receive(conn)
			if !ok {
				t.Fatalf("conn %s: missing frame %d", conn, i+1)
			}
			if f.Seq != uint64(i+1) {
				t.Fatalf("conn %s: seq jumped, want %d got %d", conn, i+1, f.Seq)
			}
			if string(f.Data) != data {
				t.Fatalf("conn %s: FIFO broken, want %q got %q", conn, data, f.Data)
			}
		}
	}
}

func TestCancelKeepsOtherConnectionFrames(t *testing.T) {
	b := New(20, 20)
	b.Attach("a")
	b.Attach("b")
	_ = b.Publish("a", []byte("a1"))
	_ = b.Publish("b", []byte("b1"))
	_ = b.Publish("a", []byte("a2"))
	b.Cancel("a")
	if _, ok := b.Receive("a"); ok {
		t.Fatal("cancelled connection must not receive")
	}
	if err := b.Publish("a", []byte("a3")); err != ErrClosed {
		t.Fatalf("publish after cancel should be ErrClosed, got %v", err)
	}
	f, ok := b.Receive("b")
	if !ok || string(f.Data) != "b1" || f.Seq != 1 {
		t.Fatalf("other connection frame lost on cancel: %#v %v", f, ok)
	}
	b.Attach("a")
	if err := b.Publish("a", []byte("a4")); err != nil {
		t.Fatalf("re-attach must allow publishes again: %v", err)
	}
	if f, ok := b.Receive("a"); !ok || f.Seq != 1 {
		t.Fatalf("fresh attach should restart independent seqs, got %#v %v", f, ok)
	}
}

func TestPartialOverflowRejectsWholeFrame(t *testing.T) {
	b := New(4, 4)
	b.Attach("a")
	b.Attach("b")
	if err := b.Publish("a", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("a", []byte("xy")); err != ErrFull {
		t.Fatalf("frame that would cross the watermark must be rejected, got %v", err)
	}
	if b.Buffered() != 3 {
		t.Fatalf("rejected frame must not consume bytes: %d", b.Buffered())
	}
	if err := b.Publish("b", []byte("zz")); err != nil {
		t.Fatalf("other connection must stay under its own watermark: %v", err)
	}
	f, ok := b.Receive("a")
	if !ok || string(f.Data) != "abc" {
		t.Fatalf("already buffered data must be retained: %#v %v", f, ok)
	}
}

func TestConcurrentPublishPreservesPerConnFIFO(t *testing.T) {
	const n = 200
	b := New(n*2+1024, 4)
	b.Attach("a")
	b.Attach("b")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if err := b.Publish("a", []byte("aaaa")); err != nil {
				t.Errorf("publish a: %v", err)
				return
			}
		}
	}()
	for i := 0; i < n; i++ {
		if err := b.Publish("b", []byte("bb")); err != nil {
			t.Fatalf("publish b: %v", err)
		}
	}
	<-done
	var lastA, lastB uint64
	for {
		f, ok := b.Receive("a")
		if !ok {
			break
		}
		if f.Seq != lastA+1 {
			t.Fatalf("conn a seq gap or reorder: %d after %d", f.Seq, lastA)
		}
		lastA = f.Seq
	}
	for {
		f, ok := b.Receive("b")
		if !ok {
			break
		}
		if f.Seq != lastB+1 {
			t.Fatalf("conn b seq gap or reorder: %d after %d", f.Seq, lastB)
		}
		lastB = f.Seq
	}
	if lastA != n || lastB != n {
		t.Fatalf("lost frames under concurrency: a=%d b=%d", lastA, lastB)
	}
	if b.Buffered() != 0 {
		t.Fatalf("all frames should drain: %d", b.Buffered())
	}
}

func TestOversizeAndInjectedFailure(t *testing.T) {
	b := New(20, 3)
	b.Attach("a")
	if err := b.Publish("a", []byte("1234")); err != ErrFrameLarge {
		t.Fatal(err)
	}
	b.InjectPublishFailure(errInjected)
	if err := b.Publish("a", []byte("x")); err != errInjected {
		t.Fatal(err)
	}
}

var errInjected = errorSentinel("injected")

type errorSentinel string

func (e errorSentinel) Error() string { return string(e) }
