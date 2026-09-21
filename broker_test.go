package stream

import (
	"fmt"
	"sync"
	"testing"
)

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

func TestPerConnectionWatermark(t *testing.T) {
	b := New(5, 5)
	b.Attach("slow")
	b.Attach("fast")
	if err := b.Publish("slow", []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("slow", []byte("xx")); err != ErrFull {
		t.Fatalf("expected slow conn full, got %v", err)
	}
	if err := b.Publish("fast", []byte("xx")); err != nil {
		t.Fatalf("fast conn blocked by slow conn: %v", err)
	}
	f, ok := b.Receive("fast")
	if !ok || string(f.Data) != "xx" || f.Seq != 1 {
		t.Fatalf("got %#v %v", f, ok)
	}
}

func TestCancelOnlyDropsThatConnection(t *testing.T) {
	b := New(5, 5)
	b.Attach("slow")
	b.Attach("fast")
	if err := b.Publish("slow", []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("fast", []byte("aa")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("fast", []byte("bb")); err != nil {
		t.Fatal(err)
	}
	b.Cancel("slow")
	if b.Buffered() != 4 {
		t.Fatalf("cancel released other conn's bytes: %d", b.Buffered())
	}
	for i, want := range []string{"aa", "bb"} {
		f, ok := b.Receive("fast")
		if !ok || string(f.Data) != want || f.Seq != uint64(i+1) {
			t.Fatalf("frame %d: got %#v %v", i, f, ok)
		}
	}
	if err := b.Publish("slow", []byte("x")); err != ErrClosed {
		t.Fatalf("expected closed, got %v", err)
	}
}

func TestSeqDoesNotJumpAfterCancel(t *testing.T) {
	b := New(16, 4)
	b.Attach("a")
	b.Attach("c")
	for _, data := range []string{"a1", "a2"} {
		if err := b.Publish("a", []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	b.Cancel("a")
	b.Attach("a")
	if err := b.Publish("a", []byte("a3")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("c", []byte("c1")); err != nil {
		t.Fatal(err)
	}
	fa, _ := b.Receive("a")
	if fa.Seq != 3 || string(fa.Data) != "a3" {
		t.Fatalf("conn a seq jumped: %#v", fa)
	}
	fc, _ := b.Receive("c")
	if fc.Seq != 1 || string(fc.Data) != "c1" {
		t.Fatalf("conn c seq disturbed: %#v", fc)
	}
}

func TestRejectOverflowWithoutDroppingBuffered(t *testing.T) {
	b := New(6, 6)
	b.Attach("a")
	if err := b.Publish("a", []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("a", []byte("567")); err != ErrFull {
		t.Fatalf("expected full, got %v", err)
	}
	if b.Buffered() != 4 {
		t.Fatalf("buffered data lost: %d", b.Buffered())
	}
	f, ok := b.Receive("a")
	if !ok || string(f.Data) != "1234" {
		t.Fatalf("got %#v %v", f, ok)
	}
	if err := b.Publish("a", []byte("567")); err != nil {
		t.Fatalf("still full after drain: %v", err)
	}
}

func TestConcurrentPublishKeepsFIFOOrder(t *testing.T) {
	const writers = 8
	const perWriter = 16
	b := New(writers*perWriter*4, 4)
	b.Attach("a")
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := b.Publish("a", []byte(fmt.Sprintf("%d%03d", w, i))); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	var prev uint64
	for i := 0; i < writers*perWriter; i++ {
		f, ok := b.Receive("a")
		if !ok {
			t.Fatalf("missing frame %d", i)
		}
		if f.Seq <= prev {
			t.Fatalf("out of order: seq %d after %d", f.Seq, prev)
		}
		prev = f.Seq
	}
	if prev != writers*perWriter {
		t.Fatalf("seq gap: last seq %d", prev)
	}
	if _, ok := b.Receive("a"); ok {
		t.Fatal("extra frame leaked")
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
