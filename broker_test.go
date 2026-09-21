package stream

import "testing"

func TestPublishReceive(t *testing.T) {
	b := New(8, 8); b.Attach("a")
	if err := b.Publish("a", []byte("one")); err != nil { t.Fatal(err) }
	f, ok := b.Receive("a")
	if !ok || string(f.Data) != "one" || f.Seq != 1 { t.Fatalf("got %#v %v", f, ok) }
}

func TestGlobalWatermarkAndCancellationBaseline(t *testing.T) {
	b := New(5, 5); b.Attach("slow"); b.Attach("fast")
	if err := b.Publish("slow", []byte("1234")); err != nil { t.Fatal(err) }
	if err := b.Publish("fast", []byte("xx")); err != ErrFull { t.Fatalf("expected shared limit, got %v", err) }
	b.Cancel("slow")
	if b.Buffered() != 0 { t.Fatalf("cancel did not release bytes: %d", b.Buffered()) }
}

func TestOversizeAndInjectedFailure(t *testing.T) {
	b := New(20, 3); b.Attach("a")
	if err := b.Publish("a", []byte("1234")); err != ErrFrameLarge { t.Fatal(err) }
	b.InjectPublishFailure(errInjected)
	if err := b.Publish("a", []byte("x")); err != errInjected { t.Fatal(err) }
}

var errInjected = errorSentinel("injected")
type errorSentinel string
func (e errorSentinel) Error() string { return string(e) }
