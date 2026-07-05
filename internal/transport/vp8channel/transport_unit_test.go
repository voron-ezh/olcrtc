package vp8channel

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	enginebuiltin "github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

var errVP8UnitBoom = errors.New("boom")

// TestControlEpochTracksDataEpoch guards the issue #95 multi-client invariant:
// the control-plane epoch is derived live from the data epoch as
// localEpoch|controlEpochFlag. This lets the server correlate a client's data
// and control planes by arithmetic (controlEpoch &^ controlEpochFlag ==
// dataEpoch), which is what keys the per-peer control sessions. The two planes
// rotate together on reconnect; the control epoch always carries the high bit
// and always shares the current data epoch's low bits.
func TestControlEpochTracksDataEpoch(t *testing.T) {
	tr := &streamTransport{
		bindingToken: bindingToken("room-95"),
		localEpoch:   randomEpoch(),
	}

	check := func(stage string) {
		data := tr.localEpochValue()
		ctrl := tr.controlEpochValue()
		if ctrl&controlEpochFlag == 0 {
			t.Fatalf("%s: control epoch 0x%08x missing control flag", stage, ctrl)
		}
		if ctrl != data|controlEpochFlag {
			t.Fatalf("%s: control epoch 0x%08x != data 0x%08x | flag", stage, ctrl, data)
		}
		if ctrl&^controlEpochFlag != data {
			t.Fatalf("%s: control epoch does not correlate to data epoch 0x%08x", stage, data)
		}
		hdr := tr.controlEpochHeader()
		_, hdrEpoch, _, ok := parseEpochHeader(hdr[:])
		if !ok {
			t.Fatalf("%s: control epoch header failed to parse", stage)
		}
		if hdrEpoch != ctrl {
			t.Fatalf("%s: control wire epoch 0x%08x != controlEpochValue 0x%08x", stage, hdrEpoch, ctrl)
		}
	}

	check("initial")
	// Both planes rotate together across reconnects.
	for range 5 {
		tr.rotateEpochHeader()
		check("after rotation")
	}
}

func TestWriterCadenceStaysAtFrameInterval(t *testing.T) {
	tr := &streamTransport{
		frameInterval: time.Second / 60,
		batchSize:     64,
	}
	if got := tr.frameInterval; got != time.Second/60 {
		t.Fatalf("frameInterval = %v, want %v", got, time.Second/60)
	}

	tr.batchSize = 1
	if got := tr.frameInterval; got != time.Second/60 {
		t.Fatalf("frameInterval after batch change = %v, want %v", got, time.Second/60)
	}
}

type fakeVideoStream struct {
	connectErr error
	closeErr   error
	canSend    bool
	trackAdded bool
	trackCB    func(*webrtc.TrackRemote, *webrtc.RTPReceiver)
	reconnect  func()
	should     func() bool
	ended      func(string)
	watched    bool
	closed     bool

	reconnects atomic.Int32
}

func (s *fakeVideoStream) Connect(context.Context) error { return s.connectErr }
func (s *fakeVideoStream) Close() error {
	s.closed = true
	return s.closeErr
}
func (s *fakeVideoStream) SetReconnectCallback(cb func())    { s.reconnect = cb }
func (s *fakeVideoStream) SetShouldReconnect(fn func() bool) { s.should = fn }
func (s *fakeVideoStream) SetEndedCallback(cb func(string))  { s.ended = cb }
func (s *fakeVideoStream) WatchConnection(context.Context)   { s.watched = true }
func (s *fakeVideoStream) CanSend() bool                     { return s.canSend }
func (s *fakeVideoStream) SubscriberCanSend() bool           { return s.canSend }
func (s *fakeVideoStream) AddTrack(webrtc.TrackLocal) error  { s.trackAdded = true; return nil }
func (s *fakeVideoStream) Reconnect(string)                  { s.reconnects.Add(1) }
func (s *fakeVideoStream) SetTrackHandler(cb func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	s.trackCB = cb
}

// fakeEngineSession adapts fakeVideoStream so it satisfies engine.Session and
// engine.VideoTrackCapable, the two interfaces the vp8channel transport
// looks up after the carrier-layer collapse.
type fakeEngineSession struct {
	stream  *fakeVideoStream
	noVideo bool
}

func (s *fakeEngineSession) Capabilities() engine.Capabilities {
	if s.noVideo {
		return engine.Capabilities{}
	}
	return engine.Capabilities{VideoTrack: true}
}
func (s *fakeEngineSession) Connect(ctx context.Context) error { return s.stream.Connect(ctx) }
func (s *fakeEngineSession) Send([]byte) error                 { return nil }
func (s *fakeEngineSession) Close() error                      { return s.stream.Close() }
func (s *fakeEngineSession) SetReconnectCallback(cb func(*webrtc.DataChannel)) {
	s.stream.SetReconnectCallback(func() {
		if cb != nil {
			cb(nil)
		}
	})
}
func (s *fakeEngineSession) SetShouldReconnect(fn func() bool) { s.stream.SetShouldReconnect(fn) }
func (s *fakeEngineSession) SetEndedCallback(cb func(string))  { s.stream.SetEndedCallback(cb) }
func (s *fakeEngineSession) WatchConnection(ctx context.Context) {
	s.stream.WatchConnection(ctx)
}
func (s *fakeEngineSession) CanSend() bool                           { return s.stream.CanSend() }
func (s *fakeEngineSession) SubscriberCanSend() bool                 { return s.stream.SubscriberCanSend() }
func (s *fakeEngineSession) GetSendQueue() chan []byte               { return nil }
func (s *fakeEngineSession) GetBufferedAmount() uint64               { return 0 }
func (s *fakeEngineSession) Reconnect(string)                        {}
func (s *fakeEngineSession) AddVideoTrack(t webrtc.TrackLocal) error { return s.stream.AddTrack(t) }
func (s *fakeEngineSession) SetVideoTrackHandler(cb func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	s.stream.SetTrackHandler(cb)
}

//nolint:cyclop // table-driven test naturally has many branches
func TestNewConnectSendCallbacksFeaturesAndClose(t *testing.T) {
	stream := &fakeVideoStream{canSend: true}
	name := "vp8channel-unit-new"
	enginebuiltin.Register(name, func(context.Context, enginebuiltin.Config) (engine.Session, error) {
		return &fakeEngineSession{stream: stream}, nil
	})

	trIface, err := New(context.Background(), transport.Config{
		Carrier:  name,
		DeviceID: "client",
		Options:  Options{FPS: 30, BatchSize: 1},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	tr, ok := trIface.(*streamTransport)
	if !ok {
		t.Fatalf("transport type = %T, want *streamTransport", trIface)
	}
	if !stream.trackAdded || stream.trackCB == nil {
		t.Fatal("New() did not attach track and handler")
	}
	if err := tr.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if tr.kcp == nil || !tr.writerUp.Load() {
		t.Fatal("Connect() should eagerly initialize kcp and writer")
	}
	tr.SetReconnectCallback(func() {})
	tr.SetShouldReconnect(func() bool { return true })
	tr.SetEndedCallback(func(string) {})
	tr.WatchConnection(context.Background())
	if stream.reconnect == nil || stream.should == nil || stream.ended == nil || !stream.watched {
		t.Fatal("callbacks/watch were not forwarded")
	}

	peerEpoch := uint32(0x200)
	firstFrame := make([]byte, epochHdrLen+4)
	copy(firstFrame, vp8Keepalive)
	binary.BigEndian.PutUint32(firstFrame[tokenOff:srcOff], tr.bindingToken)
	binary.BigEndian.PutUint32(firstFrame[srcOff:dstOff], peerEpoch)
	binary.BigEndian.PutUint32(firstFrame[dstOff:crcOff], 0)
	binary.BigEndian.PutUint32(firstFrame[crcOff:epochHdrLen], epochCRC(tr.bindingToken, peerEpoch, 0))
	copy(firstFrame[epochHdrLen:], []byte("data"))
	tr.handleIncomingFrame(firstFrame)
	if tr.kcp == nil {
		t.Fatal("kcp not initialized after first peer frame")
	}

	if !tr.CanSend() {
		t.Fatal("CanSend() = false, want true")
	}
	if features := tr.Features(); !features.Reliable || !features.Ordered ||
		!features.MessageOriented || !features.Datagram || features.MaxPayloadSize == 0 {
		t.Fatalf("Features() = %+v", features)
	}
	if err := tr.Send([]byte("payload")); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	tr.drainOutbound()
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := tr.Send([]byte("closed")); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("Send(closed) error = %v, want %v", err, ErrTransportClosed)
	}
}

func TestDatagramEnqueueBroadcast(t *testing.T) {
	tr := &streamTransport{
		stream:           &fakeVideoStream{canSend: true},
		datagramOutbound: make(chan []byte, 10),
		bindingToken:     bindingToken("client"),
		localEpoch:       0x100,
	}

	if !tr.DatagramCanSend() {
		t.Fatal("DatagramCanSend() = false, want true")
	}
	if err := tr.SendDatagram([]byte("broadcast")); err != nil {
		t.Fatalf("SendDatagram() error = %v", err)
	}
	assertQueuedDatagram(t, tr, 0, "broadcast")
}

func TestDatagramEnqueueLatchedPeer(t *testing.T) {
	tr := &streamTransport{
		stream:           &fakeVideoStream{canSend: true},
		datagramOutbound: make(chan []byte, 10),
		bindingToken:     bindingToken("client"),
		localEpoch:       0x100,
	}
	tr.peerEpoch.Store(0x200)
	if err := tr.SendDatagram([]byte("latched")); err != nil {
		t.Fatalf("SendDatagram(latched) error = %v", err)
	}
	assertQueuedDatagram(t, tr, 0x200, "latched")
}

func TestDatagramEnqueueDirectPeer(t *testing.T) {
	tr := &streamTransport{
		stream:           &fakeVideoStream{canSend: true},
		datagramOutbound: make(chan []byte, 10),
		bindingToken:     bindingToken("client"),
		localEpoch:       0x100,
	}
	if err := tr.SendDatagramTo("00000300", []byte("direct")); err != nil {
		t.Fatalf("SendDatagramTo() error = %v", err)
	}
	assertQueuedDatagram(t, tr, 0x300, "direct")
}

func assertQueuedDatagram(t *testing.T, tr *streamTransport, wantDst uint32, wantPayload string) {
	t.Helper()
	frame := <-tr.datagramOutbound
	token, src, dst, ok := parseEpochHeader(frame)
	if !ok || token != tr.bindingToken || src != tr.localEpoch || dst != wantDst {
		t.Fatalf("datagram header token=0x%x src=0x%x dst=0x%x ok=%v want dst=0x%x",
			token, src, dst, ok, wantDst)
	}
	payload, ok := splitDatagramPayload(frame[epochHdrLen:])
	if !ok || string(payload) != wantPayload {
		t.Fatalf("datagram payload=%q ok=%v want %q", payload, ok, wantPayload)
	}
}

func TestDatagramBackpressureAndClosedState(t *testing.T) {
	tr := &streamTransport{
		stream:           &fakeVideoStream{canSend: true},
		datagramOutbound: make(chan []byte, 10),
		bindingToken:     bindingToken("client"),
		localEpoch:       0x100,
	}
	for len(tr.datagramOutbound) < cap(tr.datagramOutbound)*canSendHighWatermark/100 {
		tr.datagramOutbound <- []byte("queued")
	}
	if tr.DatagramCanSend() {
		t.Fatal("DatagramCanSend() = true at high watermark")
	}
	tr.closed.Store(true)
	if err := tr.SendDatagram([]byte("closed")); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("SendDatagram(closed) error = %v, want %v", err, ErrTransportClosed)
	}
}

func TestHandleIncomingDatagramSinglePeer(t *testing.T) {
	got := make(chan string, 1)
	tr := &streamTransport{
		stream:       &fakeVideoStream{canSend: true},
		bindingToken: bindingToken("client"),
		localEpoch:   0x100,
		onDatagram: func(data []byte) {
			got <- string(data)
		},
	}

	tr.handleIncomingFrame(mkDatagramFrame(tr.bindingToken, 0x200, tr.localEpoch, []byte("udp")))
	select {
	case msg := <-got:
		if msg != "udp" {
			t.Fatalf("datagram = %q, want udp", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("datagram callback was not called")
	}
	if tr.peerEpoch.Load() != 0x200 {
		t.Fatalf("peer epoch = 0x%08x, want 0x200", tr.peerEpoch.Load())
	}

	tr.handleIncomingFrame(mkDatagramFrame(tr.bindingToken, 0x300, 0x999, []byte("foreign-dst")))
	select {
	case msg := <-got:
		t.Fatalf("unexpected datagram for foreign dst: %q", msg)
	default:
	}
}

func TestHandleIncomingDatagramPeerRouting(t *testing.T) {
	got := make(chan string, 1)
	tr := &streamTransport{
		stream:       &fakeVideoStream{canSend: true},
		bindingToken: bindingToken("server"),
		localEpoch:   0x100,
		onPeerData:   func(string, []byte) {},
		onPeerDatagram: func(peerID string, data []byte) {
			got <- peerID + ":" + string(data)
		},
	}

	tr.handleIncomingFrame(mkDatagramFrame(tr.bindingToken, 0x300, tr.localEpoch, []byte("peer-udp")))
	select {
	case msg := <-got:
		if msg != "00000300:peer-udp" {
			t.Fatalf("peer datagram = %q, want 00000300:peer-udp", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("peer datagram callback was not called")
	}
}

func TestWriterDrainsDatagramBeforeReliableData(t *testing.T) {
	tr := &streamTransport{
		outbound:         make(chan []byte, 1),
		datagramOutbound: make(chan []byte, 1),
		batchSize:        1,
	}
	dataHdr := testEpochHdr(1)
	dataFrame := append(dataHdr[:], []byte("kcp")...)
	datagramHdr := testEpochHdr(2)
	datagramFrame := append(datagramHdr[:], datagramMagic[:]...)
	datagramFrame = append(datagramFrame, []byte("udp")...)
	tr.outbound <- dataFrame
	tr.datagramOutbound <- datagramFrame

	var writes [][]byte
	tr.sampleWriter = func(data []byte) bool {
		writes = append(writes, append([]byte(nil), data...))
		return true
	}
	w := &writerState{p: tr}
	if !w.drainDatagram() {
		t.Fatal("drainDatagram() = false, want true")
	}
	w.drainData()
	if len(writes) != 2 {
		t.Fatalf("writes = %d, want 2", len(writes))
	}
	if payload, ok := splitDatagramPayload(writes[0][epochHdrLen:]); !ok || string(payload) != "udp" {
		t.Fatalf("first write datagram payload=%q ok=%v", payload, ok)
	}
	if string(writes[1][epochHdrLen:]) != "kcp" {
		t.Fatalf("second write = %q, want kcp", writes[1][epochHdrLen:])
	}
}

func TestWriterBatchesDatagramsWithSameRoute(t *testing.T) {
	tr := &streamTransport{
		datagramOutbound: make(chan []byte, 2),
		batchSize:        8,
	}
	hdr := testEpochHdr(2)
	first := append(append([]byte(nil), hdr[:]...), datagramMagic[:]...)
	first = append(first, []byte("one")...)
	second := append(append([]byte(nil), hdr[:]...), datagramMagic[:]...)
	second = append(second, []byte("two")...)
	tr.datagramOutbound <- second

	var writes [][]byte
	tr.sampleWriter = func(data []byte) bool {
		writes = append(writes, append([]byte(nil), data...))
		return true
	}
	w := &writerState{p: tr}
	sample := w.batchDatagramSample(first)
	if !w.writeSample(sample) {
		t.Fatal("writeSample(datagram batch) = false, want true")
	}
	if len(writes) != 1 {
		t.Fatalf("writes = %d, want 1", len(writes))
	}
	packets, ok := splitDatagramBatchPayload(writes[0][epochHdrLen:])
	if !ok || len(packets) != 2 {
		t.Fatalf("datagram batch packets=%d ok=%v, want 2/true", len(packets), ok)
	}
	assertDatagramPacketPayload(t, packets[0], "one")
	assertDatagramPacketPayload(t, packets[1], "two")
}

func TestWriterKeepsDifferentDatagramRoutesSeparate(t *testing.T) {
	tr := &streamTransport{
		datagramOutbound: make(chan []byte, 2),
		batchSize:        8,
	}
	first := mkDatagramFrame(bindingToken("server"), 0x200, 0x300, []byte("one"))
	second := mkDatagramFrame(bindingToken("server"), 0x200, 0x400, []byte("two"))
	tr.datagramOutbound <- second

	w := &writerState{p: tr}
	sample := w.batchDatagramSample(first)
	packets, ok := splitDatagramBatchPayload(sample[epochHdrLen:])
	if !ok || len(packets) != 1 {
		t.Fatalf("datagram batch packets=%d ok=%v, want 1/true", len(packets), ok)
	}
	if w.pendingDatagram == nil {
		t.Fatal("pendingDatagram = nil, want second route queued for next tick")
	}
	if !sameEpochHeader(w.pendingDatagram, second) {
		t.Fatal("pendingDatagram route changed")
	}
}

func TestHandleIncomingDatagramBatchPeerRouting(t *testing.T) {
	got := make(chan string, 2)
	tr := &streamTransport{
		stream:       &fakeVideoStream{canSend: true},
		bindingToken: bindingToken("server"),
		localEpoch:   0x100,
		onPeerData:   func(string, []byte) {},
		onPeerDatagram: func(peerID string, data []byte) {
			got <- peerID + ":" + string(data)
		},
	}

	tr.handleIncomingFrame(mkDatagramBatchFrame(tr.bindingToken, 0x300, tr.localEpoch, [][]byte{
		[]byte("one"),
		[]byte("two"),
	}))
	assertStringReceived(t, got, "00000300:one")
	assertStringReceived(t, got, "00000300:two")
}

func TestNewErrorPaths(t *testing.T) {
	enginebuiltin.Register("vp8channel-create-fails", func(context.Context, enginebuiltin.Config) (engine.Session, error) {
		return nil, errVP8UnitBoom
	})
	_, err := New(context.Background(), transport.Config{Carrier: "vp8channel-create-fails"})
	if err == nil || err.Error() != "open engine session: boom" {
		t.Fatalf("New() error = %v", err)
	}

	enginebuiltin.Register("vp8channel-no-video", func(context.Context, enginebuiltin.Config) (engine.Session, error) {
		return &fakeEngineSession{stream: &fakeVideoStream{}, noVideo: true}, nil
	})
	_, err = New(context.Background(), transport.Config{Carrier: "vp8channel-no-video"})
	if !errors.Is(err, ErrVideoTrackUnsupported) {
		t.Fatalf("New() error = %v, want %v", err, ErrVideoTrackUnsupported)
	}
}

func assertDatagramPacketPayload(t *testing.T, packet []byte, want string) {
	t.Helper()
	payload, ok := splitDatagramPayload(packet)
	if !ok || string(payload) != want {
		t.Fatalf("datagram packet payload=%q ok=%v, want %q/true", payload, ok, want)
	}
}

func assertStringReceived(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("received %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("did not receive %q", want)
	}
}

//nolint:cyclop // table-driven test naturally has many branches
func TestEpochHeaderTokenAndOutboundCapacity(t *testing.T) {
	tr := &streamTransport{
		stream:       &fakeVideoStream{canSend: true},
		outbound:     make(chan []byte, 10),
		closeCh:      make(chan struct{}),
		writerDone:   make(chan struct{}),
		bindingToken: bindingToken("client"),
		localEpoch:   0x01020304,
	}

	hdr := tr.epochHeader()
	if !bytes.Equal(hdr[:tokenOff], vp8Keepalive) ||
		binary.BigEndian.Uint32(hdr[tokenOff:srcOff]) != tr.bindingToken ||
		binary.BigEndian.Uint32(hdr[srcOff:dstOff]) != tr.localEpoch ||
		binary.BigEndian.Uint32(hdr[dstOff:crcOff]) != 0 ||
		binary.BigEndian.Uint32(hdr[crcOff:epochHdrLen]) != epochCRC(tr.bindingToken, tr.localEpoch, 0) {
		t.Fatalf("epochHeader() = %x", hdr)
	}
	if bindingToken("") == 0 || randomEpoch() == 0 {
		t.Fatal("bindingToken/randomEpoch returned zero")
	}

	rt, err := startKCP(tr.outbound, nil, tr.epochHeader())
	if err != nil {
		t.Fatalf("startKCP: %v", err)
	}
	defer rt.close()
	tr.kcpMu.Lock()
	tr.kcp = rt
	tr.kcpMu.Unlock()

	for len(tr.outbound) < cap(tr.outbound)*canSendHighWatermark/100 {
		tr.outbound <- []byte("queued")
	}
	if tr.CanSend() {
		t.Fatal("CanSend() = true at high watermark")
	}
	tr.drainOutbound()
	if !tr.CanSend() {
		t.Fatal("CanSend() = false after drain")
	}
	tr.closed.Store(true)
	if tr.CanSend() {
		t.Fatal("CanSend() = true after closed")
	}
}

func TestResetPeerRestartsKCPAndDrainsOutbound(t *testing.T) {
	tr := &streamTransport{
		stream:       &fakeVideoStream{canSend: true},
		outbound:     make(chan []byte, 10),
		closeCh:      make(chan struct{}),
		writerDone:   make(chan struct{}),
		bindingToken: bindingToken("client"),
		localEpoch:   0x01020304,
	}
	defer func() {
		_ = tr.Close()
	}()

	rt, err := startKCP(tr.outbound, nil, tr.epochHeader())
	if err != nil {
		t.Fatalf("startKCP: %v", err)
	}
	tr.kcpMu.Lock()
	tr.kcp = rt
	tr.kcpMu.Unlock()
	tr.outbound <- []byte("stale")
	oldEpoch := tr.localEpoch

	tr.ResetPeer()

	tr.kcpMu.RLock()
	got := tr.kcp
	tr.kcpMu.RUnlock()
	if got == nil || got == rt {
		t.Fatalf("ResetPeer kcp = %p, want fresh non-nil runtime distinct from %p", got, rt)
	}
	if len(tr.outbound) != 0 {
		t.Fatalf("ResetPeer left %d outbound frame(s), want 0", len(tr.outbound))
	}
	if tr.localEpoch == oldEpoch {
		t.Fatalf("ResetPeer localEpoch = %#x, want different epoch", tr.localEpoch)
	}
	select {
	case <-rt.readDone:
	case <-time.After(time.Second):
		t.Fatal("old KCP runtime did not stop")
	}
}

func TestVP8FrameStateAssemblesAndRejectsCorruptFrames(t *testing.T) {
	frame := append(append([]byte(nil), vp8Keepalive...), bytes.Repeat([]byte{0x01}, epochHdrLen-len(vp8Keepalive))...)
	var state vp8FrameState

	got := state.processRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 10, Marker: true},
		Payload: append([]byte{0x10}, frame...),
	})
	if !bytes.Equal(got, frame) {
		t.Fatalf("single-packet frame = %x, want %x", got, frame)
	}

	state = vp8FrameState{}
	if got := state.processRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 20},
		Payload: append([]byte{0x10}, frame[:4]...),
	}); got != nil {
		t.Fatalf("partial frame = %x, want nil", got)
	}
	got = state.processRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 21, Marker: true},
		Payload: append([]byte{0x00}, frame[4:]...),
	})
	if !bytes.Equal(got, frame) {
		t.Fatalf("fragmented frame = %x, want %x", got, frame)
	}

	state = vp8FrameState{}
	_ = state.processRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 30},
		Payload: append([]byte{0x10}, frame[:4]...),
	})
	if got := state.processRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 32, Marker: true},
		Payload: append([]byte{0x00}, frame[4:]...),
	}); got != nil {
		t.Fatalf("frame after sequence gap = %x, want nil", got)
	}

	state = vp8FrameState{}
	if got := state.processRTPPacket(&rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 40, Marker: true},
		Payload: []byte{},
	}); got != nil {
		t.Fatalf("bad vp8 payload = %x, want nil", got)
	}
}

//nolint:cyclop // table-driven test naturally has many branches
func TestHandleIncomingFrameEpochFilteringAndReconnect(t *testing.T) {
	called := 0
	tr := &streamTransport{
		stream:       &fakeVideoStream{canSend: true},
		outbound:     make(chan []byte, 16),
		closeCh:      make(chan struct{}),
		writerDone:   make(chan struct{}),
		bindingToken: bindingToken("client"),
		localEpoch:   0x100,
		onData:       func([]byte) { called++ },
	}
	defer func() {
		_ = tr.Close()
	}()

	mkFrame := func(token, epoch uint32, payload []byte) []byte {
		frame := make([]byte, epochHdrLen+len(payload))
		copy(frame, vp8Keepalive)
		binary.BigEndian.PutUint32(frame[tokenOff:srcOff], token)
		binary.BigEndian.PutUint32(frame[srcOff:dstOff], epoch)
		binary.BigEndian.PutUint32(frame[dstOff:crcOff], 0)
		binary.BigEndian.PutUint32(frame[crcOff:epochHdrLen], epochCRC(token, epoch, 0))
		copy(frame[epochHdrLen:], payload)
		return frame
	}

	tr.handleIncomingFrame(mkFrame(bindingToken("other"), 1, []byte("x")))
	tr.handleIncomingFrame(mkFrame(tr.bindingToken, tr.localEpoch, []byte("self")))
	if tr.peerConfirmed.Load() || called != 0 {
		t.Fatal("filtered frames changed peer state")
	}

	// Keepalive (nil payload) latches peer immediately.
	tr.handleIncomingFrame(mkFrame(tr.bindingToken, 1, nil))
	if !tr.peerConfirmed.Load() {
		t.Fatal("first frame should confirm peer")
	}
	if tr.peerEpoch.Load() != 1 {
		t.Fatalf("peer epoch not stored: got %d want 1", tr.peerEpoch.Load())
	}

	reconnected := false
	tr.SetReconnectCallback(func() { reconnected = true })
	stream, ok := tr.stream.(*fakeVideoStream)
	if !ok {
		t.Fatalf("stream type = %T, want *fakeVideoStream", tr.stream)
	}
	if stream.reconnect == nil {
		t.Fatal("SetReconnectCallback did not install stream callback")
	}
	stream.reconnect()
	if !reconnected || tr.kcp == nil {
		t.Fatalf("stream reconnect did not reset/callback: reconnected=%v kcp=%v", reconnected, tr.kcp)
	}
	reconnected = false
	// After reconnect, peerConfirmed is reset so the next frame re-latches
	// the peer epoch. This allows the server to restart with a new epoch.
	if tr.peerConfirmed.Load() {
		t.Fatal("reconnect should reset peerConfirmed")
	}
	tr.handleIncomingFrame(mkFrame(tr.bindingToken, 2, []byte("new-peer-after-reconnect")))
	if !tr.peerConfirmed.Load() {
		t.Fatal("frame after reconnect should re-latch peer")
	}
	if tr.peerEpoch.Load() != 2 {
		t.Fatalf("peer epoch not re-latched: got %d want 2", tr.peerEpoch.Load())
	}
}

// mkPeerFrame builds a broadcast data-plane frame (dst=0) from epoch on token,
// carrying payload.
func mkPeerFrame(token, epoch uint32, payload []byte) []byte {
	frame := make([]byte, epochHdrLen+len(payload))
	copy(frame, vp8Keepalive)
	binary.BigEndian.PutUint32(frame[tokenOff:srcOff], token)
	binary.BigEndian.PutUint32(frame[srcOff:dstOff], epoch)
	binary.BigEndian.PutUint32(frame[dstOff:crcOff], 0)
	binary.BigEndian.PutUint32(frame[crcOff:epochHdrLen], epochCRC(token, epoch, 0))
	copy(frame[epochHdrLen:], payload)
	return frame
}

func mkDatagramFrame(token, src, dst uint32, payload []byte) []byte {
	hdr := buildEpochHeaderTo(token, src, dst)
	frame := make([]byte, 0, epochHdrLen+len(datagramMagic)+len(payload))
	frame = append(frame, hdr[:]...)
	frame = append(frame, datagramMagic[:]...)
	frame = append(frame, payload...)
	return frame
}

func mkDatagramBatchFrame(token, src, dst uint32, payloads [][]byte) []byte {
	hdr := buildEpochHeaderTo(token, src, dst)
	frame := make([]byte, 0, defaultMaxPayloadSize)
	frame = append(frame, hdr[:]...)
	frame = append(frame, datagramBatchMagic[:]...)
	for _, payload := range payloads {
		packet := append(append([]byte(nil), datagramMagic[:]...), payload...)
		frame = appendBatchPacket(frame, packet)
	}
	return frame
}

// TestPeerRestartRebuildsCarrierAfterGrace guards issue #105: when the latched
// peer goes silent past peerRestartGrace and a frame from a fresh epoch
// arrives, the transport rebuilds the carrier (stream.Reconnect) so the client
// re-handshakes against the restarted server instead of stalling for the full
// control-liveness window.
func TestPeerRestartRebuildsCarrierAfterGrace(t *testing.T) {
	stream := &fakeVideoStream{canSend: true}
	tr := &streamTransport{
		stream:           stream,
		outbound:         make(chan []byte, 16),
		closeCh:          make(chan struct{}),
		writerDone:       make(chan struct{}),
		bindingToken:     bindingToken("client"),
		localEpoch:       0x100,
		peerRestartGrace: 20 * time.Millisecond,
	}
	defer func() { _ = tr.Close() }()

	// Latch the original server epoch.
	tr.handleIncomingFrame(mkPeerFrame(tr.bindingToken, 0x200, []byte("hello")))
	if tr.peerEpoch.Load() != 0x200 {
		t.Fatalf("peer epoch = 0x%08x, want 0x200", tr.peerEpoch.Load())
	}

	// A different epoch inside the grace window must NOT rebuild the carrier.
	tr.handleIncomingFrame(mkPeerFrame(tr.bindingToken, 0x300, []byte("early")))
	time.Sleep(10 * time.Millisecond)
	if got := stream.reconnects.Load(); got != 0 {
		t.Fatalf("carrier rebuilt inside grace window: got %d, want 0", got)
	}

	// After the latched peer has been silent past the grace window, a frame
	// from the new epoch is read as a restart and rebuilds the carrier.
	time.Sleep(15 * time.Millisecond)
	tr.handleIncomingFrame(mkPeerFrame(tr.bindingToken, 0x300, []byte("restart")))
	deadline := time.Now().Add(time.Second)
	for stream.reconnects.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := stream.reconnects.Load(); got != 1 {
		t.Fatalf("carrier rebuilds after grace = %d, want 1", got)
	}
	if !tr.peerRestarting.Load() {
		t.Fatal("peerRestarting flag not set after restart detection")
	}
}

// TestPeerRestartRebuildsOnlyOnce ensures repeated frames from the new epoch do
// not trigger a rebuild storm before the latch is reset.
func TestPeerRestartRebuildsOnlyOnce(t *testing.T) {
	stream := &fakeVideoStream{canSend: true}
	tr := &streamTransport{
		stream:           stream,
		outbound:         make(chan []byte, 16),
		closeCh:          make(chan struct{}),
		writerDone:       make(chan struct{}),
		bindingToken:     bindingToken("client"),
		localEpoch:       0x100,
		peerRestartGrace: 10 * time.Millisecond,
	}
	defer func() { _ = tr.Close() }()

	tr.handleIncomingFrame(mkPeerFrame(tr.bindingToken, 0x200, []byte("hello")))
	time.Sleep(15 * time.Millisecond)
	for range 5 {
		tr.handleIncomingFrame(mkPeerFrame(tr.bindingToken, 0x300, []byte("restart")))
	}
	time.Sleep(50 * time.Millisecond)
	if got := stream.reconnects.Load(); got != 1 {
		t.Fatalf("carrier rebuilt %d times, want exactly 1", got)
	}
}

// TestLivePeerKeepsLatchFresh confirms a peer that keeps sending frames within
// the grace window never trips the restart watchdog, even if a stray frame from
// another epoch shows up (unrelated room participant).
func TestLivePeerKeepsLatchFresh(t *testing.T) {
	stream := &fakeVideoStream{canSend: true}
	tr := &streamTransport{
		stream:           stream,
		outbound:         make(chan []byte, 16),
		closeCh:          make(chan struct{}),
		writerDone:       make(chan struct{}),
		bindingToken:     bindingToken("client"),
		localEpoch:       0x100,
		peerRestartGrace: 40 * time.Millisecond,
	}
	defer func() { _ = tr.Close() }()

	tr.handleIncomingFrame(mkPeerFrame(tr.bindingToken, 0x200, nil))
	// Keep the latched peer alive with frequent keepalives while a foreign
	// epoch repeatedly shows up. The latch stays fresh, so no rebuild fires.
	for range 8 {
		tr.handleIncomingFrame(mkPeerFrame(tr.bindingToken, 0x200, nil))
		tr.handleIncomingFrame(mkPeerFrame(tr.bindingToken, 0x999, []byte("noise")))
		time.Sleep(10 * time.Millisecond)
	}
	if got := stream.reconnects.Load(); got != 0 {
		t.Fatalf("carrier rebuilt %d times for a live peer, want 0", got)
	}
}

func seqList(pkts []*rtp.Packet) []uint16 {
	out := make([]uint16, len(pkts))
	for i, p := range pkts {
		out[i] = p.SequenceNumber
	}
	return out
}

func TestReorderBufferRestoresOrderAndSurvivesLoss(t *testing.T) {
	// In-order packets pass straight through.
	b := newReorderBuffer()
	got := make([]uint16, 0, 3)
	for _, seq := range []uint16{100, 101, 102} {
		got = append(got, seqList(b.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}}))...)
	}
	if !reflect.DeepEqual(got, []uint16{100, 101, 102}) {
		t.Fatalf("in-order drain = %v, want [100 101 102]", got)
	}

	// A reordered packet is held until the gap fills, then both drain in order.
	b = newReorderBuffer()
	if out := b.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 10}}); !reflect.DeepEqual(seqList(out), []uint16{10}) {
		t.Fatalf("first packet = %v, want [10]", seqList(out))
	}
	// 12 arrives before 11: must be buffered, nothing delivered yet.
	if out := b.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 12}}); out != nil {
		t.Fatalf("out-of-order packet drained early = %v, want nil", seqList(out))
	}
	// 11 fills the hole: 11 and 12 drain in order.
	out := b.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 11}})
	if !reflect.DeepEqual(seqList(out), []uint16{11, 12}) {
		t.Fatalf("gap fill drain = %v, want [11 12]", seqList(out))
	}

	// Genuine loss: a full window piles up behind a hole, buffer skips the
	// lost sequence rather than stalling forever.
	b = newReorderBuffer()
	_ = b.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 0}})
	var delivered int
	for i := 2; i <= reorderWindow+2; i++ {
		seq := uint16(i & 0xffff)
		delivered += len(b.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}}))
	}
	if delivered == 0 {
		t.Fatal("buffer stalled on lost packet: nothing delivered after window overflow")
	}

	// Stale packets older than the current position are dropped.
	b = newReorderBuffer()
	_ = b.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 50}})
	if out := b.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 49}}); out != nil {
		t.Fatalf("stale packet delivered = %v, want nil", seqList(out))
	}
}

func TestSeqLessWrapAround(t *testing.T) {
	cases := []struct {
		a, b uint16
		want bool
	}{
		{1, 2, true},
		{2, 1, false},
		{65535, 0, true},  // wrap: 65535 precedes 0
		{0, 65535, false}, // wrap: 0 follows 65535
		{10, 10, false},
	}
	for _, c := range cases {
		if got := seqLess(c.a, c.b); got != c.want {
			t.Fatalf("seqLess(%d, %d) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
