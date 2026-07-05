# Performance notes

This page documents the local checks used when changing olcRTC data-plane code.
Keep performance work measurable: add a benchmark for the hot path first, then
change code, then compare `ns/op`, `B/op`, `allocs/op`, and effective throughput.

## Quick benchmarks

UDP client flow lookup and SOCKS5 UDP parsing:

```bash
go test -run '^$' \
  -bench 'Benchmark(UDPFlowIDExistingAtLimit|ParseSocksUDPIPv4)' \
  -benchmem ./internal/client
```

UDP wire encode/decode:

```bash
go test -run '^$' \
  -bench 'Benchmark(EncodeIPv4Packet|DecodeIPv4Packet|DecodeDomainPacket)' \
  -benchmem ./internal/udpwire
```

Crypto hot path:

```bash
go test -run '^$' -bench 'Benchmark(Encrypt|Decrypt)' -benchmem ./internal/crypto
```

## Validation

For UDP datapath changes, run at least:

```bash
go test -count=1 ./internal/client ./internal/server ./internal/udpwire
```

For end-to-end SOCKS5 UDP validation, run the memory-provider test when the
host has enough CPU headroom:

```bash
go test -count=1 -run '^TestClientServerSOCKSUDPOverMemoryVP8Channel$' ./internal/e2e
```

For broader transport changes, prefer the package-local tests first, then the
real-provider matrix from CI on GitHub runners.

## Optimization priorities

1. Avoid per-packet allocations in UDP and framing paths.
2. Keep flow lookup O(1) under high concurrent-flow counts.
3. Keep bounded queues and explicit idle cleanup for UDP flows.
4. Use backpressure instead of unbounded buffering.
5. Tune mobile defaults separately from desktop/server defaults.
6. Treat raw Mbps, latency, jitter, CPU, memory, and reconnect behavior as one
   profile. Higher throughput is not useful if battery or memory pressure breaks
   the mobile client.

## Throughput expectations

Throughput depends on carrier, transport, network path, SFU behavior, CPU, and
mobile power state. Use local numbers only as regression signals. Real user
speed should be measured through the complete path:

```text
TUN or SOCKS client -> olcRTC client -> carrier transport -> olcRTC server -> internet
```

Record the transport, carrier, OS, device, packet-loss condition, and whether
the test used TCP stream traffic or UDP datagrams.

For `vp8channel` UDP datagrams, the writer batches multiple queued datagrams
with the same peer route into one VP8 sample. This avoids limiting datagram
throughput to the SFU video frame rate. Voice and other low-bitrate realtime
UDP are the intended targets. Adaptive video calls can still reduce bitrate
heavily over vp8-over-SFU because RTT and jitter remain much higher than on a
native UDP path, even when packet loss is near zero.
