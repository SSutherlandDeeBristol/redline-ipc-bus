# Redline

Redline is a **high-performance shared-memory message bus** for processes running on the same Linux host.

It is designed for systems that care about **predictable latency**, **bounded memory usage**, and **explicit backpressure**—not infinite buffering or best-effort delivery by accident.

Redline uses POSIX shared memory and lock-free data structures to provide microsecond-level pub/sub messaging between processes, without TCP, brokers, or background daemons.

When consumers fall behind, Redline does not silently degrade:
**slow subscribers are detected, throttled, or kicked**, and overload behavior is always explicit.

### Why Redline?

- 🚀 Shared memory, not sockets
- ⏱ Predictable p99 latency
- 🧱 Bounded buffers by design
- 🛑 Explicit backpressure semantics
- 🧠 Designed for same-host microservices, ML pipelines, and realtime systems

Redline is not a distributed message broker.
It is a **local message fabric**—fast, honest, and close to the metal.

## Usage

```cpp
#include <redline/bus.hpp>

redline::Config config;
config.backpressure_mode = redline::BackPressureMode::RELIABLE;

auto bus = redline::create_bus("telemetry", config);   // creates or opens /dev/shm/redline.telemetry

bus->add_subscriber([](const redline::Message& m) {
  // m.data is only valid during this call; copy it if you need it later
});

std::string_view hello = "hello";
bus->publish({reinterpret_cast<const uint8_t*>(hello.data()), hello.size()});
```

Every process attaching to a bus must agree on `max_message_size`, `bus_size`, `max_subscribers` and
`backpressure_mode`; `create_bus` throws if they don't. A bus outlives the processes using it, so call `redline::destroy_bus(name)` to remove it.

### Ways to subscribe

- **`add_subscriber(callback)`**: all callbacks on a `Bus` share one background thread and one position on the bus,
  so they run one after another for each message.
- **`add_subscriber(callback, {.own_thread = true})`**: the callback gets its own thread and position, so a slow
  callback can't hold up the others on the same `Bus`.
- **`subscribe()`**: returns a `Subscription` you drive yourself, from your own thread or event loop:

```cpp
auto subscription = bus->subscribe();
while (running) {
  if (subscription->wait(std::chrono::milliseconds(10))) {
    subscription->poll([](const redline::Message& m) { /* ... */ });
  }
}
```

Anything a callback captures must outlive the `Bus` (or `Subscription`) it was added to.

### Publishing in batches

`publish_batch` reserves room for several messages at once, so they get consecutive sequence numbers and the
producer contends with other producers once per batch instead of once per message. It is all or nothing.

### Zero-copy reads

By default subscribers copy each message out of shared memory before running callbacks, so a slow callback never
holds a slot. With `config.zero_copy_reads = true` (`RELIABLE` only; each `Bus` chooses for itself) callbacks read the slot in
place and it is released when they return. That saves a copy per message, which matters for large messages, at the
cost of holding publishers up for as long as callbacks run. Zero-copy subscribers are never evicted by
`subscriber_timeout_us`, since evicting one mid-callback would let publishers overwrite what it is reading.

## Delivery semantics

| | `RELIABLE` | `BEST_EFFORT` |
|---|---|---|
| Publisher when the ring is full | waits (up to `timeout_us`, else `BACKPRESSURE_TIMEOUT`) | overwrites the oldest message |
| Slow subscriber | throttles every publisher; optionally evicted after `subscriber_timeout_us` | skips ahead, counts `dropped_messages` |
| Messages lost | never, unless a subscriber is evicted (counted as dropped) | when a subscriber is lapped (counted as dropped) |

In both modes:

- A subscriber never sees a corrupted message. Each slot is a seqlock; messages are copied out and validated before
  the callback runs, and anything overwritten mid-read is discarded and counted as dropped.
- Messages arrive in sequence order; every sequence is either delivered or counted in `dropped_messages`.
- A subscriber starts at the newest message when its first callback is added.
- Subscribers left behind by crashed processes are detected and reclaimed, so they never block `RELIABLE` publishers.
  Each subscriber holds an open file description lock on its registry entry, which the kernel releases however the
  process exits, so this also works between containers with separate pid namespaces (as long as they share `/dev/shm`).
- `publish` is thread-safe, and in `BEST_EFFORT` mode never waits for subscribers. Publishing makes no syscalls unless
  a subscriber is asleep waiting for messages.
- `Message::publish_timestamp_ns` uses `CLOCK_MONOTONIC`, which is shared by every process on the host; compare it
  with `redline::now_ns()`.

## Building

Redline is Linux-only. Dependencies (glog, plus GoogleTest and Google Benchmark for tests and benchmarks) come from
[vcpkg](https://github.com/microsoft/vcpkg):

```sh
export VCPKG_ROOT=~/vcpkg            # wherever vcpkg is cloned and bootstrapped
cmake --preset release               # or debug, asan, tsan
cmake --build --preset release
ctest --preset release
```

Without presets, pass `-DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake` and enable
`-DREDLINE_BUILD_TESTS=ON` / `-DREDLINE_BUILD_BENCHMARKS=ON` as needed. `-DREDLINE_SANITIZER=address|thread` builds
everything with ASan+UBSan or TSan.

CI (`.github/workflows/ci.yml`) builds and tests all four presets with warnings as errors.

ThreadSanitizer needs address space layout randomisation reduced on recent kernels, so run the TSan tests as
`setarch -R ctest --preset tsan`. `tests/tsan.supp` suppresses only the seqlock payload copies, which race by design.

## Benchmarks

`redline_bench_throughput`, `redline_bench_latency` (ping-pong round trip) and `redline_bench_mp` (multiple
producers). Indicative numbers on a 12-core desktop, Release build:

| | |
|---|---|
| Publish, no subscribers, 16 B | ~37 ns |
| Sustained throughput, 1 producer, 1 subscriber, 16 B | ~4.7M msg/s (`BEST_EFFORT`), ~4M msg/s (`RELIABLE`) |
| Same, `publish_batch` of 16 | ~18M msg/s (`RELIABLE`) |
| 4 / 8 producers, 1 subscriber, 64 B, `RELIABLE` | ~16M / ~14M msg/s |
| Round trip, 16 B | ~1.1 µs |
| Round trip, 4 KiB | ~2.3 µs, ~1.8 µs with zero-copy reads |

A single producer with a caught-up subscriber is limited by cache lines bouncing between their cores on every
message; batching amortises that.

## Limits

- `max_subscribers` (default 256, up to 65536) registry entries per bus, shared by all processes. A `Bus` with
  shared-thread callbacks, each `own_thread` callback and each `subscribe()` takes one; there is no limit on how many
  callbacks share a thread. Each also holds a file descriptor for its liveness lock, so count them against
  `ulimit -n`.
- `bus_size` must be a power of two. Each slot takes `max_message_size + 32` bytes, rounded up to a 64-byte cache line.
- Subscriber liveness locks are inherited across `fork()` (not `exec`), so a subscriber's forked child keeps its
  registration alive until the child exits.
