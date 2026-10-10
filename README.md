# Redline

[![CI](https://github.com/SSutherlandDeeBristol/redline-ipc-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/SSutherlandDeeBristol/redline-ipc-bus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)
![Platform: Linux](https://img.shields.io/badge/platform-Linux-lightgrey.svg)

Shared-memory publish/subscribe for processes on the same Linux host.

A bus is a ring buffer in `/dev/shm`. Publishers write each message into it once, and every subscriber reads it from
there with its own position in the ring. There is no broker or daemon: processes coordinate through atomics in the
shared segment.

## Features

- Low latency: about 1.1 µs for a ping-pong round trip. Publishing makes no system calls unless a subscriber is asleep.
- Each subscriber chooses reliable or lossy delivery. Lossy subscribers never slow publishers down.
- Zero-copy in both directions: build messages in place with `claim()`, read them in place with `zero_copy_reads`.
- Subscribers in crashed processes are cleaned up automatically, including in other containers sharing `/dev/shm`.
- Lag, drop counts and latency percentiles for every subscriber.
- Callbacks on a background thread, or a `Subscription` polled from your own loop.
- No runtime dependencies.

## Quick start

```cpp
// subscriber.cpp
#include <redline/bus.hpp>
#include <chrono>
#include <iostream>
#include <thread>

int main() {
  auto bus = redline::create_bus("example", redline::Config{});
  bus->add_subscriber([](const redline::Message& m) {
    std::cout << m.sequence << ": " << m.data_as_string() << std::endl;
  });
  std::this_thread::sleep_for(std::chrono::hours(1));
}
```

```cpp
// publisher.cpp
#include <redline/bus.hpp>

int main() {
  auto bus = redline::create_bus("example", redline::Config{});
  bus->publish("hello");
}
```

Start the subscriber first: subscribers only receive messages published after they join. Runnable versions are in
[`examples/`](examples) and build as `redline_subscriber` and `redline_publisher`.

A bus outlives the processes using it. Remove it with `redline::destroy_bus("example")`.

## Installation

Requires Linux 3.15 or later, CMake 3.22 and a C++20 compiler. CI uses GCC 13.

With CMake's `FetchContent`:

```cmake
include(FetchContent)
FetchContent_Declare(redline
  GIT_REPOSITORY https://github.com/SSutherlandDeeBristol/redline-ipc-bus.git
  GIT_TAG main)  # pin a commit for reproducible builds
FetchContent_MakeAvailable(redline)

target_link_libraries(your_app PRIVATE redline::redline)
```

Or install it and use `find_package`:

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
cmake --install build --prefix /usr/local
```

```cmake
find_package(redline CONFIG REQUIRED)
target_link_libraries(your_app PRIVATE redline::redline)
```

## Documentation

- [User guide](docs/guide.md): configuration, publishing, the ways to subscribe, zero-copy, metrics, logging.
- [Delivery semantics](docs/semantics.md): reliable and lossy delivery, ordering, crash recovery.
- [Design](docs/design.md): segment layout, the slot seqlock, reservation, wakeups and liveness.

## Performance

AMD Ryzen 5 3600 (6 cores), Linux 6.8, GCC 13.3, Release build:

| | |
|---|---|
| Publish, no subscribers, 16 B | 37 ns |
| One publisher and subscriber, 16 B | 4.0M msg/s |
| Same, `publish_batch` of 16 | 22M msg/s |
| 4 to 8 publishers, one subscriber, 64 B | 12M to 20M msg/s, varying between runs |
| Round trip, 16 B | 1.1 µs |
| Round trip, 4 KiB | 2.4 µs (1.4 µs with zero-copy reads) |
| Publish to delivery, 4 KiB, claim and zero-copy reads | 260 ns p50, 510 ns p99 |

The round trips run between two threads through two buses, pinned to four cores. The benchmarks are in
[`benchmarks/`](benchmarks); see [CONTRIBUTING.md](CONTRIBUTING.md#benchmarks) to run them.

## Alternatives

- [Aeron](https://github.com/real-logic/aeron) also works over the network, and can archive and replay streams.
- [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) supports more operating systems and languages, and request/response.
- [ZeroMQ](https://zeromq.org) runs the same code over IPC or TCP, at the cost of a kernel copy per message.

Redline suits C++ services on one Linux host that fan messages out to several readers and want a small library with
no moving parts.

## Status

Redline is young and its API may change before 1.0. The shared-memory layout is versioned, and a process refuses to
open a bus created by an incompatible version. A publisher that crashes in the middle of writing a message stalls the
bus until it is recreated; see [semantics](docs/semantics.md#guarantees).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing under sanitizers, and running benchmarks.

## License

[MIT](LICENSE)
