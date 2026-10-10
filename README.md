# Redline

[![CI](https://github.com/SSutherlandDeeBristol/redline-ipc-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/SSutherlandDeeBristol/redline-ipc-bus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)
![Platform: Linux](https://img.shields.io/badge/platform-Linux-lightgrey.svg)

Shared-memory publish/subscribe for processes on the same Linux host.

A bus is a ring buffer in `/dev/shm`. Publishers write each message into it once, and every subscriber reads it from
there with its own position in the ring. There is no broker or daemon: processes coordinate through atomics in the
shared segment.

## Features

- Low latency: a round trip between two processes takes about 0.4 µs. Publishing makes no system calls unless a
  subscriber is asleep.
- Each subscriber chooses reliable or lossy delivery. Lossy subscribers never slow publishers down.
- Zero-copy in both directions: build messages in place with `claim()`, read them in place with `zero_copy_reads`.
- Survives crashed processes: dead subscribers stop holding publishers back, and messages left half-written by dead
  publishers are skipped. This works across containers sharing `/dev/shm`.
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
  auto bus = redline::create_bus("example", {}).value(); // value() throws if it failed
  bus->add_subscriber([](const redline::Message& m) {
    std::cout << m.sequence << ": " << m.data_as_string() << std::endl;
  });
  std::this_thread::sleep_for(std::chrono::hours(1));
}
```

```cpp
// publisher.cpp
#include <redline/bus.hpp>
#include <iostream>

int main() {
  auto bus = redline::create_bus("example", {}).value();
  if (auto published = bus->publish("hello"); !published) {
    std::cerr << redline::to_string(published.error()) << std::endl;
  }
}
```

Functions that can fail return `std::expected`; the library throws no exceptions of its own. Both processes pass the
same `BusLayout` (here the default), which sets the message size and ring size; `open_bus("example")` would instead
take it from the existing bus. Start the subscriber first: subscribers only receive messages published after they join. Runnable versions are in
[`examples/`](examples) and build as `redline_subscriber` and `redline_publisher`.

A bus outlives the processes using it. Remove it with `redline::destroy_bus("example")`.

## Installation

Requires Linux 3.15 or later, CMake 3.22 and a C++23 compiler with `<expected>`. CI uses GCC 13.

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

AMD Ryzen 5 3600 (6 cores), Linux 6.8, GCC 13.3, Release build, each thread or process pinned to its own core:

| | Same core complex | Across core complexes |
|---|---|---|
| Round trip between two processes, 16 B (p50) | 0.36 µs | 0.89 µs |
| Round trip between two processes, 4 KiB (p50) | 0.72 µs | 2.2 µs |
| One process to another, 16 B | 9M to 11M msg/s | 4.8M msg/s |
| One process to another, 4 KiB | 6.5M msg/s (27 GB/s) | 1.4M msg/s |

| | |
|---|---|
| Publish, no subscribers, 16 B | 45 ns |
| One subscriber, `publish_batch` of 16, 64 B | 20M msg/s |
| 4 to 8 publishers, one subscriber, 64 B | 16M to 20M msg/s |

Cache lines travel much further between the 3600's two core complexes, hence the second column. The cross-process
numbers come from `redline_bench_ipc`, with both sides busy-polling and latency metrics off (they add about 25 ns per
message to each subscriber). The rest are threads in one process. The benchmarks are in [`benchmarks/`](benchmarks);
see [CONTRIBUTING.md](CONTRIBUTING.md#benchmarks) to run them.

## Alternatives

- [Aeron](https://github.com/real-logic/aeron) also works over the network, and can archive and replay streams.
- [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) supports more operating systems and languages, and request/response.
- [ZeroMQ](https://zeromq.org) runs the same code over IPC or TCP, at the cost of a kernel copy per message.

Redline suits C++ services on one Linux host that fan messages out to several readers and want a small library with
no moving parts.

## Status

Redline is young and its API may change before 1.0; there are no tagged releases yet. The shared-memory layout is versioned, and a process refuses to
open a bus created by an incompatible version.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for building, testing under sanitizers, and running benchmarks.

## License

[MIT](LICENSE)
