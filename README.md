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
