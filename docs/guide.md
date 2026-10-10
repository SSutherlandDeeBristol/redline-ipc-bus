# User guide

- [Creating a bus](#creating-a-bus)
- [Publishing](#publishing)
- [Subscribing](#subscribing)
- [Zero-copy](#zero-copy)
- [Metrics](#metrics)
- [Logging](#logging)
- [Errors](#errors)

Everything is in namespace `redline`. Include `<redline/bus.hpp>`.

## Creating a bus

```cpp
redline::BusLayout layout;
layout.max_message_size = 1024;
layout.bus_size = 4096;

auto bus = redline::create_bus("telemetry", layout);
if (!bus) {
  std::cerr << redline::to_string(bus.error()) << std::endl;
}
```

`create_bus` creates the shared memory segment `/dev/shm/redline.<name>`, or opens it if it already exists, in which
case it must have the same layout (otherwise `INCOMPATIBLE`). `open_bus(name)` opens an existing bus whatever its
layout, and fails with `NOT_FOUND` if there is none; `Bus::layout()` then says what it is.

The `BusLayout` is fixed when the bus is created:

| Field | Default | |
|---|---|---|
| `max_message_size` | 4096 | Largest message in bytes. Each slot takes this plus 32 bytes, rounded up to 64. |
| `bus_size` | 1024 | Number of slots. Must be a power of two. |
| `max_subscribers` | 256 | Subscriber registry size, up to 65536, shared by all processes. |
| `max_publishers` | 256 | Publisher registry size, up to 65536. Each thread that has published holds an entry until it exits or its `Bus` is destroyed, and each open claim holds one. |

`BusOptions`, the optional third argument, only affect the `Bus` object they are passed to, so processes on one bus
can use different options:

| Field | Default | |
|---|---|---|
| `backpressure_mode` | `RELIABLE` | Default delivery mode for this `Bus`' subscribers. See [semantics](semantics.md). |
| `on_lapped` | `RESUME_AT_OLDEST` | Default for where lapped `BEST_EFFORT` subscribers resume. See [semantics](semantics.md#delivery-modes). |
| `zero_copy_reads` | `false` | Default for this `Bus`' subscribers. `RELIABLE` only. |
| `latency_metrics` | `true` | Default for this `Bus`' subscribers. See [metrics](#metrics). |
| `latency_histogram_significant_figures` | 3 | Precision of the latency percentiles, 1 to 5. |
| `consumer_busy_spin_duration_us` | 100 | How long an idle subscriber spins before sleeping. |
| `subscriber_timeout_us` | 0 | Evict `RELIABLE` subscribers that block this `Bus`' publishers for this long. 0 disables. |
| `poll_thread_cpu` | none | Pin the thread running shared-thread callbacks to this CPU. |
| `prefault` | `false` | Fault the whole segment in when mapping it, so first use of each page doesn't. |
| `lock_memory` | `false` | `mlock` the segment so it is never swapped out. Fails with `SYSTEM_ERROR` beyond `RLIMIT_MEMLOCK`. |
| `huge_pages` | `false` | Ask for transparent huge pages. Only takes effect if `/sys/kernel/mm/transparent_hugepage/shmem_enabled` is `advise`, `within_size` or `always`. |
| `shm_permissions` | `0600` | Mode of the segment, if this process creates it. |
| `init_timeout_ms` | 1000 | How long to wait for another process that is still creating the segment. |

A bus outlives the processes that use it. Remove it with `redline::destroy_bus(name)`; processes that have it open
keep working, and the next `create_bus` starts a fresh one. Changing the layout means destroying the bus first.

## Publishing

```cpp
bus->publish("a string");
bus->publish(std::span<const uint8_t>(bytes, size));
bus->publish(data, /*timeout_us=*/0);
```

`publish` is thread-safe. It waits only while a `RELIABLE` subscriber still has to read the message in the slot it
needs: up to `timeout_us` microseconds (default -1, forever), after which it fails with `BACKPRESSURE_TIMEOUT`. With
a timeout of 0 it never blocks.

### Batches

`publish_batch` reserves slots for several messages at once. They get consecutive sequence numbers, and the producer
contends with other producers once per batch. All or nothing: if any message is too large, or there is no room for
the whole batch before the timeout, nothing is published. A batch can hold at most `bus_size` messages.

```cpp
std::vector<std::span<const uint8_t>> batch = {first, second, third};
bus->publish_batch(batch);
```

### Claims

`claim` reserves the next slot and returns its memory, so a message can be built directly in shared memory. See
[zero-copy](#zero-copy).

## Subscribing

A subscriber starts at the newest message: anything published before it joined is not delivered. There are three
ways to subscribe.

**Callbacks on the `Bus`' thread.** All callbacks added this way share one background thread and one position on the
bus, and run one after another for each message.

```cpp
bus->add_subscriber([](const redline::Message& m) {
  // m.data is valid until the callback returns
});
```

**A callback with its own thread.** It gets its own position on the bus, so a slow callback can't delay the others.
It can also override the `Bus`' defaults for delivery mode, `on_lapped`, zero-copy reads and latency metrics, and be
pinned to a CPU.

```cpp
bus->add_subscriber(record_to_disk, {.own_thread = true, .backpressure_mode = redline::BackPressureMode::BEST_EFFORT});
bus->add_subscriber(trade, {.own_thread = true, .cpu = 3});
```

**A `Subscription` you poll yourself**, from your own thread or event loop. `poll` never blocks; `wait` blocks until
a message is ready or the timeout passes.

```cpp
auto subscription = bus->subscribe().value();
while (running) {
  if (subscription->wait(std::chrono::milliseconds(10))) {
    subscription->poll([](const redline::Message& m) { handle(m); });
  }
}
```

A `RELIABLE` subscription holds publishers back until it has read each message, so poll it regularly. Destroying it
unsubscribes.

Callbacks run on background threads and should return quickly. Anything they capture must outlive the `Bus` they were
added to. Exceptions thrown by callbacks on background threads are caught and logged; exceptions from a `poll`
callback propagate to the caller.

A subscriber that has nothing to read spins for `consumer_busy_spin_duration_us`, then sleeps until a publisher wakes
it. Waking a sleeping subscriber takes tens of microseconds, so latency-sensitive subscribers should spin for longer
than the usual gap between messages.

Each subscriber that has its own position (the `Bus`' shared thread, each `own_thread` callback, each `Subscription`)
takes an entry in the registry and a file descriptor.

## Zero-copy

By default a message is copied twice: into the slot by `publish`, and out of it before callbacks run. Both copies can
be removed.

**Publishing in place.** `claim` reserves a slot and returns `max_message_size` bytes to write into; `commit` publishes
them.

```cpp
if (auto claim = bus->claim()) {
  size_t size = serialize_into(claim->data());
  claim->commit(size);
}
```

A claim that is destroyed or reused without being committed is aborted, and subscribers skip its sequence number.
Claims can be moved and committed from another thread. Subscribers receive messages in order, so none of them can get
past an open claim: commit promptly, and don't publish more than `bus_size` messages from the thread holding a claim
while `RELIABLE` subscribers are on the bus.

**Reading in place.** With `zero_copy_reads`, callbacks read the slot directly and it is released when they return.
This requires `RELIABLE` mode, since only then are publishers guaranteed not to overwrite the slot. A slow callback
holds publishers up for as long as it runs, and zero-copy subscribers are never evicted.

```cpp
auto subscription = bus->subscribe({.zero_copy_reads = true}).value();
```

Together these suit formats that are read without parsing, such as FlatBuffers, Cap'n Proto or SBE.

## Metrics

`Bus::get_subscriber_metrics()` reports on the callbacks sharing the `Bus`' thread, and `Subscription::get_metrics()`
on a subscription.

| Field | |
|---|---|
| `messages_consumed` | Messages delivered since the subscriber joined |
| `dropped_messages` | Messages it missed after being lapped or evicted, or lost with a crashed publisher |
| `current_lag` | Messages published that it hasn't read yet |
| `max_lag` | Largest lag seen |
| `last_latency_ns` | Publish-to-delivery time of the latest message |
| `p50_latency_ns` ... `p999_latency_ns` | Latency percentiles |

`reset_subscriber_metrics()` / `reset_metrics()` clear the percentiles and `max_lag`, for reporting per interval.

Measuring latency costs a clock read and a histogram update for every message, about 25 ns. Set `latency_metrics` to
`false` to save it; `last_latency_ns` and the percentiles are then always 0, and the other fields are unaffected. This
shortens delivery but doesn't raise throughput: a faster subscriber spends the time it saves polling for the next
message.

## Logging

Redline logs a handful of events: segments created or opened, subscribers evicted or reclaimed, and callbacks
throwing. By default warnings and errors go to stderr.

```cpp
redline::set_log_handler([](redline::LogLevel level, std::string_view message) {
  my_logger.log(to_my_level(level), message);
});
redline::set_log_handler({});                            // discard everything
redline::set_log_handler(redline::default_log_handler);  // back to stderr
```

The handler can be called from any thread.

## Errors

Functions that can fail return `redline::Result<T>`, which is `std::expected<T, redline::Error>`. Redline throws no
exceptions of its own, though `std::bad_alloc` can still escape. `to_string(error)` names an error. Failures with more
to say than the error code, such as which option is invalid or which system call failed, are also
[logged](#logging).

| Error | |
|---|---|
| `BACKPRESSURE_TIMEOUT` | A `RELIABLE` subscriber didn't free the slot within the timeout |
| `MESSAGE_TOO_LARGE` | Larger than `max_message_size` |
| `SUBSCRIBER_REGISTRY_FULL` | All `max_subscribers` entries are in use |
| `PUBLISHER_REGISTRY_FULL` | All `max_publishers` entries are in use |
| `INVALID_ARGUMENT` | Invalid name, layout or options, an empty callback, a batch larger than the bus, or committing a claim that isn't open |
| `NOT_FOUND` | `open_bus` or `destroy_bus` on a bus that doesn't exist |
| `INCOMPATIBLE` | The bus exists with a different layout, or was created by an incompatible version of Redline |
| `SYSTEM_ERROR` | A system call failed, or another process took too long to create the segment |
