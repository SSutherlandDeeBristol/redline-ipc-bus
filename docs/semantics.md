# Delivery semantics

## Delivery modes

Each subscriber chooses how it copes with publishers that are faster than it. Subscribers of both kinds can share a
bus, for example a consumer that must see every message alongside monitoring tools that must never slow it down.

| | `RELIABLE` | `BEST_EFFORT` |
|---|---|---|
| When it is a full ring behind | publishers wait (up to their timeout, then `BACKPRESSURE_TIMEOUT`) | publishers overwrite the oldest message |
| If it is slow | every publisher slows down | it skips ahead (see below) |
| Messages it can lose | none, unless evicted | any it was lapped on |

Publishers only ever wait for `RELIABLE` subscribers. With none on the bus, `publish` never blocks.

The mode comes from `BusOptions::backpressure_mode`. `own_thread` callbacks and `subscribe()` can override it with
`SubscriberOptions::backpressure_mode`.

A `BEST_EFFORT` subscriber that has been lapped resumes according to `on_lapped`:

- `RESUME_AT_OLDEST` (the default) resumes at the oldest message still in the ring, so it drops as few messages as
  possible. Under sustained overload it stays a full ring behind, so every message it reads is as old as it can be.
- `RESUME_AT_NEWEST` resumes at the newest message, dropping everything before it. This suits consumers that only
  need the latest value, such as a display: each time it falls behind, it catches up completely.

Either way, the messages skipped are counted in `dropped_messages`.

### Eviction

A `RELIABLE` subscriber that stops reading blocks publishers indefinitely. To bound that, set
`BusOptions::subscriber_timeout_us` in the publishing processes: a subscriber that is a full ring behind and hasn't made
progress for that long is evicted. When it resumes it skips to the newest message and counts what it missed as
dropped. Subscribers using zero-copy reads are never evicted, since publishers would then overwrite the message they
are reading.

## Guarantees

These hold in both modes.

- **No torn messages.** Every slot carries a version number that publishers update before and after writing.
  Subscribers copy a message out and check the version again; a message overwritten during the copy is discarded and
  counted as dropped.
- **Order.** Messages arrive in sequence-number order. Every sequence number is either delivered, counted in
  `dropped_messages` (including messages lost with a crashed publisher), or belongs to an aborted claim.
- **Start position.** A subscriber receives messages published after it joined, and nothing earlier.
- **Crashed processes.** A subscriber whose process exits, however it exits, stops counting towards backpressure and
  its registry entry is reclaimed. Each subscriber holds an `F_OFD_SETLK` lock on one byte of the segment, which the
  kernel releases when the process dies; other processes test the lock. This works across pid namespaces, provided
  the processes share `/dev/shm`. A child forked (without `exec`) from a subscriber keeps the lock, and so the
  registration, until it exits.
- **Crashed publishers.** A publisher that dies between reserving a slot and committing it (including with a claim
  open) leaves that sequence unfinished, and subscribers and later publishers wait at it. After a millisecond (up to
  50 ms for a subscriber that is asleep and gets no other messages) they check whether its publisher is still alive,
  using the same kind of lock as subscribers, and if it isn't, they finish the sequence on its behalf. Subscribers count the message as dropped. A publisher that is merely slow, or a
  claim held open for a long time, is never interfered with.
- **Timestamps.** `Message::publish_timestamp_ns` is `CLOCK_MONOTONIC`, comparable across processes on the host with
  `redline::now_ns()`. `publish` stamps the message before writing it; a claim is stamped when it is committed.

## Lifetimes

- `Message::data` is valid until the callback returns. Copy it to keep it.
- Anything a callback captures must outlive the `Bus` it was added to.
- A `Subscription` may outlive the `Bus` that created it. A `Claim` must be committed or aborted before its `Bus` is
  destroyed.
