# Design

How a bus works inside. None of this is needed to use Redline, but it explains its guarantees and costs.

## Segment layout

A bus is one shared memory segment, `/dev/shm/redline.<name>`, laid out as:

```
+--------------------+---------------------------+---------------------------+------------------------------------+
| header             | subscriber registry       | publisher registry        | slots                              |
| sizes, write_seq,  | max_subscribers x 128 B   | max_publishers x 64 B     | bus_size x (32 B header + payload, |
| wakeup counters    |                           |                           | rounded up to 64 B)                |
+--------------------+---------------------------+---------------------------+------------------------------------+
```

The header records a magic number, a layout version, the struct sizes and the segment-wide config. A process that
opens the segment checks all of them and refuses a mismatch rather than misreading memory. The creator builds the
segment with `O_EXCL` and publishes it by setting an init flag last; openers wait for that flag, up to
`init_timeout_ms`.

Hot fields that different parties write (`write_seq`, the wakeup counters, each subscriber's position) sit on separate
cache lines.

## Sequences and slots

Every message gets a sequence number from `write_seq`; sequence `s` lives in slot `s & (bus_size - 1)`. Each slot
header holds a version that works as a seqlock:

| Version | Meaning for a reader of `s` |
|---|---|
| below `2s + 1` | not written yet (still holds an earlier lap) |
| `2s + 1` | being written |
| `2s + 2` | committed |
| above `2s + 2` | overwritten by a later lap |

A publisher takes a slot by compare-and-swapping its version to odd, so two publishers a lap apart can never write
the same slot at once. A subscriber copies the message out, then reads the version again; if it changed, the copy is
discarded.

## Publishing

A publisher reserves sequences with a compare-and-swap on `write_seq`, which checks that the slots are free and claims
them in one step. "Free" means every `RELIABLE` subscriber has moved past the message the slot currently holds.
Publishers cache the lowest `RELIABLE` position and only rescan the registry when the ring looks full. While it is
full, one thread per process rescans, at most every 16 spins, so waiting publishers don't keep pulling subscribers'
cache lines away from them.

After writing, a publisher wakes sleeping subscribers, but only if some are asleep. Sleepers increment a `waiters`
counter before checking for messages; publishers check it after committing, with fences on both sides, so either the
publisher sees the sleeper or the sleeper sees the message. In the common case `publish` makes no system calls.

`claim` is the same as `publish` split in two: the slot is reserved and its version made odd, the caller writes the
payload, and `commit` stores the size and the even version. Aborting a claim commits it with a sentinel size that
subscribers skip.

## Subscribing

Each subscriber has a registry entry holding its state, its position (`read_seq`), a heartbeat, flags (reliable,
never evict) and its metrics. It reads sequence `read_seq`, then advances `read_seq`, which is what frees the slot
for `RELIABLE` publishers. In copy mode it advances before running callbacks; with zero-copy reads it advances after
they return.

A lapped `BEST_EFFORT` subscriber jumps to the oldest message still in the ring and adds the gap to
`dropped_messages`. An evicted `RELIABLE` subscriber finds its entry marked `DEAD`, jumps to the newest message, and
marks itself live again.

## Liveness

A registry entry is claimed by taking an open file description lock (`F_OFD_SETLK`) on one byte of the segment file,
through a file descriptor of the owner's own, and then compare-and-swapping the entry from `EMPTY`. The kernel
releases the lock when the last descriptor for it closes, which includes the process dying. Any process can test
whether an entry's lock is held. Subscribers and publishers lock different byte ranges.

Unlike checking pids, this works across pid namespaces. Unlike robust mutexes, the lock belongs to a file description
rather than a thread, so a subscription can be polled from, and a claim committed from, any thread.

Dead subscribers are reclaimed when a publisher is blocked or the subscriber registry is full.

## Recovering from dead publishers

Each publish in progress, and each open claim, holds a publisher registry entry. Before its reservation
compare-and-swap, a publisher writes the range it is about to reserve into its entry, and it clears the record only
after committing everything in it. So any sequence below `write_seq` that isn't committed is covered by its owner's
record.

When a subscriber has waited on a reserved sequence for a millisecond, or a publisher has waited that long for an
older lap's publisher to leave a slot, it runs recovery. Recovery looks at entries whose lock is free (dead
publishers) and, for each sequence in their records, finishes it as lost unless a live publisher's record also covers
it. A sequence covered only by dead records, and already reserved, can only belong to a dead publisher, and nobody can
reserve it again, so finishing it can't interfere with anyone. Only one process recovers at a time, under another
byte lock, because finishing a slot the dead publisher had claimed takes several stores.

Taking a lease and writing the record cost about 10 ns per publish.

## Costs

- Crash safety for publishers adds about 10 ns to each publish.
- One cache line moves between a publisher and a subscriber that has caught up, for every message. A single producer
  with a subscriber that keeps up manages about 4M messages/s; batching amortises this.
- Copy mode copies each message twice. Claims and zero-copy reads remove both copies.
- Fixed-size slots waste space when message sizes vary widely.
