#pragma once

#include <cstdint>

#include "segment.hpp"

// Liveness via open file description (OFD) locks on single bytes of the segment file. An OFD lock belongs to the open
// file description it was taken through, not to a process or thread; locks through different descriptions conflict
// (even within one process), locks through the same one don't, and the kernel releases them when the description's
// last fd is closed, which happens however its process exits. Testing a lock therefore tells whether its owner is
// alive, across pid namespaces.
namespace redline::shm {
// Opens a new description of the segment for holding leases. Reopening through /proc also works after the segment
// has been unlinked. Returns -1, having logged why, on failure.
int open_lease_description(const SegmentMapping& segment) noexcept;

// Write-locks the byte at `offset` through `fd`. Fails if another description holds it.
bool try_lock_byte(int fd, uint64_t offset) noexcept;
void unlock_byte(int fd, uint64_t offset) noexcept;

// True if a description other than the segment's own holds the byte at `offset`. Probes go through the segment's fd,
// so callers must hold segment.probe_mutex(): two threads probing through it at once could both believe they hold it.
bool held_elsewhere(const SegmentMapping& segment, uint64_t offset) noexcept;
}
