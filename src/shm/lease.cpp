#include "lease.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace redline::shm {
namespace {
bool set_lock(const int fd, const uint64_t offset, const short type) noexcept {
  struct flock lock{};
  lock.l_type = type;
  lock.l_whence = SEEK_SET;
  lock.l_start = off_t(offset);
  lock.l_len = 1;
  return fcntl(fd, F_OFD_SETLK, &lock) == 0;
}
}

int open_lease_description(const SegmentMapping& segment) {
  const std::string path = "/proc/self/fd/" + std::to_string(segment.fd());
  const int fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    throw std::runtime_error("Failed to open a lease on " + segment.name() + ": " + std::strerror(errno));
  }
  return fd;
}

bool try_lock_byte(const int fd, const uint64_t offset) noexcept {
  return set_lock(fd, offset, F_WRLCK);
}

void unlock_byte(const int fd, const uint64_t offset) noexcept {
  set_lock(fd, offset, F_UNLCK);
}

bool held_elsewhere(const SegmentMapping& segment, const uint64_t offset) noexcept {
  if (!try_lock_byte(segment.fd(), offset)) {
    return true;
  }
  unlock_byte(segment.fd(), offset);
  return false;
}
}
