#pragma once

namespace redline {
// Hint to the CPU that we are in a spin-wait loop. Saves power and frees execution resources for a hyperthread sibling.
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#endif
}
}
