// Child process used by the cross-process tests:
//   helper publish <bus> <bus_size> <msg_size> <count>      publish <count> self-validating messages (RELIABLE)
//   helper subscribe-and-crash <bus> <bus_size> <msg_size>  register a subscriber, then die from SIGKILL
//   helper claim-and-crash <bus> <bus_size> <msg_size>      claim a slot, write half a message, then die from SIGKILL
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "redline/bus.hpp"
#include "test_helpers.hpp"

using namespace redline;

int main(int argc, char** argv) {
  if (argc < 5) {
    std::fprintf(stderr, "usage: %s <mode> <bus> <bus_size> <msg_size> [count]\n", argv[0]);
    return 2;
  }
  const std::string mode = argv[1];
  const std::string name = argv[2];

  Config config;
  config.backpressure_mode = BackPressureMode::RELIABLE;
  config.bus_size = static_cast<uint32_t>(std::stoul(argv[3]));
  config.max_message_size = static_cast<uint32_t>(std::stoul(argv[4]));
  auto bus = create_bus(name, config);

  if (mode == "publish" && argc >= 6) {
    const uint64_t count = std::stoull(argv[5]);
    for (uint64_t i = 0; i < count; ++i) {
      const auto payload = test::make_payload(99, i, 16 + uint32_t(i % (config.max_message_size - 16)));
      if (bus->publish(payload, 5'000'000) != ErrorCode::OK) {
        std::fprintf(stderr, "publish %llu failed\n", static_cast<unsigned long long>(i));
        return 1;
      }
    }
    return 0;
  }

  if (mode == "claim-and-crash") {
    Claim claim;
    if (bus->claim(claim) != ErrorCode::OK) return 1;
    std::memset(claim.data().data(), 0x5A, claim.data().size() / 2);
    std::raise(SIGKILL);
  }

  if (mode == "subscribe-and-crash") {
    if (bus->add_subscriber([](const Message&) {}) != ErrorCode::OK) return 1;
    std::raise(SIGKILL);
  }

  return 2;
}
