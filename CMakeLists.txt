cmake_minimum_required(VERSION 3.22)

project(redline
  VERSION 0.1.0
  DESCRIPTION "Shared-memory message bus for same-host IPC"
  LANGUAGES CXX
)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "redline is Linux-only (it relies on POSIX shared memory and futexes)")
endif()

# ----------------------------
# Options
# ----------------------------
option(REDLINE_BUILD_TESTS      "Build unit/integration tests" OFF)
option(REDLINE_BUILD_EXAMPLES   "Build examples" ON)
option(REDLINE_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(REDLINE_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)
set(REDLINE_SANITIZER "" CACHE STRING "Build everything with a sanitizer: address (ASan+UBSan), thread (TSan) or empty for none")
set_property(CACHE REDLINE_SANITIZER PROPERTY STRINGS "" address thread)

# ----------------------------
# C++ standard
# ----------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# ----------------------------
# Sanitizers (applied to every target so the library and executables agree)
# ----------------------------
if(REDLINE_SANITIZER STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(REDLINE_SANITIZER STREQUAL "thread")
  # TSan doesn't model the fences in the seqlock and sleep/wake protocols, and GCC warns about every one of them.
  # The races they order are either benign by design (see tests/tsan.supp) or also ordered by atomics TSan does see.
  add_compile_options(-fsanitize=thread -Wno-tsan)
  add_link_options(-fsanitize=thread)
elseif(NOT REDLINE_SANITIZER STREQUAL "")
  message(FATAL_ERROR "Unknown REDLINE_SANITIZER '${REDLINE_SANITIZER}'")
endif()

# ----------------------------
# Library
# ----------------------------
add_library(redline)
add_library(redline::redline ALIAS redline)

target_include_directories(redline
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

target_sources(redline
  PRIVATE
  src/bus.cpp
  src/cpu.hpp
  src/log.hpp
  src/log.cpp
  src/histogram.hpp
  src/subscriber.cpp
  src/shm/layout.hpp
  src/shm/segment.hpp
  src/shm/segment.cpp
  src/shm/ring_buffer.hpp
  src/shm/ring_buffer.cpp
  src/shm/subscriber_registry.hpp
  src/shm/subscriber_registry.cpp
  src/shm/futex.hpp
  src/shm/futex.cpp
)

target_include_directories(redline
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# ----------------------------
# Warnings
# ----------------------------
function(redline_apply_warnings target)
  target_compile_options(${target} PRIVATE
    -Wall -Wextra -Wpedantic
    -Wshadow -Wconversion -Wsign-conversion
    -Wnon-virtual-dtor
  )
  if(REDLINE_WARNINGS_AS_ERRORS)
    target_compile_options(${target} PRIVATE -Werror)
  endif()
endfunction()

redline_apply_warnings(redline)

# ----------------------------
# Examples
# ----------------------------
if(REDLINE_BUILD_EXAMPLES)
  add_executable(redline_pubsub_basic examples/pubsub_basic.cpp)
  target_link_libraries(redline_pubsub_basic PRIVATE redline)
  redline_apply_warnings(redline_pubsub_basic)
endif()

# ----------------------------
# Tests (GoogleTest via vcpkg)
# ----------------------------
if(REDLINE_BUILD_TESTS)
  include(CTest)
  enable_testing()

  find_package(GTest CONFIG REQUIRED)

  add_executable(redline_tests
    tests/test_helpers.hpp
    tests/test_histogram.cpp
    tests/test_futex.cpp
    tests/test_segment.cpp
    tests/test_ring_buffer.cpp
    tests/test_registry.cpp
    tests/test_bus.cpp
    tests/test_bus_reliable.cpp
    tests/test_bus_best_effort.cpp
    tests/test_subscription.cpp
    tests/test_zero_copy.cpp
    tests/test_publish_batch.cpp
    tests/test_logging.cpp
    tests/test_mixed_modes.cpp
  )
  # Tests exercise internals directly, so they see the private headers too
  target_include_directories(redline_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
  target_link_libraries(redline_tests
    PRIVATE
    redline
    GTest::gtest
    GTest::gtest_main
  )
  redline_apply_warnings(redline_tests)

  # Child process for the cross-process tests
  add_executable(redline_test_helper tests/helper_process.cpp)
  target_link_libraries(redline_test_helper PRIVATE redline GTest::gtest)
  add_dependencies(redline_tests redline_test_helper)
  target_compile_definitions(redline_tests PRIVATE REDLINE_TEST_HELPER="$<TARGET_FILE:redline_test_helper>")

  include(GoogleTest)
  gtest_discover_tests(redline_tests
    PROPERTIES ENVIRONMENT "TSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/tests/tsan.supp halt_on_error=1"
    DISCOVERY_TIMEOUT 30
    # Discover when ctest runs rather than at build time, so sanitizer environments (e.g. `setarch -R` for TSan) apply
    DISCOVERY_MODE PRE_TEST
  )
endif()

# ----------------------------
# Benchmarks (Google Benchmark via vcpkg)
# ----------------------------
if(REDLINE_BUILD_BENCHMARKS)
  find_package(benchmark CONFIG REQUIRED)

  foreach(bench bench_latency bench_throughput bench_mp)
    add_executable(redline_${bench} benchmarks/${bench}.cpp)
    target_link_libraries(redline_${bench} PRIVATE redline benchmark::benchmark)
    redline_apply_warnings(redline_${bench})
  endforeach()
endif()

# ----------------------------
# Install + packaging
# ----------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS redline
  EXPORT redlineTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/redline DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT redlineTargets
  NAMESPACE redline::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redline
)

configure_package_config_file(cmake/redlineConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/redlineConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redline
)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/redlineConfigVersion.cmake
  COMPATIBILITY SameMinorVersion
)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/redlineConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/redlineConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redline
)
