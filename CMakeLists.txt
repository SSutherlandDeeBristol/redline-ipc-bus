cmake_minimum_required(VERSION 3.22)

project(redline
  VERSION 0.1.0
  DESCRIPTION "Shared-memory message bus for same-host IPC"
  LANGUAGES CXX
)

# ----------------------------
# Options
# ----------------------------
option(REDLINE_BUILD_TESTS      "Build unit/integration tests" OFF)
option(REDLINE_BUILD_EXAMPLES   "Build examples" ON)
option(REDLINE_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(REDLINE_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)
option(REDLINE_ENABLE_SANITIZERS  "Enable ASan/UBSan in Debug" OFF)

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
# Dependencies (vcpkg)
# ----------------------------
find_package(glog CONFIG REQUIRED)

# ----------------------------
# Library
# ----------------------------
add_library(redline)

target_include_directories(redline
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

target_sources(redline
  PRIVATE
  src/bus.cpp
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

target_link_libraries(redline
  PUBLIC
  glog::glog
)

# ----------------------------
# Warnings
# ----------------------------
function(redline_apply_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4)
    if(REDLINE_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE /WX)
    endif()
  else()
    target_compile_options(${target} PRIVATE
      -Wall -Wextra -Wpedantic
      -Wshadow -Wconversion -Wsign-conversion
      -Wnon-virtual-dtor
    )
    if(REDLINE_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
endfunction()

redline_apply_warnings(redline)

# ----------------------------
# Sanitizers
# ----------------------------
if(REDLINE_ENABLE_SANITIZERS AND (CMAKE_BUILD_TYPE STREQUAL "Debug") AND NOT MSVC)
  target_compile_options(redline PRIVATE -fsanitize=address,undefined)
  target_link_options(redline PRIVATE -fsanitize=address,undefined)
endif()

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
    tests/test_registry.cpp
  )
  target_link_libraries(redline_tests
          PRIVATE
          redline
          GTest::gtest
          GTest::gtest_main
  )
  redline_apply_warnings(redline_tests)

  include(GoogleTest)
  gtest_discover_tests(redline_tests)
endif()

# ----------------------------
# Benchmarks (Google Benchmark via vcpkg)
# ----------------------------
if(REDLINE_BUILD_BENCHMARKS)
  find_package(benchmark CONFIG REQUIRED)

  add_executable(redline_bench_latency benchmarks/bench_latency.cpp)
  target_link_libraries(redline_bench_latency PRIVATE redline benchmark::benchmark)
  redline_apply_warnings(redline_bench_latency)

  add_executable(redline_bench_throughput benchmarks/bench_throughput.cpp)
  target_link_libraries(redline_bench_throughput PRIVATE redline benchmark::benchmark)
  redline_apply_warnings(redline_bench_throughput)

  add_executable(redline_bench_mp benchmarks/bench_mp.cpp)
  target_link_libraries(redline_bench_mp PRIVATE redline benchmark::benchmark)
  redline_apply_warnings(redline_bench_mp)
endif()

# ----------------------------
# Install + packaging
# ----------------------------
# (unchanged)