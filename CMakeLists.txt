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
option(REDLINE_BUILD_EXAMPLES   "Build examples" OFF)
option(REDLINE_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(REDLINE_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)
option(REDLINE_ENABLE_SANITIZERS  "Enable ASan/UBSan in Debug" OFF)

# ----------------------------
# C++ standard
# ----------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Default build type if not set (useful for single-config generators)
if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# ----------------------------
# Library
# ----------------------------
add_library(redline)

# Public headers live in include/redline/...
target_include_directories(redline
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Source files — adjust to match your actual tree
target_sources(redline
  PRIVATE
    src/bus.cpp
)

# If some internals are header-only in src/, keep them private via include dirs:
target_include_directories(redline
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Link libs (Linux)
target_link_libraries(redline
  PUBLIC
    # None for now
  PRIVATE
    rt         # shm_open on some systems (glibc often doesn't require it)
    pthread
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
# Sanitizers (Debug only)
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
# Tests (GoogleTest)
# ----------------------------
if(REDLINE_BUILD_TESTS)
  include(CTest)
  enable_testing()

  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  )
  # Avoid overriding parent project's compiler/linker settings on Windows;
  # harmless on Linux.
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  add_executable(redline_tests
    tests/test_registry.cpp
  )
  target_link_libraries(redline_tests PRIVATE redline GTest::gtest_main)
  redline_apply_warnings(redline_tests)

  include(GoogleTest)
  gtest_discover_tests(redline_tests)
endif()

# ----------------------------
# Benchmarks (Google Benchmark)
# ----------------------------
if(REDLINE_BUILD_BENCHMARKS)
  include(FetchContent)
  FetchContent_Declare(
    benchmark
    URL https://github.com/google/benchmark/archive/refs/tags/v1.8.3.zip
  )
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(benchmark)

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
# Install
# ----------------------------
include(GNUInstallDirs)

install(TARGETS redline
  EXPORT redlineTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY include/
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT redlineTargets
  FILE redlineTargets.cmake
  NAMESPACE redline::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redline
)

# ----------------------------
# Packaging config (minimal)
# ----------------------------
include(CMakePackageConfigHelpers)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/redlineConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/redlineConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/redlineConfig.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redline
)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/redlineConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/redlineConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redline
)
