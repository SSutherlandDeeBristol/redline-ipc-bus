# Contributing

## Building

Tests and benchmarks need GoogleTest and Google Benchmark, which come from [vcpkg](https://github.com/microsoft/vcpkg)
at the baseline pinned in `vcpkg.json`.

```sh
export VCPKG_ROOT=~/vcpkg
cmake --preset debug          # or release, asan, tsan
cmake --build --preset debug
ctest --preset debug
```

The presets need CMake 3.25 and build into `build/<preset>`. Without them, pass
`-DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake` and the options you need:

| Option | Default | |
|---|---|---|
| `REDLINE_BUILD_TESTS` | `OFF` | Unit and cross-process tests |
| `REDLINE_BUILD_BENCHMARKS` | `OFF` | `redline_bench_*` |
| `REDLINE_BUILD_EXAMPLES` | on when top-level | `redline_publisher`, `redline_subscriber`, `redline_pubsub_basic` |
| `REDLINE_SANITIZER` | empty | `address` (ASan and UBSan) or `thread` (TSan), applied to everything |
| `REDLINE_WARNINGS_AS_ERRORS` | `OFF` | CI turns this on |

## Sanitizers

Run the test suite under both sanitizers before sending changes to concurrency code:

```sh
ctest --preset asan
setarch -R ctest --preset tsan
```

TSan needs address space randomisation reduced on recent kernels, hence `setarch -R`. `tests/tsan.supp` suppresses
only the seqlock payload copies, which race by design; don't add to it to silence other reports.

## Benchmarks

```sh
cmake --preset release && cmake --build --preset release
./build/release/redline_bench_throughput
./build/release/redline_bench_mp
taskset -c 0-3 ./build/release/redline_bench_latency
```

Round-trip times depend on which cores the threads land on, so pin the latency benchmark when comparing runs.

## Changes

- Changing anything in `src/shm/layout.hpp` means bumping `VERSION` there.
- Tests that start background delivery must declare the state their callbacks use before the `Bus`, so it is
  destroyed after the poll thread stops.
- CI builds every preset with warnings as errors and runs the tests on each push and pull request.
