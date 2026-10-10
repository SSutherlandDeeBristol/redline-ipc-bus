# Contributing

## Building

Tests and benchmarks need GoogleTest and Google Benchmark, which come from [vcpkg](https://github.com/microsoft/vcpkg)
at the baseline pinned in `vcpkg.json`.

```sh
export VCPKG_ROOT=~/vcpkg
cmake --preset debug          # or release, asan, tsan
cmake --build --preset debug -j8
ctest --preset debug
```

Build one preset at a time with a bounded `-j`: the sanitizer builds are heavy, and several at once can exhaust
memory on a desktop.

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
cmake --preset release && cmake --build --preset release -j8
./build/release/redline_bench_ipc --cpus 1,2
./build/release/redline_bench_throughput
./build/release/redline_bench_mp
taskset -c 0-3 ./build/release/redline_bench_latency
```

`redline_bench_ipc` measures latency and throughput between two processes. The others use threads in one process.
Results depend heavily on which cores are used, especially on CPUs with several core complexes or sockets, so pin
the benchmarks when comparing runs.

## Changes

- Changing anything in `src/shm/layout.hpp` means bumping `VERSION` there.
- Tests that start background delivery must declare the state their callbacks use before the `Bus`, so it is
  destroyed after the poll thread stops.
- CI builds every preset with warnings as errors, on x86-64 and arm64, and runs the tests on each push and pull
  request. ARM reorders memory operations that x86 doesn't, so it is where ordering bugs show up.
