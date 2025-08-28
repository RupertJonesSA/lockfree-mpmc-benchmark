#ifndef PERF_HPP
#define PERF_HPP

#include <chrono>
#include <utility>
#include <x86intrin.h>

struct BufferTestStats {
  uint64_t consumerAttempts;
  uint64_t producerAttempts;
  bool allOperationsComplete;
  double time_ms;
  uint64_t cycles;
};

class Timer {
private:
  std::chrono::time_point<std::chrono::high_resolution_clock> startTimepoint_;
  uint64_t startCycles_{};

public:
  Timer();
  ~Timer() = default;
  std::pair<double, uint64_t> Stop(); // (milliseconds, cycles)
};

#endif // PERF_HPP
