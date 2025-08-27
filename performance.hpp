#ifndef PERF_HPP
#define PERF_HPP

#include <chrono>

struct BufferTestStats {
  int64_t consumerAttempts;
  int64_t producerAttempts;
  bool allOperationsComplete;
  double time_ms;
};

class Timer {
private:
  std::chrono::time_point<std::chrono::high_resolution_clock> startTimepoint_;

public:
  Timer();
  ~Timer() = default;
  double Stop();
};

#endif // PERF_HPP
