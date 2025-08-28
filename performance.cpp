#include "./performance.hpp"

Timer::Timer() {
  startTimepoint_ = std::chrono::high_resolution_clock::now();
  startCycles_ = __rdtsc();
}

std::pair<double, uint64_t> Timer::Stop() {
  auto endTimepoint = std::chrono::high_resolution_clock::now();
  auto start =
      std::chrono::time_point_cast<std::chrono::microseconds>(startTimepoint_)
          .time_since_epoch()
          .count();
  auto stop =
      std::chrono::time_point_cast<std::chrono::microseconds>(endTimepoint)
          .time_since_epoch()
          .count();

  auto duration = stop - start;
  uint64_t endCycles = __rdtsc();

  uint64_t totalCycles = endCycles - startCycles_;
  double ms = duration * 0.001;

  return {ms, totalCycles};
}
