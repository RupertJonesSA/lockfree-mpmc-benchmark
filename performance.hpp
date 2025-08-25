#ifndef PERF_HPP
#define PERF_HPP

#include <chrono>
#include <iostream>

class Timer {
public:
  Timer();
  ~Timer();
  void Stop();

private:
  std::chrono::time_point<std::chrono::high_resolution_clock> startTimepoint_;
};

#endif // PERF_HPP
