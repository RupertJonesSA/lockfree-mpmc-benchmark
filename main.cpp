#include "./mpmc_lock_ring.hpp"
#include "./mpmc_ring.hpp"
#include "./performance.hpp"
#include <atomic>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

static constexpr std::size_t MAXN = 1 << 15;
static constexpr std::size_t N_TRIALS = 50;
static const int MAX_THREADS = std::thread::hardware_concurrency();

static std::mutex mx;

alignas(CLS) std::atomic<std::size_t> producer_fails;
alignas(CLS) std::atomic<std::size_t> consumer_fails;

template <typename BUFFER_T, typename PAYLOAD_T>
void producer(int iterations, PAYLOAD_T item,
              std::unique_ptr<BUFFER_T> &buffer) {
  std::size_t local_fail = 0;
  for (int i{}; i < iterations; ++i) {
    while (!buffer->try_enqueue(item)) {
      local_fail++;
    }
  }
  producer_fails.fetch_add(local_fail, std::memory_order_relaxed);
}

template <typename BUFFER_T, typename PAYLOAD_T>
void consumer(int nItems, std::unique_ptr<BUFFER_T> &buffer,
              std::vector<PAYLOAD_T> &output) {
  PAYLOAD_T item;
  std::size_t local_fail = 0;
  for (std::size_t i{}; i < (std::size_t)nItems; ++i) {
    while (!(buffer->try_dequeue(item))) {
      local_fail++;
    }

    {
      std::lock_guard<std::mutex> lk(mx);
      output.push_back(item);
    }
  }

  consumer_fails.fetch_add(local_fail, std::memory_order_relaxed);
}

template <typename BUFFER_T, typename PAYLOAD_T>
BufferTestStats runBufferTest(std::size_t nWrites, std::size_t nThreads) {
  int nP = nThreads >> 1, nC = (nThreads >> 1) + (nThreads & 1);

  std::vector<PAYLOAD_T> output;
  int divW = nWrites / nP;
  int remW = nWrites % nP;

  int divR = nWrites / nC;
  int remR = nWrites % nC;

  producer_fails.store(0, std::memory_order_relaxed);
  consumer_fails.store(0, std::memory_order_relaxed);

  std::unique_ptr<BUFFER_T> buffer = std::make_unique<BUFFER_T>();

  // Measures total operation time for all writes to be produced and consumed
  BufferTestStats stats;

  Timer t;
  {

    std::vector<std::jthread> producers(nP), consumers(nC);

    for (int i{}; i < nP; ++i) {
      producers[i] =
          std::jthread(producer<BUFFER_T, PAYLOAD_T>, divW + (i < remW ? 1 : 0),
                       i, std::ref(buffer));
    }
    for (std::size_t i{}; i < nC; ++i) {
      consumers[i] =
          std::jthread(consumer<BUFFER_T, PAYLOAD_T>, divR + (i < remR ? 1 : 0),
                       std::ref(buffer), std::ref(output));
    }
  }

  auto [ms, cycles] = t.Stop();
  stats.time_ms = ms;
  stats.cycles = cycles;
  stats.allOperationsComplete = (output.size() == nWrites);
  stats.consumerAttempts = consumer_fails.load(std::memory_order_relaxed);
  stats.producerAttempts = producer_fails.load(std::memory_order_relaxed);

  return stats;
}

enum BufferType { LOCK_BUFFER, LOCK_FREE_BUFFER };

template <typename BUFFER_T, typename PAYLOAD_T>
BufferTestStats runTestAndStats(std::size_t nWrites, BufferType t,
                                std::size_t nThreads) {
  if (t == LOCK_BUFFER) {
    std::cout << "Test on lock ring MPMC with " << nWrites << " writes...\n\n";
  } else if (t == LOCK_FREE_BUFFER) {
    std::cout << "Test on lock-free ring MPMC before cache optimization with "
              << nWrites << " writes..\n\n";
  } else {
    std::cout << "\nTest on lock-free ring MPMC with " << nWrites
              << " writes...\n\n";
  }

  std::cout << "==============================================================="
               "==\n\n";
  uint64_t avgConsumerFails{}, avgProducerFails{}, avgCycles{};
  int16_t failedTrials{};
  double avgTime{};

  for (std::size_t i{}; i < N_TRIALS; ++i) {
    auto [cA, pA, aO, t, c] =
        runBufferTest<BUFFER_T, PAYLOAD_T>(nWrites, nThreads);
    failedTrials += (1 - aO);
    avgConsumerFails = (avgConsumerFails * i + cA) / (i + 1);
    avgProducerFails = (avgProducerFails * i + pA) / (i + 1);
    avgTime = (avgTime * i + t) / (i + 1);
    avgCycles = (avgCycles * i + c) / (i + 1);
  }

  std::cout << "Performance Results from " << N_TRIALS << " trials:\n";
  std::cout << "    Consumer Fails = " << avgConsumerFails << "\n";
  std::cout << "    Producer Fails = " << avgProducerFails << "\n";
  std::cout << "    Latency = " << avgTime << "ms\n";
  std::cout << "    Cycles = " << avgCycles << "\n";
  std::cout << "    Trial Fails = " << failedTrials << "\n";

  return {avgConsumerFails, avgProducerFails, failedTrials == 0, avgTime,
          avgCycles};
}

template <typename T> double percent_error(const T a, const T b) {
  return (a - b) * 100.0f / a;
}

int main() {
  int nWrites{};
  std::cout << "Number of writes: ";
  std::cin >> nWrites;

  std::cout << "Buffer capacity: " << MAXN << "\n";

  for (std::size_t nThreads{2}; nThreads <= MAX_THREADS; nThreads += 2) {
    std::cout << "\n\n\n\n\n\nTesting with " << nThreads << " threads...\n";

    BufferTestStats lock = runTestAndStats<mpmc_lock_ring<int, MAXN>, int>(
        nWrites, LOCK_BUFFER, nThreads);
    BufferTestStats lockfree = runTestAndStats<mpmc_ring<int, MAXN>, int>(
        nWrites, LOCK_FREE_BUFFER, nThreads);

    std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
                 ">>>>\n";
    std::cout << "Percent Errors (Lock vs. Lockfree)\n";
    std::cout << "    Latency = "
              << percent_error(lock.time_ms, lockfree.time_ms) << "%\n";
    std::cout << "    Cycles = " << percent_error(lock.cycles, lockfree.cycles)
              << "%\n";
  }
}
