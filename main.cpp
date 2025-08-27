#include "./mpmc_lock_ring.hpp"
#include "./mpmc_ring.hpp"
#include "./mpmc_ring_lowper.hpp"
#include "./performance.hpp"
#include <algorithm>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

static constexpr std::size_t MAXN = 1 << 20;
static constexpr std::size_t BATCH_SIZE = 25;
static constexpr std::size_t N_TRIALS = 50;
static const int MAX_THREADS = std::thread::hardware_concurrency();

static std::mutex mx;

PaddedAtomicU64<std::size_t> producer_fails, consumer_fails;

template <typename BUFFER_T, typename PAYLOAD_T, std::size_t BATCH_SZ>
void producer(int iterations, PAYLOAD_T item,
              std::unique_ptr<BUFFER_T> &buffer) {
  std::size_t local_fail = 0;
  std::array<PAYLOAD_T, BATCH_SZ> batch;

  while (iterations > 0) {
    std::size_t cur_batch = std::min<std::size_t>(BATCH_SZ, iterations);

    for (std::size_t i{}; i < cur_batch; ++i)
      batch[i] = item;

    std::size_t actual_batch =
        buffer->try_enqueue_batch(batch.begin(), batch.begin() + cur_batch);
    iterations -= actual_batch;
    if (actual_batch == 0)
      local_fail++;
  }

  producer_fails.v.fetch_add(local_fail, std::memory_order_relaxed);
}

template <typename BUFFER_T, typename PAYLOAD_T>
void producer(int iterations, PAYLOAD_T item,
              std::unique_ptr<BUFFER_T> &buffer) {
  std::size_t local_fail = 0;
  for (int i{}; i < iterations; ++i) {
    while (!buffer->try_enqueue(item)) {
      local_fail++;
    }
  }
  producer_fails.v.fetch_add(local_fail, std::memory_order_relaxed);
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

  consumer_fails.v.fetch_add(local_fail, std::memory_order_relaxed);
}

template <typename BUFFER_T, typename PAYLOAD_T>
BufferTestStats runBufferTest(int nW, bool includeBatch) {
  int nP = MAX_THREADS >> 1, nC = (MAX_THREADS >> 1) + (MAX_THREADS & 1);

  std::vector<PAYLOAD_T> output;
  int divW = nW / nP;
  int remW = nW % nP;

  int divR = nW / nC;
  int remR = nW % nC;

  producer_fails.v.store(0, std::memory_order_relaxed);
  consumer_fails.v.store(0, std::memory_order_relaxed);

  std::unique_ptr<BUFFER_T> buffer = std::make_unique<BUFFER_T>();

  // Measures total operation time for all writes to be produced and consumed
  BufferTestStats stats;

  Timer t;
  {

    std::vector<std::jthread> producers(nP), consumers(nC);

    for (int i{}; i < nP; ++i) {
      if (includeBatch && BUFFER_T::hasBatch()) {
        producers[i] =
            std::jthread(producer<BUFFER_T, PAYLOAD_T, BATCH_SIZE>,
                         divW + (i < remW ? 1 : 0), i, std::ref(buffer));
      } else {
        producers[i] =
            std::jthread(producer<BUFFER_T, PAYLOAD_T>,
                         divW + (i < remW ? 1 : 0), i, std::ref(buffer));
      }
    }
    for (std::size_t i{}; i < nC; ++i) {
      consumers[i] =
          std::jthread(consumer<BUFFER_T, PAYLOAD_T>, divR + (i < remR ? 1 : 0),
                       std::ref(buffer), std::ref(output));
    }
  }

  stats.time_ms = t.Stop();

  stats.allOperationsComplete = (output.size() == nW);
  stats.consumerAttempts = consumer_fails.v.load(std::memory_order_relaxed);
  stats.producerAttempts = producer_fails.v.load(std::memory_order_relaxed);

  return stats;
}

enum BufferType { LOCK_BUFFER, LOCK_FREE_BUFFER, LOCK_FREE_BUFFER_UO };

template <typename BUFFER_T, typename PAYLOAD_T>
void runTestAndStats(int nW, BufferType t, bool includeBatch) {
  if (t == LOCK_BUFFER) {
    std::cout << "Test on lock ring MPMC with " << nW << " writes...\n\n";
  } else if (t == LOCK_FREE_BUFFER) {
    std::cout << "Test on lock-free ring MPMC before cache optimization with "
              << nW << " writes..\n\n";
  } else {
    std::cout << "\nTest on lock-free ring MPMC with " << nW
              << " writes...\n\n";
  }

  std::cout << "==============================================================="
               "==\n\n";
  uint64_t avgConsumerFails{}, avgProducerFails{};
  int16_t failedTrials{};
  double avgTime{};

  for (std::size_t i{}; i < N_TRIALS; ++i) {
    auto [cA, pA, aO, t] = runBufferTest<BUFFER_T, PAYLOAD_T>(nW, includeBatch);
    failedTrials += (1 - aO);
    avgConsumerFails = (avgConsumerFails * i + cA) / (i + 1);
    avgProducerFails = (avgProducerFails * i + pA) / (i + 1);
    avgTime = (avgTime * i + t) / (i + 1);
  }

  std::cout << "Performance Results from " << N_TRIALS << " trials:\n";
  std::cout << "    Consumer Fails = " << avgConsumerFails << "\n";
  std::cout << "    Producer Fails = " << avgProducerFails << "\n";
  std::cout << "    Latency = " << avgTime << "ms\n";
  std::cout << "    Trial Fails = " << failedTrials << "\n\n\n";
}

int main() {
  int nWrites{};
  std::cout << "Number of writes: ";
  std::cin >> nWrites;

  char response{};
  std::cout << "Include batched enqueues (Y/N)? ";
  std::cin >> response;

  bool includeBatch = (response == 'Y');

  std::cout << "Buffer capacity: " << MAXN << "\n";

  runTestAndStats<mpmc_lock_ring<int, MAXN>, int>(nWrites, LOCK_BUFFER,
                                                  includeBatch);
  runTestAndStats<mpmc_ring<int, MAXN>, int>(nWrites, LOCK_FREE_BUFFER,
                                             includeBatch);
  runTestAndStats<mpmc_ring_lowper<int, MAXN>, int>(
      nWrites, LOCK_FREE_BUFFER_UO, includeBatch);
}
