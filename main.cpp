#include "./mpmc_lock_ring.hpp"
#include "./mpmc_ring.hpp"
#include "./performance.hpp"
#include <atomic>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

static constexpr std::size_t MAXN = 1 << 15;
static const int MAX_THREADS = std::thread::hardware_concurrency();

static std::mutex mx;

PaddedAtomicU64<std::size_t> producer_fails, consumer_fails, active_producers;

template <typename BUFFER_T, typename PAYLOAD_T>
void producer(int iterations, PAYLOAD_T item, BUFFER_T &buffer) {
  std::size_t local_fail = 0;
  for (int i{}; i < iterations; ++i) {
    while (!buffer.try_enqueue(item)) {
      local_fail++;
    }
  }
  active_producers.v.fetch_sub(1);
  producer_fails.v.fetch_add(local_fail, std::memory_order_relaxed);
}

template <typename BUFFER_T, typename PAYLOAD_T>
void consumer(int nItems, BUFFER_T &buffer, std::vector<PAYLOAD_T> &output) {
  PAYLOAD_T item;
  std::size_t local_fail = 0;
  while (active_producers.v.load() > 0) {
    if (buffer.try_dequeue(item)) {
      // {
      //   std::lock_guard<std::mutex> lk(mx);
      //   output.push_back(item);
      // }
    } else {
      local_fail++;
    }
  }

  consumer_fails.v.fetch_add(local_fail, std::memory_order_relaxed);
}

template <typename BUFFER_T, typename PAYLOAD_T> void runBufferTest(int nW) {
  int nP = MAX_THREADS >> 1, nC = (MAX_THREADS >> 1) + (MAX_THREADS & 1);

  std::cout << "Creating " << nP << " producers & " << nC << " consumers...\n";

  std::vector<PAYLOAD_T> output;
  int divW = nW / nP;
  int rem = nW % nP;

  std::cout << "Each producer thread writes " << divW << " with some (" << rem
            << ") writing " << divW + 1 << "...\n";

  producer_fails.v.store(0, std::memory_order_relaxed);
  consumer_fails.v.store(0, std::memory_order_relaxed);
  active_producers.v.store(nP, std::memory_order_relaxed);

  BUFFER_T buffer{};

  // Measures total operation time for all writes to be produced and consumed

  {
    Timer t;

    std::vector<std::jthread> producers(nP), consumers(nC);

    for (int i{}; i < nP; ++i) {
      producers[i] =
          std::jthread(producer<BUFFER_T, PAYLOAD_T>, divW + (i < rem ? 1 : 0),
                       i, std::ref(buffer));
    }
    for (std::size_t i{}; i < nC; ++i) {
      consumers[i] = std::jthread(consumer<BUFFER_T, PAYLOAD_T>, nW,
                                  std::ref(buffer), std::ref(output));
    }
  }

  std::cout << "\nReceived: " << output.size() << "\n";
  std::cout << "Consumer Fails: " << consumer_fails.v.load()
            << "    Producer Fails: " << producer_fails.v.load() << "\n";
}

int main() {
  int nWrites{};

  std::cout << "# of writes: ";
  std::cin >> nWrites;

  std::cout << "Test on lock ring MPMC with " << nWrites << " writes...\n\n";
  runBufferTest<mpmc_lock_ring<int, MAXN>, int>(nWrites);

  std::cout << "============================================\n";
  std::cout << "\nTest on lock-free ring MPMC with " << nWrites
            << " writes...\n\n";
  runBufferTest<mpmc_ring<int, MAXN>, int>(nWrites);
}
