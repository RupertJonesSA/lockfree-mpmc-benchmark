/*
 * Attempt of implementing Vyukov MPMC lockfree (non-blocking) bounded buffer
 *
 * Reference - Dimitry Vyukov (https://www.1024cores.net/home)
 * Author - Sami Al-Jamal
 * Filename - mpmc_ring.hpp
 * */
#ifndef MPMC_RING_HPP
#define MPMC_RING_HPP
#include <algorithm>
#include <atomic>
#include <cstddef>

#if defined(__cpp_lib_hardware_interference_size)
constexpr std::size_t CLS = std::hardware_destructive_interference_size;
#else
constexpr std::size_t CLS = 64; // x86 arch only
#endif

/*
 * To prevent "thundering herd" problem from occurring when a
 * thread attempts to enqueue or dequeue, an exponential backoff
 * class is implemented to avoid cache line ping-ponging between cores.
 * */
class Exponential_backoff {
private:
  static constexpr int min_delay = 1;
  static constexpr int max_delay = 1 << 10;
  int current = min_delay;

public:
  void wait() {
    for (int i = 0; i < current; ++i) {
      __builtin_ia32_pause(); // x86  pause instruction
    }
    current = std::min(current << 1, max_delay);
  }

  void reset() { current = min_delay; }
};

template <typename T, const std::size_t BUFFER_SIZE> class mpmc_ring {
private:
  static_assert(BUFFER_SIZE > 1, "capacity must be > 1");
  static_assert((BUFFER_SIZE & (BUFFER_SIZE - 1)) == 0,
                "capacity must be a power of 2 for optimal performance");

  /*
   * Maintaing bounded array implementation, however beware of the fact that
   * each cell is no longer padded. This is due to my previous ignorance
   * regarding how the cells will be read and write in a sequential manner; this
   * implementation follows a FIFO pattern, therefore spatial locality when
   * reading and writing may prove benefitial.
   *
   * Placing each on a separete cache line prevents sharing all together, but
   * this hinders benefits of the contiguous nature of the data structure.
   * */
  struct cell_t {
    std::atomic<std::size_t> seq_;
    T payload_;
  };

  alignas(CLS) cell_t *buffer_;
  std::size_t const buffer_mask_;
  alignas(CLS) std::atomic<std::size_t> enqueue_pos_;
  alignas(CLS) std::atomic<std::size_t> dequeue_pos_;

public:
  mpmc_ring();
  ~mpmc_ring();
  bool try_enqueue(const T &);
  bool try_dequeue(T &);

  mpmc_ring(const mpmc_ring &) = delete;
  mpmc_ring &operator=(const mpmc_ring &) = delete;

  template <typename Titerator>
  std::size_t try_enqueue_batch(Titerator, Titerator);

  static constexpr std::size_t capacity() { return BUFFER_SIZE; }
};

template <typename T, const std::size_t BUFFER_SIZE>
mpmc_ring<T, BUFFER_SIZE>::mpmc_ring()
    : buffer_(new cell_t[BUFFER_SIZE]), buffer_mask_(BUFFER_SIZE - 1) {
  for (std::size_t i{}; i < BUFFER_SIZE; ++i) {
    buffer_[i].seq_.store(i, std::memory_order_relaxed);
  }

  enqueue_pos_.store(0, std::memory_order_relaxed);
  dequeue_pos_.store(0, std::memory_order_relaxed);
}

template <typename T, const std::size_t BUFFER_SIZE>
mpmc_ring<T, BUFFER_SIZE>::~mpmc_ring() {
  delete[] buffer_;
}

template <typename T, std::size_t N>
bool mpmc_ring<T, N>::try_enqueue(const T &item) {
  /* LOGIC:
   * A cell is identified as being "empty" (payload has been read already) if
   * its seq is equivalent to the monotonic ticket loaded from the tail_
   * attribute.
   *
   * Therefore, three scenarios may occur
   * (1) diff = 0, cell is available to be written to.
   * (2) diff < 0, cell has not yet been read from, therefore cannot be written
   * (3) diff > 0, cell has already been written to by a previous thread
   * */

  cell_t *cell;
  std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);

  Exponential_backoff backoff;
  while (true) {
    cell = &buffer_[pos & buffer_mask_];
    std::size_t seq = cell->seq_.load(std::memory_order_acquire);

    intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

    if (diff == 0) {
      if (enqueue_pos_.compare_exchange_weak(pos, pos + 1,
                                             std::memory_order_relaxed))
        break;
      else
        backoff.wait();
    } else if (diff < 0) {
      return false;
    } else {
      backoff.reset();
      pos = enqueue_pos_.load(std::memory_order_relaxed);
    }
  }
  cell->payload_ = item;
  cell->seq_.store(pos + 1, std::memory_order_release);

  return true;
}

template <typename T, std::size_t BUFFER_SIZE>
bool mpmc_ring<T, BUFFER_SIZE>::try_dequeue(T &item) {
  /* LOGIC:
   * A cell is identified as being "filled" (payload is fresh) if its
   * seq is equivalent to the monotonic ticket loaded from the head_
   * attribute plus 1.
   *
   * Two scenarios may occur
   * (1) diff = 0, cell is available to be read from
   * (2) diff < 0, cell contains stail data (new write needed)
   * (3) diff > 0, cell has already been consumed (try another cell)
   * */
  cell_t *cell;
  std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);

  Exponential_backoff backoff;
  while (true) {
    cell = &buffer_[pos & buffer_mask_];
    std::size_t seq = cell->seq_.load(std::memory_order_acquire);
    intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

    if (diff == 0) {
      if (dequeue_pos_.compare_exchange_weak(pos, pos + 1,
                                             std::memory_order_relaxed))
        break;
      else
        backoff.wait();
    } else if (diff < 0) {
      return false; // cell empty; no new data has been written by a producer
                    // yet
    } else {
      backoff.reset();
      pos = dequeue_pos_.load(std::memory_order_relaxed);
    }
  }

  item = cell->payload_;
  cell->seq_.store(pos + BUFFER_SIZE, std::memory_order_release);

  return true;
}
#endif // MPMC_RING_HPP
