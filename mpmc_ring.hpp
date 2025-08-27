/*
 * Attempt of implementing Vyukov MPMC lockfree (non-blocking) bounded buffer
 *
 * Author - Sami Al-Jamal
 * Filename - mpmc_ring.hpp
 * */
#ifndef MPMC_RING_HPP
#define MPMC_RING_HPP
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <mutex>
#include <utility>
#include <vector>

#if defined(__cpp_lib_hardware_interference_size)
constexpr std::size_t CLS = std::hardware_destructive_interference_size;
#else
constexpr std::size_t CLS = 64; // x86 arch only
#endif

template <typename T> struct alignas(CLS) PaddedAtomicU64 {
  std::atomic<T> v;
};

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

template <typename T, const std::size_t N> class mpmc_ring {
private:
  static_assert(N > 1, "capacity must be > 1");
  static_assert((N & (N - 1)) == 0,
                "capacity must be a power of 2 for optimal performance");

  /*
   * Separate payload from atomic variable to avoid cache
   * invalidation that may occur when attempting to write and read
   * from both.
   *
   * When seq_ is being updated it may invalidate the cache line
   * while another is trying to read from payload (if tightly packed).
   * */
  std::array<PaddedAtomicU64<std::size_t>, N>
      seq_;                  // used to determine if cell within buffer has
  std::array<T, N> payload_; // been processed or not

  PaddedAtomicU64<std::size_t> head_; // consumer tickets
  PaddedAtomicU64<std::size_t> tail_; // producer tickets
  std::mutex mx;

public:
  mpmc_ring();
  bool try_enqueue(const T &);
  bool try_enqueue(T &&);
  bool try_dequeue(T &);

  template <typename Titerator>
  std::size_t try_enqueue_batch(Titerator, Titerator);

  static constexpr std::size_t capacity() { return N; }
  static constexpr bool hasBatch() { return true; }
};

template <typename T, const std::size_t N> mpmc_ring<T, N>::mpmc_ring() {
  for (std::size_t i{}; i < N; ++i)
    seq_[i].v.store(i, std::memory_order_relaxed);

  head_.v.store(0, std::memory_order_relaxed);
  tail_.v.store(0, std::memory_order_relaxed);
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

  std::size_t ticket = tail_.v.load(
      std::memory_order_relaxed); // producer thread specific ticket identifier

  Exponential_backoff backoff;

  while (true) {
    T &cur_payload =
        payload_[ticket &
                 (N - 1)]; // reference to particular cell to be written to
    PaddedAtomicU64<std::size_t> &cur_seq = seq_[ticket & (N - 1)];
    std::size_t s = cur_seq.v.load(std::memory_order_acquire);
    ptrdiff_t diff = static_cast<ptrdiff_t>(s) - static_cast<ptrdiff_t>(ticket);

    if (diff == 0) {
      if (tail_.v.compare_exchange_weak(ticket, ticket + 1,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
        // Thread owns this slot
        cur_payload = item;
        cur_seq.v.store(ticket + 1,
                        std::memory_order_release); // publish to consumers that
                                                    // cell is fresh
        return true;
      }

      // CAS failing indicates that another thread had used the ticket, refresh
      // and  try again
      backoff.wait();

    } else if (diff < 0) {
      // queue is full; try later
      return false;

    } else {
      // refresh ticket; another producer has already used the current ticket
      ticket = tail_.v.load(std::memory_order_relaxed);
      backoff.reset();
    }
  }

  return false;
}

template <typename T, std::size_t N>
bool mpmc_ring<T, N>::try_enqueue(T &&item) {
  std::size_t ticket = tail_.v.load(std::memory_order_relaxed);
  Exponential_backoff backoff;

  while (true) {
    T &cur_payload = payload_[ticket & (N - 1)];
    PaddedAtomicU64<std::size_t> &cur_seq = seq_[ticket & (N - 1)];
    std::size_t s = cur_seq.v.load(std::memory_order_acquire);
    ptrdiff_t diff = static_cast<ptrdiff_t>(s) - static_cast<ptrdiff_t>(ticket);

    if (diff == 0) {
      if (tail_.v.compare_exchange_weak(ticket, ticket + 1,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
        cur_payload = std::move(item);
        cur_seq.v.store(ticket + 1, std::memory_order_release);

        return true;
      }

      backoff.wait();

    } else if (diff < 0) {
      return false;
    } else {
      ticket = tail_.v.load(std::memory_order_relaxed);
      backoff.reset();
    }
  }

  return false;
}

template <typename T, std::size_t N>
bool mpmc_ring<T, N>::try_dequeue(T &item) {
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

  std::size_t ticket = head_.v.load(std::memory_order_relaxed);

  Exponential_backoff backoff;

  for (;;) {
    T &cur_payload = payload_[ticket & (N - 1)];
    PaddedAtomicU64<std::size_t> &cur_seq = seq_[ticket & (N - 1)];
    std::size_t s = cur_seq.v.load(std::memory_order_acquire);
    ptrdiff_t diff =
        static_cast<ptrdiff_t>(s) - static_cast<ptrdiff_t>(ticket + 1);

    if (diff == 0) {
      if (head_.v.compare_exchange_weak(ticket, ticket + 1,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
        item = cur_payload;
        cur_seq.v.store(ticket + N, std::memory_order_release);

        return true;
      }

      // If CAS failed, another thread already dequed using the current ticket,
      // retry.
      backoff.wait();

    } else if (diff < 0) {
      // still stail
      return false;
    } else {
      ticket = head_.v.load(std::memory_order_relaxed);
      backoff.reset();
    }
  }

  return false;
}

template <typename T, std::size_t N>
template <typename Titerator>
std::size_t mpmc_ring<T, N>::try_enqueue_batch(Titerator start, Titerator end) {
  std::size_t desired_batch_size = std::distance(start, end);
  if (desired_batch_size == 0)
    return 0;

  std::size_t ticket = tail_.v.load(std::memory_order_relaxed);

  Exponential_backoff backoff;
  for (;;) {
    const std::size_t head = head_.v.load(std::memory_order_acquire);
    const std::size_t free_cells = N - (ticket - head);
    if (free_cells == 0)
      return 0;

    const std::size_t take = std::min(desired_batch_size, free_cells);
    if (tail_.v.compare_exchange_weak(ticket, ticket + take,
                                      std::memory_order_acq_rel,
                                      std::memory_order_relaxed)) {
      for (std::size_t i{}; i < take; ++i, ++start) {
        const std::size_t pos = ticket + i;
        const std::size_t idx = pos & (N - 1);
        PaddedAtomicU64<std::size_t> &cell = seq_[idx];
        T &payload = payload_[idx];

        // Ensure the cell is actually free for this pos.
        // With correct capacity accounting this should be ready quickly;
        // if it isn't, something else is wrong.
        while (cell.v.load(std::memory_order_acquire) != pos) {
          backoff.wait();
        }
        backoff.reset();

        payload = *start;
        cell.v.store(pos + 1, std::memory_order_release);
      }

      return take;
    }

    // lost race; retry with updated ticket
  }
}
#endif // MPMC_RING_HPP
