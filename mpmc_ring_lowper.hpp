/*
 * Attempt of implementing Vyukov MPMC lockfree (non-blocking) bounded buffer
 *
 * Author - Sami Al-Jamal
 * Filename - mpmc_ring_lowper.hpp
 * */
#ifndef MPMC_RING_LOWPER_HPP
#define MPMC_RING_LOWPER_HPP
#include <array>
#include <atomic>
#include <cstddef>
#include <utility>

#if defined(__cpp_lib_hardware_interference_size)
constexpr std::size_t CLS_t = std::hardware_destructive_interference_size;
#else
constexpr std::size_t CLS_t = 64; // x86 arch only
#endif

template <typename T> struct alignas(CLS_t) PaddedAtomicU64_t {
  std::atomic<T> v;
};

template <typename T, const std::size_t N> class mpmc_ring_lowper {
private:
  static_assert(N > 1, "capacity must be > 1");
  static_assert((N & (N - 1)) == 0,
                "capacity must be a power of 2 for optimal performance");

  struct alignas(CLS_t) cell {
    std::atomic<std::size_t> seq; // used to determine if cell within buffer has
                                  // been processed or not
    T payload;
  };

  std::array<cell, N> buf_;
  PaddedAtomicU64_t<std::size_t> head_; // consumer tickets
  PaddedAtomicU64_t<std::size_t> tail_; // producer tickets

public:
  mpmc_ring_lowper();
  bool try_enqueue(const T &);
  bool try_enqueue(T &&);
  bool try_dequeue(T &);

  template <typename Titerator>
  std::size_t try_enqueue_batch(Titerator, Titerator);

  static constexpr std::size_t capacity() { return N; }
  static constexpr bool hasBatch() { return false; }
};

template <typename T, const std::size_t N>
mpmc_ring_lowper<T, N>::mpmc_ring_lowper() {
  for (std::size_t i{}; i < N; ++i)
    buf_[i].seq.store(i, std::memory_order_relaxed);

  head_.v.store(0, std::memory_order_relaxed);
  tail_.v.store(0, std::memory_order_relaxed);
}

template <typename T, std::size_t N>
bool mpmc_ring_lowper<T, N>::try_enqueue(const T &item) {
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

  while (true) {
    cell &c =
        buf_[ticket & (N - 1)]; // reference to particular cell to be written to
    std::size_t s = c.seq.load(std::memory_order_acquire);
    ptrdiff_t diff = static_cast<ptrdiff_t>(s) - static_cast<ptrdiff_t>(ticket);

    if (diff == 0) {
      if (tail_.v.compare_exchange_weak(ticket, ticket + 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
        // Thread owns this slot
        c.payload = item;
        c.seq.store(ticket + 1,
                    std::memory_order_release); // publish to consumers that
                                                // cell is fresh
        return true;
      }

      // CAS failing indicates that another thread had used the ticket, refresh
      // and  try again

    } else if (diff < 0) {
      // queue is full; try later
      return false;

    } else {
      // refresh ticket; another producer has already used the current ticket
      ticket = tail_.v.load(std::memory_order_relaxed);
    }
  }

  return false;
}

template <typename T, std::size_t N>
bool mpmc_ring_lowper<T, N>::try_enqueue(T &&item) {
  std::size_t ticket = tail_.v.load(std::memory_order_relaxed);

  while (true) {
    cell &c = buf_[ticket & (N - 1)];
    std::size_t s = c.seq.load(std::memory_order_acquire);
    ptrdiff_t diff = static_cast<ptrdiff_t>(s) - static_cast<ptrdiff_t>(ticket);

    if (diff == 0) {
      if (tail_.v.compare_exchange_weak(ticket, ticket + 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
        c.payload = std::move(item);
        c.seq.store(ticket + 1, std::memory_order_release);

        return true;
      }
    } else if (diff < 0) {
      return false;
    } else {
      ticket = tail_.v.load(std::memory_order_relaxed);
    }
  }

  return false;
}

template <typename T, std::size_t N>
bool mpmc_ring_lowper<T, N>::try_dequeue(T &item) {
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

  while (true) {
    cell &c = buf_[ticket & (N - 1)];
    std::size_t s = c.seq.load(std::memory_order_acquire);
    ptrdiff_t diff =
        static_cast<ptrdiff_t>(s) - static_cast<ptrdiff_t>(ticket + 1);

    if (diff == 0) {
      if (head_.v.compare_exchange_weak(ticket, ticket + 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
        item = std::move(c.payload);
        c.seq.store(ticket + N, std::memory_order_release);
        return true;
      }

      // If CAS failed, another thread already dequed using the current ticket,
      // retry.
    } else if (diff < 0) {
      // still stail
      return false;
    } else {
      ticket = head_.v.load(std::memory_order_relaxed);
    }
  }

  return false;
}

template <typename T, std::size_t N>
template <typename Titerator>
std::size_t mpmc_ring_lowper<T, N>::try_enqueue_batch(Titerator start,
                                                      Titerator end) {
  return 0;
}

#endif // MPMC_RING_LOWPER_HPP
