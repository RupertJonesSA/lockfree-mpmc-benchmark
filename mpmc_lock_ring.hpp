/*
 * Attempt of implementing standard locking MPMC bounded buffer (OSTEP)
 *
 * Author - Sami Al-Jamal
 * Filename - mpmc_lock_ring.hpp
 * */
#ifndef MPMC_LOCK_RING_HPP
#define MPMC_LOCK_RING_HPP
#include <array>
#include <mutex>
#include <semaphore.h>
#include <utility>

template <typename T, const std::size_t N> class mpmc_lock_ring {
public:
  static_assert(N > 1, "capacity must be > 1");
  static_assert((N & (N - 1)) == 0,
                "capacity must be a power of two for optimal performance");

  mpmc_lock_ring();
  ~mpmc_lock_ring();
  bool try_enqueue(const T &);
  bool try_enqueue(T &&);
  bool try_dequeue(T &);

  static constexpr std::size_t capacity() { return N; }

private:
  std::mutex mlck_;

  sem_t empty_cells_,
      full_cells_; // unfortunately C++20 std::counting_semaphore suffers from
                   // deadlock bug

  std::size_t head_{0}; // consumer index
  std::size_t tail_{0}; // producer index

  std::array<T, N> buf_;
};

template <typename T, std::size_t N> mpmc_lock_ring<T, N>::mpmc_lock_ring() {
  sem_init(&empty_cells_, 0, N);
  sem_init(&full_cells_, 0, 0);
}

template <typename T, std::size_t N> mpmc_lock_ring<T, N>::~mpmc_lock_ring() {
  sem_destroy(&empty_cells_);
  sem_destroy(&full_cells_);
}

template <typename T, std::size_t N>
bool mpmc_lock_ring<T, N>::try_enqueue(const T &item) {
  if (sem_trywait(&empty_cells_)) {
    return false; // buffer fulls, return immediately to avoid deadlock
  }

  {
    std::lock_guard<std::mutex> lkg(mlck_);
    buf_[tail_] = item;
    tail_ = (tail_ + 1) & (N - 1);
  }
  sem_post(&full_cells_); // notify consumers
  return true;
}

template <typename T, std::size_t N>
bool mpmc_lock_ring<T, N>::try_enqueue(T &&item) {
  if (sem_trywait(&empty_cells_)) {
    return false;
  }

  {
    std::lock_guard<std::mutex> lkg(mlck_);
    buf_[tail_] = std::move(item);
    tail_ = (tail_ + 1) & (N - 1);
  }

  sem_post(&full_cells_);
  return true;
}

template <typename T, std::size_t N>
bool mpmc_lock_ring<T, N>::try_dequeue(T &item) {
  if (sem_trywait(&full_cells_)) {
    return false; // buffer empty, return immediately
  }

  {
    std::lock_guard<std::mutex> lkg(mlck_);
    item = buf_[head_];
    head_ = (head_ + 1) & (N - 1);
  }

  sem_post(&empty_cells_); // notify producers of new empty cell
  return true;
}
#endif // MPMC_LOCK_RING_HPP
