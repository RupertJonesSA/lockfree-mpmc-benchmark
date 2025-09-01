/*
 * Attempt of implementing standard locking MPMC bounded buffer (OSTEP)
 *
 * Author - Sami Al-Jamal
 * Filename - mpmc_lock_ring.hpp
 * */
#ifndef MPMC_LOCK_RING_HPP
#define MPMC_LOCK_RING_HPP
#include <array>
#include <semaphore.h>

template <typename T, const std::size_t BUFFER_SIZE> class mpmc_lock_ring {
private:
  sem_t empty_cells_, full_cells_,
      mlck_; // unfortunately C++20 std::counting_semaphore suffers from
             // deadlock bug; note that std::mutex interfers with sem_t (use
             // only one)

  std::size_t head_{0}; // consumer index
  std::size_t tail_{0}; // producer index

  std::array<T, BUFFER_SIZE> buffer_;
  std::size_t buffer_mask_{BUFFER_SIZE - 1};

public:
  static_assert(BUFFER_SIZE > 1, "capacity must be > 1");
  static_assert((BUFFER_SIZE & (BUFFER_SIZE - 1)) == 0,
                "capacity must be a power of two for optimal performance");

  mpmc_lock_ring();
  ~mpmc_lock_ring();
  bool try_enqueue(const T &);
  bool try_dequeue(T &);

  static constexpr std::size_t capacity() { return BUFFER_SIZE; }
};

template <typename T, std::size_t BUFFER_SIZE>
mpmc_lock_ring<T, BUFFER_SIZE>::mpmc_lock_ring() {
  sem_init(&empty_cells_, 0, BUFFER_SIZE);
  sem_init(&full_cells_, 0, 0);
  sem_init(&mlck_, 0, 1);
}

template <typename T, std::size_t BUFFER_SIZE>
mpmc_lock_ring<T, BUFFER_SIZE>::~mpmc_lock_ring() {
  sem_destroy(&empty_cells_);
  sem_destroy(&full_cells_);
  sem_destroy(&mlck_);
}

template <typename T, std::size_t BUFFER_SIZE>
bool mpmc_lock_ring<T, BUFFER_SIZE>::try_enqueue(const T &item) {
  if (sem_trywait(&empty_cells_)) {
    return false; // buffer full, return immediately to avoid deadlock
  }
  sem_wait(&mlck_);
  buffer_[tail_] = item;
  tail_ = (tail_ + 1) & (BUFFER_SIZE - 1);
  sem_post(&mlck_);
  sem_post(&full_cells_); // notify consumers
  return true;
}

template <typename T, std::size_t BUFFER_SIZE>
bool mpmc_lock_ring<T, BUFFER_SIZE>::try_dequeue(T &item) {
  if (sem_trywait(&full_cells_)) {
    return false; // buffer empty, return immediately
  }
  sem_wait(&mlck_);
  item = buffer_[head_];
  head_ = (head_ + 1) & (BUFFER_SIZE - 1);
  sem_post(&mlck_);
  sem_post(&empty_cells_); // notify producers of new empty cell
  return true;
}

#endif // MPMC_LOCK_RIBUFFER_SIZEG_HPP
