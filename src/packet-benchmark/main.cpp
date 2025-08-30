#include "../../include/performance.hpp"
#include "./udp_multicast.cpp"
#include <unordered_map>

static const std::size_t MAX_THREADS = std::thread::hardware_concurrency();
static const std::size_t MAX_MCGRP = 1;
static const std::size_t BATCH_SZ = 10; // determines fix msg per file write

alignas(CLS) std::atomic<std::size_t> consumer_fails;
std::mutex data_io_lock;

static std::array<Multicast_grp, MAX_MCGRP> mutlicast_groups{
    {"224.0.0.1", "3000"}};
static constexpr std::string_view fix_log_path = "./log/data.txt";
static std::fstream fix_stream(fix_log_path.data(), fstream_mode);
static Fix_engine feed;

struct Accum {
  int64_t buy_qty;
  int64_t sell_qty;
  int64_t buy_notional; // (qty * price_ticks)
  int64_t sell_notional;
  uint32_t msg_count;
  uint32_t buy_count;
  uint32_t sell_count;
};

using Shard = std::unordered_map<std::string, Accum>;

void msg_processor() {
  Payload_t rx_msg;
  std::size_t batch_cnt = 0;
  std::size_t local_fail = 0;

  uint64_t window_id{0}, prev_window_id{0};
  Shard shard;
  shard.reserve(Fix_engine::symbol_count());

  while (true) {
    while (!(msg_ring_buffer.try_dequeue(rx_msg))) {
      local_fail++;
    }

    auto [sym, qty, px, side] = feed.interpret_fix_message(
        rx_msg.data, rx_msg.len); // get relevant data

    window_id = rx_msg.rx_ns / 100'000'000; // window of 100 ms

    if (window_id != prev_window_id) {

    } else {
      std::string key(sym);
      auto &a = shard[key];
      a.msg_count++;

      if (side == 1) {
        a.buy_count++;
        a.buy_qty += qty;
        a.buy_notional += static_cast<long double>(qty) * px;
      } else if (side == 2) {
        a.sell_count++;
        a.sell_qty += qty;
        a.sell_notional += static_cast<long double>(qty) * px;
      }
    }
  }
}
