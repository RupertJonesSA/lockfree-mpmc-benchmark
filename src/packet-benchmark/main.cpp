#include "../../include/udp_multicast.hpp"
#include <atomic>
#include <bit>
#include <csignal>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <unordered_map>

static const std::size_t MAX_THREADS = std::thread::hardware_concurrency();
#ifndef SIMULATION_TIME_S
#define SIMULATION_TIME_S 30
#endif // in seconds

alignas(CLS) std::atomic<std::size_t> consumer_fails;
alignas(CLS) std::atomic<uint64_t> successfully_processed;
alignas(CLS) std::atomic<uint64_t> emitted_window_id{
    0}; // previous 100ms window emitted
alignas(CLS) std::atomic<uint64_t> cur_shared_window_id{
    0}; // current 100ms window being constructed
alignas(CLS) std::atomic_flag emitter_busy = ATOMIC_FLAG_INIT;
std::mutex shard_lock;

// ==== NEW: simulation start (ns since epoch) to normalize window ids to 0
alignas(CLS) std::atomic<uint64_t> sim_t0_ns{0};

static std::array<Multicast_grp, 14> multicast_groups{{{"224.0.0.1", "3000"},
                                                       {"224.0.1.0", "3001"},
                                                       {"224.1.0.0", "3002"},
                                                       {"224.0.1.1", "3003"},
                                                       {"224.1.1.1", "3004"},
                                                       {"224.1.1.2", "3005"},
                                                       {"224.1.2.2", "3006"},
                                                       {"224.2.1.1", "3007"},
                                                       {"224.2.1.2", "3008"},
                                                       {"224.2.2.1", "3009"},
                                                       {"224.2.2.2", "3010"},
                                                       {"224.3.0.0", "3011"},
                                                       {"224.3.0.1", "3012"},
                                                       {"224.3.1.0", "3013"}}};
static Fix_engine feed;

struct Accum {
  int64_t buy_qty;
  int64_t sell_qty;
  long double buy_notional; // (qty * price_ticks)
  long double sell_notional;
  uint32_t msg_count;
  uint32_t buy_count;
  uint32_t sell_count;
  // Exact queue-wait mean support
  uint64_t qwait_sum_ns = 0;
  uint64_t qwait_cnt = 0;
};

using Shard = std::unordered_map<std::string, Accum>;

static Shard active_shared_shard;
static Shard drained_shared_shard;

// histogram with 64 log2 buckets in microseconds
struct Histo64 {
  uint64_t buckets[64]{};
  uint64_t samples{0};
  uint64_t max_ns{0};

  inline static uint64_t bucket_ns(uint64_t ns) {
    if (ns == 0)
      return 0;
    uint64_t e = 64 - std::countl_zero(ns); // ceil(log2(ns+1))
    return (e < 64 ? e : 63);
  }
  inline void observe_ns(uint64_t ns) {
    uint8_t b = bucket_ns(ns);
    buckets[b]++;
    samples++;
    max_ns = std::max(max_ns, ns);
  }

  inline void clear() {
    memset(buckets, 0, sizeof(buckets));
    samples = 0;
    max_ns = 0;
  }
};

static Histo64 active_qwait, drained_qwait; // per window globals

static inline void histo_merge(const Histo64 &src, Histo64 &dst) {
  for (int i{}; i < 64; ++i)
    dst.buckets[i] += src.buckets[i];
  dst.samples += src.samples;
  dst.max_ns = std::max(dst.max_ns, src.max_ns);
}

// Compute {p50, p90, p99, max, count} from a histogram
static inline void histo_percentiles(const Histo64 &h, uint64_t &p50,
                                     uint64_t &p90, uint64_t &p99,
                                     uint64_t &pmax, uint64_t &count) {
  count = h.samples;
  pmax = h.max_ns;

  if (h.samples == 0) {
    p50 = p90 = p99 = 0;
    return;
  }

  const uint64_t k50 = (h.samples + 1) * 0.5;
  const uint64_t k90 = (h.samples + 1) * 0.9;
  const uint64_t k99 = (h.samples + 1) * 0.99;

  uint64_t c = 0;
  auto upper = [](int b) -> uint64_t {
    // bucket i represents [2^(i-1), 2^i) us for i>=1; i=0 => [0, 1)
    if (b == 0)
      return 1;
    if (b >= 63)
      return std::numeric_limits<uint64_t>::max();
    return 1ull << b;
  };

  bool f50 = false, f90 = false, f99 = false;
  for (int i{}; i < 64; ++i) {
    c += h.buckets[i];
    if (!f50 && c >= k50) {
      p50 = std::min(upper(i) - 1, h.max_ns);
      f50 = true;
    }
    if (!f90 && c >= k90) {
      p90 = std::min(upper(i) - 1, h.max_ns);
      f90 = true;
    }
    if (!f99 && c >= k99) {
      p99 = std::min(upper(i) - 1, h.max_ns);
      f99 = true;
    }

    if (f50 && f90 && f99)
      break;
  }
}

static void emit_csv_header_once(ssize_t recv, ssize_t send, ssize_t msg_proc) {
  static std::once_flag once;
  std::call_once(once, [recv, send, msg_proc] {
#ifdef USE_LOCK
    stats_out << "impl,lock\n";
#else 
    stats_out << "impl,lockfree\n";
#endif

    stats_out << "threads, udp_recv=" << recv << ",udp_send=" << send
              << ",msg_proc=" << msg_proc << "\n"
              << "host, i7-12650H" << "\n"
              << "build, O3-march=native\n"
              << "cur_mcgrp," << CUR_MCGRP << "\n"
              << "send_rate, fixed\n"
              << "schema,1\n"
              << "ts_start_ns,ts_end_ns,window_id,sym,"
              << "buy_qty,sell_qty,buy_notional,sell_notional,"
              << "buy_trades,sell_trades,buy_vwap,sell_vwap,imbalance,"
              << "qwait_p50_ns,qwait_p90_ns,qwait_p99_ns,qwait_max_ns,qwait_"
                 "count,"
              << "qwait_mean_ns,"
              << "drops_full,drops_oversize,recv_count,parse_errs,partial\n";
    stats_out.flush();
  });
}

static inline double safe_div(double num, double den) {
  return (den == 0.0) ? 0.0 : (num / den);
}

// Emit one CSV row and a compact line to stdout
static void emit_symbol_row(uint64_t ts_start_ns, uint64_t ts_end_ns,
                            uint64_t window_id, const std::string &sym,
                            const Accum &a, uint64_t q_p50, uint64_t q_p90,
                            uint64_t q_p99, uint64_t q_max, uint64_t q_n,
                            uint64_t drops_full, uint64_t drops_oversize,
                            uint64_t recv_count, uint64_t parse_errs,
                            int partial) {
  long double q_mean = (a.qwait_cnt) ? (a.qwait_sum_ns / a.qwait_cnt) : 0;
  long double buy_vwap =
      safe_div((double)a.buy_notional, (double)std::max<int64_t>(a.buy_qty, 1));
  long double sell_vwap = safe_div((double)a.sell_notional,
                                   (double)std::max<int64_t>(a.sell_qty, 1));
  int64_t bt = a.buy_qty, st = a.sell_qty;
  double imb = (bt + st) ? (double)(bt - st) / (double)(bt + st) : 0.0;

  stats_out << ts_start_ns << ',' << ts_end_ns << ',' << window_id << ',' << sym
            << ',' << a.buy_qty << ',' << a.sell_qty << ',' << a.buy_notional
            << ',' << a.sell_notional << ',' << a.buy_count << ','
            << a.sell_count << ',' << buy_vwap << ',' << sell_vwap << ',' << imb
            << ',' << q_p50 << ',' << q_p90 << ',' << q_p99 << ',' << q_max
            << ',' << q_n << ',' << q_mean << ',' << drops_full << ','
            << drops_oversize << ',' << recv_count << ',' << parse_errs << ','
            << partial << '\n';
}

void msg_processor() {
  Payload_t rx_msg;
  std::size_t local_processed = 0;

  uint64_t window_id{0}, prev_window_id{0};
  Shard shard;
  shard.reserve(Fix_engine::symbol_count());

  Histo64 local_qwait; // distribution of latency enqueue->dequeue

  while (true) {
    while (!(msg_ring_buffer.try_dequeue(rx_msg))) {
      if (!running.load(std::memory_order_relaxed)) {
        goto End;
      }
    }

    // normalize window-id to simulation start
    const uint64_t base = sim_t0_ns.load(std::memory_order_acquire);

    // qwait observe (enqueue->dequeue time)
    const uint64_t q_ns = now_ns() - rx_msg.rx_ns;
    local_qwait.observe_ns(q_ns);

    // bucket by 100 ms windows starting at 0
    const uint64_t relns =
        (rx_msg.rx_ns >= base) ? (rx_msg.rx_ns - base) : 0ULL;
    window_id = relns / 100'000'000ull; // 100 ms windows

    auto [sym, qty, px, side] = feed.interpret_fix_message(
        rx_msg.data, rx_msg.len); // get relevant data

    // publish the newest window id (monotone max)
    uint64_t cur = cur_shared_window_id.load(std::memory_order_relaxed);
    while (window_id > cur && !cur_shared_window_id.compare_exchange_weak(
                                  cur, window_id, std::memory_order_release,
                                  std::memory_order_relaxed))
      ;

    if (window_id > prev_window_id) {
      // roll: merge shard + qwait into global active
      {
        std::lock_guard<std::mutex> lock(shard_lock);
        for (auto &[k, accum] : shard) {
          Accum &a = active_shared_shard[k];
          a.msg_count += accum.msg_count;
          a.buy_count += accum.buy_count;
          a.sell_count += accum.sell_count;
          a.buy_notional += accum.buy_notional;
          a.sell_notional += accum.sell_notional;
          a.buy_qty += accum.buy_qty;
          a.sell_qty += accum.sell_qty;
          a.qwait_sum_ns += accum.qwait_sum_ns;
          a.qwait_cnt += accum.qwait_cnt;
        }
        histo_merge(local_qwait, active_qwait);
      }

      shard.clear();
      local_qwait.clear();
      prev_window_id = window_id;
    }

    // accumulate into shard
    std::string key(sym);
    auto &a = shard[key];
    a.msg_count++;
    a.qwait_cnt++;
    a.qwait_sum_ns += q_ns;
    if (side == 1) {
      a.buy_count++;
      a.buy_qty += qty;
      a.buy_notional += (long double)qty * px;
    } else if (side == 2) {
      a.sell_count++;
      a.sell_qty += qty;
      a.sell_notional += (long double)qty * px;
    }

    local_processed++;
  }

End:
  // final push of this worker's shard + qwait upon exit
  {
    std::lock_guard<std::mutex> lock(shard_lock);
    for (auto &[k, accum] : shard) {
      Accum &a = active_shared_shard[k];
      a.msg_count += accum.msg_count;
      a.buy_count += accum.buy_count;
      a.sell_count += accum.sell_count;
      a.buy_notional += accum.buy_notional;
      a.sell_notional += accum.sell_notional;
      a.buy_qty += accum.buy_qty;
      a.sell_qty += accum.sell_qty;
      a.qwait_sum_ns += accum.qwait_sum_ns;
      a.qwait_cnt += accum.qwait_cnt;
    }
    histo_merge(local_qwait, active_qwait);
  }

  successfully_processed.fetch_add(local_processed, std::memory_order_relaxed);
}

static uint64_t prev_full_drops{0}, prev_oversize_drops{0}, prev_enqueued{0},
    prev_parse_errs{0};
static inline void snapshot_counters(uint64_t &drops_full,
                                     uint64_t &drops_oversize,
                                     uint64_t &recv_count,
                                     uint64_t &parse_errs) {
  // these atomics live in udp_multicast.cpp
  uint64_t f = full_drops.load(std::memory_order_relaxed);
  uint64_t os = oversize_drops.load(std::memory_order_relaxed);
  uint64_t en = enqueued.load(std::memory_order_relaxed);
  uint64_t pe = 0;

  drops_full = f - prev_full_drops;
  prev_full_drops = f;
  drops_oversize = os - prev_oversize_drops;
  prev_oversize_drops = os;
  recv_count = en - prev_enqueued;
  prev_enqueued = en;
  parse_errs = pe - prev_parse_errs;
  prev_parse_errs = pe;
}

static void request_stop_and_unblock_io() {
  running.store(false, std::memory_order_release);

  // wake any blocking syscalls in udp receivers/senders
  for (int fd : all_udp_fds) {
    shutdown(fd, SHUT_RD);
  }
}

static void handle_signal_local(int) { request_stop_and_unblock_io(); }

std::fstream receiver_stream;
std::fstream sender_stream;
std::fstream stats_out;

int main() {
  std::string stats_path;
  std::cin >> stats_path;

  // set simulation time-zero before launching any threads
  sim_t0_ns.store(now_ns(), std::memory_order_release);

  std::cout << "Running UDP FIX MPMC Data Pipeline with " << MAX_THREADS
            << " threads...\n";

  open_logs(); // name says it all
  stats_out.open(stats_path, fstream_mode);
  // Just gonna evenly distribute all threads between UDP senders,
  // UDP receivers, and message processors

  ssize_t nThreads = MAX_THREADS;
  ssize_t udp_recv_threads = CUR_MCGRP;
  ssize_t udp_send_threads = CUR_MCGRP;
  nThreads -= udp_recv_threads + udp_send_threads;
  ssize_t message_proc_threads = nThreads;
  nThreads -= message_proc_threads;

  if (message_proc_threads < 1) {
    std::cout << "Insufficient amount of threads...\n";
    return 1;
  }

  std::vector<std::thread> threads;

  for (int i{}; i < udp_send_threads; ++i) {
    threads.emplace_back(udp_sender, multicast_groups[i], i);
  }

  for (int i{}; i < udp_recv_threads; ++i) {
    threads.emplace_back(udp_receiver, multicast_groups[i],
                         i + udp_send_threads);
  }

  for (int i{}; i < message_proc_threads; ++i) {
    threads.emplace_back(msg_processor);
  }

  emit_csv_header_once(udp_recv_threads, udp_send_threads,
                       message_proc_threads);

  // install signal handler to flip `running=false` (declared in
  // udp_multicast.cpp)
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, handle_signal_local);
  std::signal(SIGTSTP, handle_signal_local);
  std::signal(SIGTERM, handle_signal_local);

  auto t0 = std::chrono::steady_clock::now(); // run simulation for fixed time

  std::size_t idx = 0;
  uint64_t win_ns = 100'000'000ull;
  while (running.load(std::memory_order_acquire)) {
    uint64_t cur = cur_shared_window_id.load(std::memory_order_acquire);
    uint64_t last = emitted_window_id.load(std::memory_order_acquire);
    if (cur > last) {
      uint64_t W = last + 1;
      uint64_t ts_start_ns = W * win_ns;     // normalized to sim start
      uint64_t ts_end_ns = (W + 1) * win_ns; // normalized

      Shard to_emit;
      Histo64 q_drain;
      uint64_t drops_full{}, drops_oversize{}, recv_count{}, parse_errs{};

      {
        std::lock_guard<std::mutex> lock(shard_lock);
        active_shared_shard.swap(drained_shared_shard);
        to_emit.swap(drained_shared_shard);
        std::swap(active_qwait, q_drain); // swap histograms
      }

      snapshot_counters(drops_full, drops_oversize, recv_count, parse_errs);

      uint64_t p50{}, p90{}, p99{}, pmax{}, qn{};
      histo_percentiles(q_drain, p50, p90, p99, pmax, qn);

      for (auto &kv : to_emit) {
        const std::string &sym = kv.first;
        const Accum &a = kv.second;
        emit_symbol_row(ts_start_ns, ts_end_ns, W, sym, a, p50, p90, p99, pmax,
                        qn, drops_full, drops_oversize, recv_count, parse_errs,
                        /*partial=*/0);
        idx++;
      }
      emitted_window_id.store(W, std::memory_order_release);
      stats_out.flush();
    }
    auto now = std::chrono::steady_clock::now();
    if (now - t0 >= std::chrono::seconds(SIMULATION_TIME_S)) {
      request_stop_and_unblock_io();

      for (std::thread &th : threads) {
        if (th.joinable()) {
          th.join();
        }
      }

      break;
    }

    // light pause to avoid hot spinning the CPU if nothing to emit
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // ==== shutdown flush ====
  // Last partial window: roll & emit whatever is in
  // active_shared_shard/active_qwait once.
  {
    uint64_t cur = cur_shared_window_id.load(std::memory_order_relaxed);
    uint64_t W = cur; // treat current as final window id
    uint64_t ts_start_ns = W * win_ns;

    // normalized partial end at shutdown
    uint64_t ts_end_ns = now_ns() - sim_t0_ns.load(std::memory_order_acquire);

    Shard to_emit;
    Histo64 q_drain;
    uint64_t drops_full, drops_oversize, recv_count, parse_errs;

    {
      std::lock_guard<std::mutex> lock(shard_lock);
      active_shared_shard.swap(drained_shared_shard);
      to_emit.swap(drained_shared_shard);
      std::swap(active_qwait, q_drain);
    }

    snapshot_counters(drops_full, drops_oversize, recv_count, parse_errs);

    uint64_t p50, p90, p99, pmax;
    uint64_t qn;
    histo_percentiles(q_drain, p50, p90, p99, pmax, qn);

    for (auto &kv : to_emit) {
      emit_symbol_row(ts_start_ns, ts_end_ns, W, kv.first, kv.second, p50, p90,
                      p99, pmax, qn, drops_full, drops_oversize, recv_count,
                      parse_errs, /*partial=*/1);
    }
    stats_out.flush();
  }

  double throughput = successfully_processed.load(std::memory_order_relaxed) *
                      1.0f / SIMULATION_TIME_S;

  receiver_stream.close();
  sender_stream.close();
  stats_out.close();
}
