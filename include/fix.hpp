#ifndef FIX_HPP
#define FIX_HPP

#include <array>
#include <iomanip>
#include <random>
#include <string>
#include <string_view>

class Fix_engine {
public:
  Fix_engine() = default;
  Fix_engine(const Fix_engine &) = delete;
  Fix_engine &operator=(const Fix_engine &) = delete;

  static void set_thread_seed(uint64_t seed);
  std::string get_fix_message() const;
  std::string interpret_fix_message(char *msg, std::size_t len);

private:
  // Data
  static constexpr std::array<std::string_view, 10> symbols_{
      "AAPL", "GOOG", "MSFT",    "TSLA",    "ESZ3",
      "NQZ3", "CLG4", "EUR/USD", "USD/JPY", "WTI"};
  static constexpr std::array<std::string_view, 7> senders_{
      "GS", "MS", "JPM", "BAML", "JS", "2S", "ML"};
  static constexpr std::array<std::string_view, 8> targets_{
      "NASDAQ", "NYSE", "CITADEL", "GETCO", "TSX", "HRT", "LONX", "SGEX"};

  static constexpr int min_qty_ = 1, max_qty_ = 1000;
  static constexpr int min_seq_ = 1, max_seq_ = 1000;
  static constexpr double min_px_ = 100.0, max_px_ = 500.0;
  static constexpr std::size_t max_packet_len_ = 1000;

  // RNG + distributions (no sharing across threads)
  struct TLS {
    std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<int> sym{0, int(symbols_.size() - 1)};
    std::uniform_int_distribution<int> snd{0, int(senders_.size() - 1)};
    std::uniform_int_distribution<int> tgt{0, int(targets_.size() - 1)};
    std::uniform_int_distribution<int> qty{min_qty_, max_qty_};
    std::uniform_int_distribution<int> seq{min_seq_, max_seq_};
    std::uniform_real_distribution<double> px{min_px_, max_px_};
    std::bernoulli_distribution side{0.5};    // 1=buy, 2=sell
    std::bernoulli_distribution ordtype{0.5}; // 1=market, 2=limit
  };

  static TLS &tls(); // thread_local instance

  static std::string checksum_(std::string_view msg); // FIX checksum (3 digits)
  void
  parse_fix_message_(const char *data, std::size_t len,
                     std::array<std::string_view, max_packet_len_> &parsed);
};

#endif // FIX_HPP
