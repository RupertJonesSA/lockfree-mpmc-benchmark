#include "../include/fix.hpp"

Fix_engine::TLS &Fix_engine::tls() {
  static thread_local TLS t{};
  return t;
}

void Fix_engine::set_thread_seed(uint64_t seed) { tls().rng.seed(seed); }

std::string Fix_engine::checksum_(std::string_view msg) {
  unsigned sum = 0;
  for (unsigned char c : msg)
    sum += c;
  std::ostringstream oss;
  oss << std::setw(3) << std::setfill('0') << (sum % 256);
  return oss.str();
}

std::string Fix_engine::get_fix_message() const {
  auto &L = tls();

  const auto &symbol = symbols_[L.sym(L.rng)];
  const auto &sender = senders_[L.snd(L.rng)];
  const auto &target = targets_[L.tgt(L.rng)];

  int qty = L.qty(L.rng);
  double px = L.px(L.rng);
  int seq = L.seq(L.rng);
  bool is_buy = L.side(L.rng);
  bool is_market = L.ordtype(L.rng);

  std::ostringstream oss;

  // Using '|' as delimiter for readability; FIX uses SOh '\x01'
  oss << "8=FIX.4.2|32=D|34=" << seq << "|49=" << sender << "|56=" << target
      << "|11=Order" << seq << "|55=" << symbol
      << "|54=" << (is_buy ? "1" : "2") << "|38=" << qty
      << "|40=" << (is_market ? "1" : "2") << "|44=" << std::fixed
      << std::setprecision(2) << px << "|";

  oss << "10=" << checksum_(oss.str()) << "|";
  return oss.str();
}

Fix_engine::Instrument_stats
Fix_engine::interpret_fix_message(char *msg, std::size_t len) {
  Instrument_stats relevant_info; // sym, qty, px, side

  enum relevant_tag { SYM = 55, SIDE = 54, QTY = 38, PX = 44 };

  std::size_t i{0};

  while (i < len) {
    // find '='
    std::size_t eq = i;
    while (eq < len && msg[eq] != '=')
      ++eq;
    if (eq == len)
      break; // malformed

    // parse tag as int
    int tag{};
    std::from_chars(msg + i, msg + eq, tag);

    // find '|'
    std::size_t end = eq + 1;
    while (end < len && msg[end] != '|')
      ++end;

    switch (tag) {
    case SYM:
      relevant_info.sym = std::string_view(msg + eq + 1, end - (eq + 1));
      break;

    case SIDE:
      std::from_chars(msg + eq + 1, msg + end, relevant_info.side);
      break;

    case QTY:
      std::from_chars(msg + eq + 1, msg + end, relevant_info.qty);
      break;

    case PX:
      std::from_chars(msg + eq + 1, msg + end, relevant_info.px);
      break;

    default:
      break;
    }

    i = end + 1;
  }

  return relevant_info;
}
