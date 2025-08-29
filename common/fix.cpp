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

std::string Fix_engine::interpret_fix_message(char *msg, std::size_t len) {
  std::array<std::string_view, max_packet_len_> parsed_fix;
  parse_fix_message_(msg, len, parsed_fix);

  std::ostringstream oss;

  for (std::size_t tag{}; tag < max_packet_len_; ++tag) {
    if (parsed_fix[tag].empty())
      continue;

    switch (tag) {
    case 8:
      oss << "FIX Version: " << parsed_fix[tag] << "\n";
      break;
    case 35:
      oss << "Message Type: " << parsed_fix[tag] << "\n";
      break;
    case 49:
      oss << "SenderCompID: " << parsed_fix[tag] << "\n";
      break;
    case 56:
      oss << "TargetCompID: " << parsed_fix[tag] << "\n";
      break;
    case 55:
      oss << "Symbol: " << parsed_fix[tag] << "\n";
      break;
    case 54:
      oss << "Side: " << (parsed_fix[tag] == "1" ? "Buy" : "Sell") << "\n";
      break;
    case 38:
      oss << "Order Quantity: " << parsed_fix[tag] << "\n";
      break;
    case 44:
      oss << "Price: " << parsed_fix[tag] << "\n";
      break;
    case 10:
      oss << "Checksum: " << parsed_fix[tag] << "\n";
      break;
    case 40:
      oss << "OrdType: " << parsed_fix[tag] << "\n";
      break;
    case 11:
      oss << "ClOrdID: " << parsed_fix[tag] << "\n";
      break;
    case 34:
      oss << "MsgSeqNum: " << parsed_fix[tag] << "\n";
      break;
    default:
      break;
    }
  }

  return oss.str();
}

void Fix_engine::parse_fix_message_(
    const char *data, std::size_t len,
    std::array<std::string_view, max_packet_len_> &parsed_fix) {

  std::size_t i{0};

  while (i < len) {
    // find '='
    std::size_t eq = i;
    while (eq < len && data[eq] != '=')
      ++eq;
    if (eq == len)
      break; // malformed

    // parse tag as int
    int tag{0};
    for (std::size_t j = i; j < eq; ++j) {
      char c = data[j];
      if (c < '0' || c > '9') {
        tag = -1;
        break;
      }
      tag = tag * 10 + (c - '0');
    }

    // find '|'
    std::size_t end = eq + 1;
    while (end < len && data[end] != '|')
      ++end;

    if (tag > 0 && tag < max_packet_len_) {
      parsed_fix[tag] = std::string_view(data + eq + 1, end - (eq + 1));
    }

    i = end + 1;
  }
}
