#include "../../include/fix.hpp"
#include "../../include/mpmc_lock_ring.hpp"
#include "../../include/mpmc_ring.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex> // avoid thread racing
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

static constexpr uint64_t SEED = 1097;
static constexpr std::size_t MAX_RX = 1 << 8;
static constexpr std::size_t MAXN = 1 << 10;

/*
 * Note for future development; please ensure that the received
 * message is <= MAXN to avoid truncating important info.
 * */
template <std::size_t K> struct Msg {
  uint64_t rx_ns; // from recv to fully processed;
  uint16_t len;   // payload length (<= MAXN)
  uint16_t flags; // spare
  char data[K];   // payload bytes
};

using Payload_t = Msg<MAX_RX>;
using Buffer_t = mpmc_ring<Payload_t, MAXN>;

struct Multicast_grp {
  std::string ip;
  std::string port;
};

/* Global mutex for synchronizing console output */
std::mutex sender_io_mutex, receiver_io_mutex;

/* Logging file paths as well as static stream objects */
static constexpr std::string_view receiver_log_path = "./log/received.txt";
static constexpr std::string_view sender_log_path = "./log/sent.txt";
static constexpr std::ios_base::openmode fstream_mode =
    std::fstream::out | std::fstream::trunc;

static std::fstream receiver_stream(receiver_log_path.data(), fstream_mode);
static std::fstream sender_stream(sender_log_path.data(), fstream_mode);

alignas(CLS) std::atomic<bool> running(true);
alignas(CLS) std::atomic<std::size_t> full_drops; // when Buffer_t is full and
                                                  // cannot process
alignas(
    CLS) std::atomic<std::size_t> oversize_drops; // drops when RX greater than
                                                  // alloted size (MAX_RX)
alignas(CLS) std::atomic<std::size_t> enqueued;

void handle_signal(int signal) { running = false; }

inline uint64_t now_ns() {
  using namespace std::chrono;
  return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch())
      .count();
}

void writeToFile(std::stringstream &msg, std::fstream &file,
                 std::mutex &io_lock) {
  std::lock_guard<std::mutex> lock(io_lock);
  file << msg.rdbuf();    // avoid constructing string O(n) -> O(1)
  msg.str(std::string()); // little trick to avoid reallocation
  msg.clear();
}

static Buffer_t msg_ring_buffer;

void *udp_receiver(Multicast_grp info) {
  /* added to prevent threads for sending interleaving messages */
  std::stringstream oss;
  oss << "Establishing connection to multicast group " << info.ip << " on port "
      << info.port << ".\033[0m\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  struct sockaddr_in in_addr;
  int sockfd{};

  if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    perror("Failed to create socket");
    return nullptr;
  }

  oss << "OK: created socket for multicast group.\033[0m\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  memset(&in_addr, 0, sizeof(in_addr));
  in_addr.sin_family = AF_INET;
  in_addr.sin_port = htons(std::stoi(info.port));
  in_addr.sin_addr.s_addr = INADDR_ANY;

  if (bind(sockfd, (struct sockaddr *)&in_addr, sizeof(in_addr)) < 0) {
    perror("Failed to bind socket to port");
    close(sockfd);
    return nullptr;
  }

  oss << "OK: binded socket to port.\033[0m\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  struct ip_mreq mreq;
  memset(&mreq, 0, sizeof(mreq));
  inet_pton(AF_INET, info.ip.data(), &(mreq.imr_multiaddr.s_addr));
  mreq.imr_interface.s_addr = htonl(INADDR_ANY);

  if (setsockopt(sockfd, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char *)&mreq,
                 sizeof(mreq)) < 0) {
    oss << "Error joining multicast group";
    writeToFile(oss, receiver_stream, receiver_io_mutex);
    close(sockfd);
    return nullptr;
  }

  oss << "OK: joined multicast group.\033[0m\n";
  oss << "Listening for packets...\033[0m\n\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  char msg[MAX_RX];
  ssize_t bytes_recv{};

  /* Object to parse and interpret FIX packets */
  Fix_engine feed;

  oss << "=============================================\033[0m\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  while (running) {
    bytes_recv = recvfrom(sockfd, msg, MAX_RX - 1, 0, NULL, NULL);

    // Interrupted by signal
    if (errno == EINTR)
      break;

    if (bytes_recv < 0) {
      oss << "Error receiving packet from multicast sender";
      writeToFile(oss, receiver_stream, receiver_io_mutex);
      continue;
    } else if (bytes_recv == 0) {
      oss << "Multicast sender disconnected.\033[0m\n";
      writeToFile(oss, receiver_stream, receiver_io_mutex);
    }

    msg[bytes_recv] = '\0';
    if (bytes_recv > MAX_RX) {
      full_drops++;
    }

    Msg<MAX_RX> payload_preprocess{now_ns(), static_cast<uint16_t>(bytes_recv),
                                   0};
    memcpy(payload_preprocess.data, msg, bytes_recv);
    if (!msg_ring_buffer.try_enqueue(payload_preprocess)) {
      full_drops++;
    } else {
      enqueued++;
    }

    // oss << "Received from " << info.ip << " on port " << info.port
    //     << ".\033[0m\n";
    // oss << "=============================================\033[0m\n";
    // writeToFile(oss, receiver_stream, receiver_io_mutex);
  }

  close(sockfd);

  oss << "Closing mutlicast receiver for group " << info.ip << " on port "
      << info.port << ".\033[0m\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  return nullptr;
}

template <std::size_t N_GRPS>
int udp_sender(std::array<Multicast_grp, N_GRPS> &multicast_grps) {
  int sendfd{};
  ssize_t bytes_sent{};
  std::array<struct sockaddr_in, N_GRPS> out_addrs;
  char ip_present[INET_ADDRSTRLEN];
  std::string msg;
  std::stringstream oss;

  std::chrono::milliseconds delay(1000);

  if ((sendfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    perror("socket");
    return 1;
  }

  oss << "OK: Created sender socket.\n";
  writeToFile(oss, sender_stream, sender_io_mutex);

  /* Parse IP:Port pairs from `multicast_grps` */
  std::size_t idx = 0;
  for (const auto [ip, port] : multicast_grps) {
    // std::size_t colon_pos = grp.find(':');
    // if (colon_pos == std::string::npos) {
    //   oss << "Invalid format for multicast entry: " << grp << "\n";
    //   writeToConsole(oss);
    //   return 1;
    // }
    //
    // std::string_view ip = grp.substr(0, colon_pos);
    // std::string_view port = grp.substr(colon_pos + 1);

    struct sockaddr_in out_addr;
    memset(&out_addr, 0, sizeof(struct sockaddr_in));
    out_addr.sin_port =
        htons(std::stoi(port));    // changes byte order to big endian
    out_addr.sin_family = AF_INET; // IPv4
    inet_pton(AF_INET, ip.data(), &(out_addr.sin_addr));

    out_addrs[idx++] = out_addr;

    oss << "Configured multicast group " << ip << " on port " << port << "\n";
    writeToFile(oss, sender_stream, sender_io_mutex);
  }

  oss << "Sending packets to multicast groups...\n\n";
  writeToFile(oss, sender_stream, sender_io_mutex);

  /* Object to randomly generate a subset of FIX messages */
  Fix_engine feed;
  feed.set_thread_seed(SEED);

  while (true) {
    msg = feed.get_fix_message();
    oss << "Pending (" << strlen(msg.c_str()) << " bytes):\n" << msg << "\n";
    writeToFile(oss, sender_stream, sender_io_mutex);
    for (std::size_t i = 0; i < out_addrs.size(); ++i) {
      bytes_sent =
          sendto(sendfd, msg.c_str(), strlen(msg.c_str()), 0,
                 (struct sockaddr *)&out_addrs[i], sizeof(out_addrs[i]));

      if (bytes_sent < 0) {
        if (errno == EINTR)
          break;
        perror("sendto");
        oss << "    ERROR: Failed to send.\n";
        writeToFile(oss, sender_stream, sender_io_mutex);
        continue;
      }

      memset(&ip_present, 0, INET_ADDRSTRLEN);
      inet_ntop(AF_INET, &(out_addrs[i].sin_addr), ip_present, INET_ADDRSTRLEN);

      oss << "   OK: Sent (" << bytes_sent << " bytes) to " << ip_present << ":"
          << ntohs(out_addrs[i].sin_port) << ".\033[0m\n";
      writeToFile(oss, sender_stream, sender_io_mutex);
    }
    std::this_thread::sleep_for(delay);
  }

  close(sendfd);
  oss << "OK: Closed UDP multicast sender application.\n";
  writeToFile(oss, sender_stream, sender_io_mutex);

  return 0;
}
