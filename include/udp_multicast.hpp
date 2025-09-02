#ifndef UDP_MULTICAST_HPP
#define UDP_MULTICAST_HPP

#include "./fix.hpp"
#include "./mpmc_lock_ring.hpp"
#include "./mpmc_ring.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
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

namespace fs = std::filesystem;

#ifndef CUR_MCGRP
#define CUR_MCGRP 7
#endif

#ifndef MAX_RX
#define MAX_RX (1 << 8)
#endif

#ifndef MAXN
#define MAXN (1 << 15)
#endif

#ifndef SEED_OVERRIDE
#define SEED_OVERRIDE 1097ull
#endif

constexpr uint64_t RNG_SEED = SEED_OVERRIDE;

struct Multicast_grp {
  std::string ip;
  std::string port;
};

/*
 * Note for future development; please ensure that the received
 * message is <= MAXN to avoid truncating important info.
 * */
template <std::size_t K> struct Msg {
  uint64_t rx_ns; // timestamp for when received
  uint16_t len;   // payload length (<= MAXN)
  uint16_t flags; // spare
  char data[K];   // payload bytes
};

using Payload_t = Msg<MAX_RX>;
#if defined(USE_LOCK)
using Buffer_t = mpmc_lock_ring<Payload_t, MAXN>;
#else
using Buffer_t = mpmc_ring<Payload_t, MAXN>;
#endif

/* Global mutex for synchronizing console output */
std::mutex sender_io_mutex, receiver_io_mutex;

/* Logging file paths as well as static stream objects */
static constexpr std::string_view receiver_log_path = "./log/received.txt";
static constexpr std::string_view sender_log_path = "./log/sent.txt";

static constexpr std::ios_base::openmode fstream_mode =
    std::fstream::out | std::fstream::trunc;

extern std::fstream receiver_stream;
extern std::fstream sender_stream;
extern std::fstream stats_out;

alignas(CLS) std::atomic<bool> running(true);
alignas(CLS) std::atomic<std::size_t> full_drops; // when Buffer_t is full and
                                                  // cannot process
alignas(
    CLS) std::atomic<std::size_t> oversize_drops; // drops when RX greater than
                                                  // alloted size (MAX_RX)
alignas(CLS) std::atomic<std::size_t> enqueued;

inline Buffer_t msg_ring_buffer;

std::array<int, CUR_MCGRP * 2> all_udp_fds;

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

static void ensure_log_dir() {
  try {
    fs::create_directories("./log");
  } catch (...) {
    std::fprintf(stderr, "ERROR: failed to create ./log\n");
    std::abort();
  }
}

static void open_logs() {
  ensure_log_dir();

  receiver_stream.open(receiver_log_path.data(), fstream_mode);
  sender_stream.open(sender_log_path.data(), fstream_mode);

  if (!receiver_stream.is_open() || !sender_stream.is_open()) {
    std::fprintf(stderr,
                 "ERROR: failed to open receiver/sender logs (cwd=%s)\n",
                 fs::current_path().string().c_str());
    std::abort();
  }
}

void *udp_receiver(Multicast_grp info, int id) {
  /* added to prevent threads for sending interleaving messages */
  std::stringstream oss;
  oss << "Establishing connection to multicast group " << info.ip << " on port "
      << info.port << ".\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  struct sockaddr_in in_addr;
  int sockfd{};

  if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    perror("Failed to create socket");
    return nullptr;
  }

  all_udp_fds[id] = sockfd;

  oss << "OK: created socket for multicast group.\n";
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

  oss << "OK: binded socket to port.\n";
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

  oss << "OK: joined multicast group.\n";
  oss << "Listening for packets...\n\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  char msg[MAX_RX];
  ssize_t bytes_recv{};

  oss << "=============================================\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);

  while (running.load(std::memory_order_relaxed)) {
    bytes_recv = recvfrom(sockfd, msg, MAX_RX - 1, 0, NULL, NULL);

    // Check if we should exit due to shutdown or error
    if (bytes_recv < 0) {
      // Save errno immediately as it can be overwritten
      int saved_errno = errno;

      // Check if we're shutting down
      if (!running.load(std::memory_order_relaxed)) {
        break;
      }

      // Handle specific errors
      if (saved_errno == EINTR || saved_errno == EBADF) {
        // Socket was shut down or interrupted
        break;
      } else if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
        // Timeout occurred, just continue to check running flag
        continue;
      } else {
        oss << "Error receiving packet from multicast sender: "
            << strerror(saved_errno) << "\n";
        writeToFile(oss, receiver_stream, receiver_io_mutex);
        continue;
      }
    } else if (bytes_recv == 0) {
      oss << "Multicast sender disconnected.\n";
      writeToFile(oss, receiver_stream, receiver_io_mutex);
      continue; // Continue instead of falling through
    } else {
      // Process the received message
      msg[bytes_recv] = '\0';

      if (bytes_recv > static_cast<ssize_t>(MAX_RX)) {
        oversize_drops.fetch_add(1, std::memory_order_relaxed);
        continue; // Don't process oversized messages
      }

      Msg<MAX_RX> payload_preprocess{now_ns(),
                                     static_cast<uint16_t>(bytes_recv), 0};
      memcpy(payload_preprocess.data, msg, bytes_recv);

      if (!msg_ring_buffer.try_enqueue(payload_preprocess)) {
        full_drops.fetch_add(1, std::memory_order_relaxed);
      } else {
        enqueued.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  close(sockfd);

  oss << "Closing multicast receiver for group " << info.ip << " on port "
      << info.port << ".\n";
  writeToFile(oss, receiver_stream, receiver_io_mutex);
  receiver_stream.flush();

  return nullptr;
}

int udp_sender(Multicast_grp multicast_grp, int id) {
  int sendfd{};
  ssize_t bytes_sent{};
  struct sockaddr_in out_addr;
  // char ip_present[INET_ADDRSTRLEN];
  std::string msg;
  std::stringstream oss;

  if ((sendfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    perror("socket");
    return 1;
  }

  all_udp_fds[id] = sendfd;

  oss << "OK: Created sender socket.\n";
  writeToFile(oss, sender_stream, sender_io_mutex);

  const auto &[ip, port] = multicast_grp;
  memset(&out_addr, 0, sizeof(struct sockaddr_in));
  out_addr.sin_port = htons(std::stoi(port));
  out_addr.sin_family = AF_INET;
  inet_pton(AF_INET, ip.data(), &(out_addr.sin_addr));
  oss << "Configured multicast group " << ip << " on port " << port << "\n";
  writeToFile(oss, sender_stream, sender_io_mutex);

  oss << "Sending packets to multicast group...\n\n";
  writeToFile(oss, sender_stream, sender_io_mutex);

  /* Object to randomly generate a subset of FIX messages */
  Fix_engine feed;
  feed.set_thread_seed(RNG_SEED);

  while (running.load(std::memory_order_relaxed)) {
    msg = feed.get_fix_message();
    // oss << "Pending (" << strlen(msg.c_str()) << " bytes):\n" << msg << "\n";
    // writeToFile(oss, sender_stream, sender_io_mutex);

    bytes_sent = sendto(sendfd, msg.c_str(), strlen(msg.c_str()), 0,
                        (struct sockaddr *)&out_addr, sizeof(out_addr));
    if (errno == EINTR)
      break;

    if (bytes_sent < 0) {

      perror("sendto");
      oss << "    ERROR: Failed to send.\n";
      writeToFile(oss, sender_stream, sender_io_mutex);
      continue;
    }

    // memset(&ip_present, 0, INET_ADDRSTRLEN);
    // inet_ntop(AF_INET, &(out_addr.sin_addr), ip_present, INET_ADDRSTRLEN);

    // oss << "   OK: Sent (" << bytes_sent << " bytes) to " << ip_present <<
    // ":"
    //     << ntohs(out_addrs[i].sin_port) << ".\n";
    // writeToFile(oss, sender_stream, sender_io_mutex);
  }

  close(sendfd);
  oss << "OK: Closed UDP multicast sender application.\n";
  writeToFile(oss, sender_stream, sender_io_mutex);
  sender_stream.flush();

  return 0;
}

#endif // UDP_MULTICAST_HPP
