#include "../../include/fix.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <mutex> // avoid thread racing
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#define BUFFER 1024

/* Global mutex for synchronizing console output */
std::mutex console_mutex;

std::atomic<bool> running(true);

void handle_signal(int signal) { running = false; }

struct Multicast_grp {
  std::string ip;
  std::string port;
};

void *udp_receiver(Multicast_grp info) {
  /* added to prevent threads for sending interleaving messages */
  {
    std::lock_guard<std::mutex> lock(console_mutex);
    std::cout << "Establishing connection to multicast group " << info.ip
              << " on port " << info.port << ".\033[0m\n";
  }

  struct sockaddr_in in_addr;
  int sockfd{};

  if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    perror("Failed to create socket");
    return nullptr;
  }

  {
    std::lock_guard<std::mutex> lock(console_mutex);
    std::cout << "OK: created socket for multicast group.\033[0m\n";
  }

  memset(&in_addr, 0, sizeof(in_addr));
  in_addr.sin_family = AF_INET;
  in_addr.sin_port = htons(std::stoi(info.port));
  in_addr.sin_addr.s_addr = INADDR_ANY;

  if (bind(sockfd, (struct sockaddr *)&in_addr, sizeof(in_addr)) < 0) {
    perror("Failed to bind socket to port");
    close(sockfd);
    return nullptr;
  }

  {
    std::lock_guard<std::mutex> lock(console_mutex);
    std::cout << "OK: binded socket to port.\033[0m\n";
  }

  struct ip_mreq mreq;
  memset(&mreq, 0, sizeof(mreq));
  inet_pton(AF_INET, (info.ip).c_str(), &(mreq.imr_multiaddr.s_addr));
  mreq.imr_interface.s_addr = htonl(INADDR_ANY);

  if (setsockopt(sockfd, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char *)&mreq,
                 sizeof(mreq)) < 0) {
    perror("Error joining multicast group");
    close(sockfd);
    return nullptr;
  }

  {
    std::lock_guard<std::mutex> lock(console_mutex);
    std::cout << "OK: joined multicast group.\033[0m\n";
    std::cout << "Listening for packets...\033[0m\n\n";
  }

  char msg[BUFFER];
  ssize_t bytes_recv{};

  /* Object to parse and interpret FIX packets */
  Fix_engine feed;

  std::cout << "=============================================\033[0m\n";
  while (running) {
    bytes_recv = recvfrom(sockfd, msg, BUFFER - 1, 0, NULL, NULL);

    // Interrupted by signal
    if (errno == EINTR)
      break;

    if (bytes_recv < 0) {
      perror("Error receiving packet from multicast sender");
      continue;
    } else if (bytes_recv == 0) {
      std::cout << "Multicast sender disconnected.\033[0m\n";
    }

    msg[bytes_recv] = '\0';
    {
      std::lock_guard<std::mutex> lock(console_mutex);
      feed.interpret_fix_message(msg, bytes_recv);
      std::cout << "Received from " << info.ip << " on port " << info.port
                << ".\033[0m\n";
      std::cout << "=============================================\033[0m\n";
    }
  }

  close(sockfd);

  {
    std::lock_guard<std::mutex> lock(console_mutex);
    std::cout << "Closing mutlicast receiver for group " << info.ip
              << " on port " << info.port << ".\033[0m\n";
  }

  return nullptr;
}

int runReceivers() {
  // signal handler for graceful exiting of process
  signal(SIGINT, handle_signal);

  std::cout << "===== UDP MULTITHREAD RECEIVER =====\n\n";
  std::cout << "Enter amount of multicast networks: ";

  int multi_cnt;
  std::cin >> multi_cnt;

  std::vector<Multicast_grp> multicast(multi_cnt);
  std::vector<std::jthread> threads(multi_cnt);

  for (int i = 0; i < multi_cnt; ++i) {
    std::cout << "\nMutlcast group #" << (i + 1) << "\n";
    std::cout << "-----------------\n";

    std::cout << "Enter ip: ";
    std::cin >> multicast[i].ip;

    std::cout << "Enter port: ";
    std::cin >> multicast[i].port;

    std::cout << "\n";
  }

  {
    /* Create thread and start receiving */
    for (int i{}; i < multi_cnt; ++i) {
      threads.emplace_back(udp_receiver, multicast[i]);
    }

    /* wait for all threads to finish */
  }

  std::cout << "All receivers closed. Exiting program.\n";

  return 0;
}
