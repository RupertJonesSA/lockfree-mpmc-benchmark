#include "../../include/fix.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

alignas(64) std::atomic<bool> running(true);
static constexpr uint64_t SEED = 1097;

static const std::array<std::string, 7> textColors{
    "\033[91m", // Red
    "\033[92m", // Green
    "\033[94m", // Blue
    "\033[97m", // Yellow
    "\033[95m", // Magenta
    "\033[96m", // cyan
    "\033[90m", // grey
};

int runSender(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <IP1:Port1> <IP2:Port2> ...\n";
    return 1;
  }

  std::cout << "===== UDP MUTLICAST SENDER =====\n\n";

  int sendfd;
  ssize_t bytes_sent;
  std::vector<struct sockaddr_in> out_addrs;
  char ip_present[INET_ADDRSTRLEN];
  std::string msg;

  std::chrono::milliseconds delay(1000); // so I can see

  if ((sendfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    perror("socket");
    return 1;
  }

  std::cout << "OK: Created socket.\n";

  /* Parse command-line arguements for IP:Port pairs */
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    size_t colon_pos = arg.find(':');
    if (colon_pos == std::string::npos) {
      std::cerr << "Invalid format for argument: " << arg << "\n";
      std::cerr << "Expected: <IP>:<Port>\n";
      return 1;
    }

    std::string ip = arg.substr(0, colon_pos);
    std::string port = arg.substr(colon_pos + 1);

    struct sockaddr_in out_addr;
    memset(&out_addr, 0, sizeof(out_addr));
    out_addr.sin_port = htons(std::stoi(port));
    out_addr.sin_family = AF_INET;
    inet_pton(AF_INET, ip.c_str(), &(out_addr.sin_addr));

    out_addrs.push_back(out_addr);
    std::cout << "Configured multicast group " << ip << " on port " << port
              << ".\n";
  }

  std::cout << "Sending packets to multicast groups...\n\n";

  /* Object to randomly generate a subset of FIX messages */
  Fix_engine feed;
  feed.set_thread_seed(SEED);

  while (running) {
    msg = feed.get_fix_message();
    std::cout << "Pending (" << strlen(msg.c_str()) << " bytes):\n"
              << msg << "\n";
    for (int i = 0; i < (int)out_addrs.size(); ++i) {
      bytes_sent =
          sendto(sendfd, msg.c_str(), strlen(msg.c_str()), 0,
                 (struct sockaddr *)&out_addrs[i], sizeof(out_addrs[i]));

      if (bytes_sent < 0) {
        if (errno == EINTR)
          break;
        perror("sendto");
        std::cout << "    ERROR: Failed to send.\n";
        continue;
      }

      memset(&ip_present, 0, INET_ADDRSTRLEN);
      inet_ntop(AF_INET, &(out_addrs[i].sin_addr), ip_present, INET_ADDRSTRLEN);

      std::cout << textColors[i % (int)textColors.size()] << "   OK: Sent ("
                << bytes_sent << " bytes) to " << ip_present << ":"
                << ntohs(out_addrs[i].sin_port) << ".\033[0m\n";
    }
    std::this_thread::sleep_for(delay);
  }

  close(sendfd);
  std::cout << "OK: Closed UDP multicast sender application.\n";
  return 0;
}
