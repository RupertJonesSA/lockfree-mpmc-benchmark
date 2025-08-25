#include "mpmc_ring.hpp"
#include <iostream>

static constexpr std::size_t MAXN = 1 << 16;

int main() {
  mpmc_ring<int, MAXN> buffer;
  std::cout << "Made buffer...\n";
}
