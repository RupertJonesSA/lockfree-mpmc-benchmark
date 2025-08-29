CXX := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -O2 -pthread
DEBUG_FLAGS := -g -DDEBUG -fsanitize=thread
RELEASE_FLAGS := -O3 -DNDEBUG -march=native
VALGRIND_FLAGS := --leak-check=full --show-leak-kinds=all \
                  --track-origins=yes --num-callers=50 \
                  --error-exitcode=1

RAW_BUILD_DIR := build/raw
APP_BUILD_DIR := build/app
RAW_TARGET := raw_test.out
APP_TARGET := app_test.out

RAW_SOURCES := ./src/raw-benchmark/main.cpp
APP_SOURCES := ./src/packet-benchmark/udp_mult_recv.cpp ./src/packet-benchmark/udp_mult_sender.cpp ./src/packet-benchmark/main.cpp
COMMON_SOURCES := ./common/performance.cpp ./common/fix.cpp
HEADERS := ./include/mpmc_ring.hpp ./include/mpmc_lock_ring.hpp ./include/performance.hpp ./include/fix.hpp

RAW_OBJECTS := $(RAW_SOURCES:./src/raw-benchmark/%.cpp=$(RAW_BUILD_DIR)/%.o) \
               $(COMMON_SOURCES:./common/%.cpp=$(RAW_BUILD_DIR)/%.o)
APP_OBJECTS := $(APP_SOURCES:./src/packet-benchmark/%.cpp=$(APP_BUILD_DIR)/%.o) \
               $(COMMON_SOURCES:./common/%.cpp=$(APP_BUILD_DIR)/%.o)

.PHONY: all clean debug release raw app help memory

all: $(RAW_TARGET) $(APP_TARGET)

$(RAW_TARGET): $(RAW_OBJECTS) | $(RAW_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(RAW_OBJECTS) -o $@

$(APP_TARGET): $(APP_OBJECTS) | $(APP_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(APP_OBJECTS) -o $@

# Object file rules
$(RAW_BUILD_DIR)/%.o: ./src/raw-benchmark/%.cpp $(HEADERS) | $(RAW_BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(APP_BUILD_DIR)/%.o: ./src/packet-benchmark/%.cpp $(HEADERS) | $(APP_BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(RAW_BUILD_DIR)/%.o: ./common/%.cpp $(HEADERS) | $(RAW_BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(APP_BUILD_DIR)/%.o: ./common/%.cpp $(HEADERS) | $(APP_BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Create build dirs
$(RAW_BUILD_DIR):
	mkdir -p $(RAW_BUILD_DIR)

$(APP_BUILD_DIR):
	mkdir -p $(APP_BUILD_DIR)

debug: CXXFLAGS += $(DEBUG_FLAGS)
debug: clean all

release: CXXFLAGS += $(RELEASE_FLAGS)
release: clean all

raw: $(RAW_TARGET)
	./$(RAW_TARGET)

app: $(APP_TARGET)
	./$(APP_TARGET)

clean:
	rm -rf $(RAW_BUILD_DIR) $(APP_BUILD_DIR) $(RAW_TARGET) $(APP_TARGET)

memory: CXXFLAGS += $(DEBUG_FLAGS)
memory: clean $(RAW_TARGET)
	valgrind $(VALGRIND_FLAGS) ./$(RAW_TARGET)

help:
	@echo "Available targets:"
	@echo "  all       - Build both programs (default)"
	@echo "  debug     - Build with debug flags and thread sanitizer"
	@echo "  release   - Build with full optimizations"
	@echo "  raw       - Build and run the raw benchmark"
	@echo "  app       - Build and run the packet benchmark"
	@echo "  clean     - Remove all build artifacts"
	@echo "  memory    - Run raw benchmark under Valgrind"
	@echo "  help      - Show this help message"
