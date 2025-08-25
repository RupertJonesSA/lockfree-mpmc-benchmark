CXX := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -O2 -pthread
DEBUG_FLAGS := -g -DDEBUG -fsanitize=thread
RELEASE_FLAGS := -03 -DNDEBUG -march=native

SRC_DIR := .
BUILD_DIR := build
TARGET := test.out

SOURCES := main.cpp performance.cpp
HEADERS := mpmc_ring.hpp mpmc_lock_ring.hpp performance.hpp
OBJECTS := $(SOURCES:%.cpp=$(BUILD_DIR)/%.o)

.PHONY: all clean debug release run help

all: $(TARGET)

# Main build
$(TARGET): $(OBJECTS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(OBJECTS) -o $@

# Object file compilation
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Create build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

debug: CXXFLAGS += $(DEBUG_FLAGS)
debug: clean $(TARGET)

run: $(TARGET)
	./$(TARGET) 

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

# Help target
help:
	@echo "Available targets:"
	@echo "  all     - Build the program (default)"
	@echo "  debug   - Build with debug flags and thread sanitizer"
	@echo "  release - Build with full optimizations"
	@echo "  run     - Build and run the program"
	@echo "  clean   - Remove all build artifacts"
	@echo "  help    - Show this help message"
