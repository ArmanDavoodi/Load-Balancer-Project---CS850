CXX = g++
CXXFLAGS = -std=c++20 -Iinclude -pthread -g3
LDFLAGS = -pthread

SRC_DIR = src
BUILD_DIR = build
INCLUDE_DIR = include
CONFIG_DIR = $(INCLUDE_DIR)
CONFIG_H = $(CONFIG_DIR)/config.h

TARGET = load_balancer_test

SOURCES = $(addprefix $(SRC_DIR)/, load_info_container.cpp load_balancer.cpp load_balancer_test.cpp)
OBJECTS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SOURCES))

all: $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJECTS)
	@echo "Linking object files..."
	$(CXX) $(LDFLAGS) -o $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR) $(CONFIG_DIR) $(CONFIG_H)
	@echo "Creating object file for $<"
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR):
	@echo "Building build directory..."
	mkdir -p $(BUILD_DIR)

clean:
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR)

.PHONY: all clean