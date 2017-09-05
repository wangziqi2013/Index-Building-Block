
#
# This is the main Makefile
#

# This should appear before including makefile common
SRC_DIR = ./src
BUILD_DIR = ./build
BIN_DIR = ./bin

# This include the common make file
-include ./src/common/Makefile-common

$(info Compiling modules...)

.PHONY: all test-all test common clean prepare

all: test-all

test-all: common test

COMMON_OBJ = $(patsubst ./src/common/%.cpp, $(BUILD_DIR)/%.o, $(wildcard ./src/common/*.cpp))
common: 
	$(MAKE) -C ./src/common

TEST_OBJ = $(patsubst ./src/test/%.cpp, $(BUILD_DIR)/%.o, $(wildcard ./src/test/*.cpp))
test: 
	$(MAKE) -C ./src/test

test-common: common test
	$(CXX) -o $(BIN_DIR)/$@ $(COMMON_OBJ) $(TEST_OBJ) ./test/test-common.cpp $(CXXFLAGS) $(LDFLAGS)
	# Make a shortcut of the binary
	$(LN) -sf $(BIN_DIR)/$@ ./$@-bin

clean:
	$(RM) -f ./build/*
	$(RM) -f ./bin/*

prepare:
	$(MKDIR) -p build
	$(MKDIR) -p bin
