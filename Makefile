
#
# This is the main Makefile
#

# This should appear before including makefile common
SRC_DIR = ./src
BUILD_DIR = ./build
BIN_DIR = ./bin

# We print the mode fir the first invocation, but 
# not the following
MODE_PRINT = 1

# This include the common make file
-include ./src/common/Makefile-common

$(info = Invoking the main compilation dispatcher...)

$(info = CXXFLAGS: $(CXXFLAGS))
$(info = LDFLAGS: $(LDFLAGS))

.PHONY: all test-all test common clean prepare

all: test-all

test-all: common test

# Note that calling make with -C will also pass the environmental variable set inside and by
# the shell to the subprocess of make

COMMON_OBJ = $(patsubst ./src/common/%.cpp, $(BUILD_DIR)/%.o, $(wildcard ./src/common/*.cpp))
common: 
	@$(MAKE) -s -C ./src/common

TEST_OBJ = $(patsubst ./src/test/%.cpp, $(BUILD_DIR)/%.o, $(wildcard ./src/test/*.cpp))
test: 
	@$(MAKE) -s -C ./src/test

test-common: common test ./test/test-common.cpp
	$(info >>> Building binary for test-common)
	$(CXX) -o $(BIN_DIR)/$@ $(COMMON_OBJ) $(TEST_OBJ) ./test/test-common.cpp $(CXXFLAGS) $(LDFLAGS)
	@$(LN) -sf $(BIN_DIR)/$@ ./$@-bin

test-binary-util: common test ./test/test-binary-util.cpp
	$(info >>> Building binary for test-binary-util)
	$(CXX) -o $(BIN_DIR)/$@ $(COMMON_OBJ) $(TEST_OBJ) ./test/test-binary-util.cpp $(CXXFLAGS) $(LDFLAGS)
	@$(LN) -sf $(BIN_DIR)/$@ ./$@-bin


clean:
	$(RM) -f ./build/*
	$(RM) -f ./bin/*
	$(RM) -f *-bin

prepare:
	$(MKDIR) -p build
	$(MKDIR) -p bin
