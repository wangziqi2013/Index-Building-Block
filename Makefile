
#
# This is the main Makefile
#

# This should appear before including makefile common
SRC_DIR = ./src
BUILD_DIR = ./build
BIN_DIR = ./bin

# This include the common make file
-include ../common/Makefile-common

$(info Compiling modules...)

.PHONY: all test-all test common clean

all: test-all

test-all: common test

common: 
	$(MAKE) -C ./src/common

test: 
	$(MAKE) -C ./src/test

test-common: common test
	@$(CXX) -o $(BIN_DIR)/$@ $(BUILD_DIR)/*.o ./test/test-common.cpp $(CXXFLAGS) $(LDFLAGS)

clean:
	rm -f ./build/*
	rm -f ./bin/*