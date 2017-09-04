
#
# This is the main Makefile
#

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

clean:
	rm -f ./build/*
	rm -f ./bin/*