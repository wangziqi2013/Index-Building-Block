
/*
 * test-util.h - This file contains declarations for test-util.cpp
 */

#ifndef _TEST_UTIL_H
#define _TEST_UTIL_H

#include "common.h"
NAMESPACE_USE_ALL

// This defines test printing. Note that this is not affected by the flag
#define test_printf(fmt, ...)                               \
    do {                                                    \
      fprintf(stderr, "%-24s: " fmt, __FUNCTION__, ##__VA_ARGS__); \
      fflush(stderr);                                       \
    } while (0);

// If called this function prints the current function name
// as the name of the test case
#define PrintTestName() do { \
                          dbg_printf("=\n"); \
                          dbg_printf("========== %s ==========\n", \
                                     __FUNCTION__); \
                          dbg_printf("=\n"); \
                        }

#endif