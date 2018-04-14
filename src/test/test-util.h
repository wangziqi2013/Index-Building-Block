
/*
 * test-util.h - This file contains declarations for test-util.cpp
 *
 * For those definitions that will not be used outside the testing framework
 * please do not include them into common.h. Put them here instead.
 */

#ifndef _TEST_UTIL_H
#define _TEST_UTIL_H

#include "common.h"
#include <thread>

// This defines test printing. Note that this is not affected by the flag
// i.e. It always prints even under debug mode
#define test_printf(fmt, ...)                               \
    do {                                                    \
      fprintf(stderr, "%-24s: " fmt, __FUNCTION__, ##__VA_ARGS__); \
      fflush(stderr);                                       \
    } while(0);

// If called this function prints the current function name
// as the name of the test case - this always prints in any mode
#define PrintTestName() do { \
                          test_printf("=\n"); \
                          test_printf("========== %s ==========\n", \
                                      __FUNCTION__); \
                          test_printf("=\n"); \
                        } while(0);

// This macro adds a new test by declaring the test name and then 
// call the macro to print test name.
// Note that the block must be ended by 2 '}}'
#define BEGIN_TEST(n) void n() { PrintTestName();
// Or use this macro to make it prettier
#define END_TEST }

/*
 * StartThread() - Starts a given number of threads with parameters
 * 
 * The thread function must have the first argument being size_t, which is
 * the current thread ID, starting from 0
 */
template <typename Function, typename... Args>
void StartThread(size_t thread_num, 
                 Function &&fn, 
                 Args &&...args) {
  // Store thread ID here
  std::thread thread_group[thread_num];

  // Launch a group of threads
  for (size_t thread_id = 0; thread_id < thread_num; thread_id++) {
    thread_group[thread_id] = std::thread{fn, thread_id, std::ref(args...)};
  }

  // Join the threads with the main thread
  for (size_t thread_id = 0; thread_id < thread_num; ++thread_id) {
    thread_group[thread_id].join();
  }

  return;
}

#endif