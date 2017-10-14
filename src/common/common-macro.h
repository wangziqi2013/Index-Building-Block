/*
 * common-macro.h - This file implements common macro definitions, usually
 *                  at a high level, which controls compilation or chooses
 *                  environments, etc.
 *
 * Domain-specific macros should be defined in their own domain, and then
 * included by adding the path to compiler
 * 
 * Note that this file should not include any other file in the source tree.
 * Also, do not directly include this file in the source code; include 
 * common.h instead
 */

// Whether to use "wangziqi2013" name space to avoid name collision
// with other libraries
#define USE_WANGZIQI2013_NAMESPACE

// This macro defines the project's name space
#define PROJECT_NAMESPACE index_building_block

// Empty name space
namespace PROJECT_NAMESPACE {}
namespace wangziqi2013 {}

// This should be used with the ending macro as a pair
// If the name space is not used then we just define an empty pair
#ifdef USE_WANGZIQI2013_NAMESPACE
  #define NAMESPACE_WANGZIQI2013 namespace wangziqi2013 {
  #define NAMESPACE_WANGZIQI2013_END }
  #define NAMESPACE_USE_ALL using namespace wangziqi2013; \
                            using namespace PROJECT_NAMESPACE;
#else
  #define NAMESPACE_WANGZIQI2013
  #define NAMESPACE_WANGZIQI2013_END
  #define NAMESPACE_USE_ALL using namespace PROJECT_NAMESPACE;
#endif

// The debug print facility will be disabled if NDEBUG is enabled
// NDEBUG is enabled only under DEBUG mode; Under release node and 
// default mode there is no debug printing
#ifndef NDEBUG
  #define dbg_printf(fmt, ...)                              \
    do {                                                    \
      fprintf(stderr, "%-24s: " fmt, __FUNCTION__, ##__VA_ARGS__); \
      fflush(stderr);                                       \
    } while (0);
#else
  #define dbg_printf(fmt, ...)   \
    do {                         \
    } while (0);
#endif

// This defines the exit status if there is an error
#define ERROR_EXIT_STATUS 1
// Error printing: Always print no matter whether the NDEBUG flag is set
// also, it exits with return number 1 if called
#define err_printf(fmt, ...)                              \
  do {                                                    \
    fprintf(stderr, "%-24s: ERROR @ %d " fmt,             \
            __FUNCTION__, __LINE__, ##__VA_ARGS__);       \
    fflush(stderr);                                       \
    exit(ERROR_EXIT_STATUS);                              \
  } while (0);
  