/*
 * common-macro.h - This file implements common macro definitions, usually
 *                  at a high level, which controls compilation or chooses
 *                  environments, etc.
 *
 * Domain-specific macros should be defined in their own domain, and then
 * included by adding the path to compiler
 */

// Whether to use "wangziqi2013" name space to avoid name collision
// with other libraries
#define USE_WANGZIQI2013_NAMESPACE

// This macro defines the project's name space
#define PROJECT_NAMESPACE index_building_block

// This should be used with the ending macro as a pair
// If the name space is not used then we just define an empty pair
#ifdef USE_WANGZIQI2013_NAMESPACE
  #define OPTIONAL_NAMESPACE_WANGZIQI2013 namespace wangziqi2013 {
  #define NAMESPACE_WANGZIQI2013_END }
  #define NAMESPACE_USE_ALL using wangziqi2013; \
                            using PROJECT_NAMESPACE;
#else
  #define OPTIONAL_NAMESPACE_WANGZIQI2013
  #define NAMESPACE_WANGZIQI2013_END
  #define NAMESPACE_USE_ALL using PROJECT_NAMESPACE;
#endif

// This flag defines debug printing inside the source code
// Note: This should not be used for test printing
#define DEBUG_PRINT

#ifdef DEBUG_PRINT
  #define dbg_printf(fmt, ...)                              \
    do {                                                    \
      fprintf(stderr, "%-24s: " fmt, __FUNCTION__, ##__VA_ARGS__); \
      fflush(stderr);                                       \
    } while (0);
#else
  static void dummy(const char*, ...) {}
  #define dbg_printf(fmt, ...)   \
    do {                         \
      dummy(fmt, ##__VA_ARGS__); \
    } while (0);
#endif