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
    fprintf(stderr, "%-24s: ERROR @ File %s Line %d: " fmt, \
            __FUNCTION__, __FILE__,                       \
            __LINE__, ##__VA_ARGS__);                     \
    fflush(stderr);                                       \
    exit(ERROR_EXIT_STATUS);                              \
  } while (0);

// Error printing that allows us to specify the exit status
// as the first argument
#define err_printf_status(status, fmt, ...)               \
  do {                                                    \
    fprintf(stderr, "%-24s: ERROR @ File %s Line %d: " fmt, \
            __FUNCTION__, __FILE__,                       \
            __LINE__, ##__VA_ARGS__);                     \
    fflush(stderr);                                       \
    exit(status);                              \
  } while (0);

// Under debug mode, always assert is a normal assert
// Otherwise it is err_printf()
#ifndef NDEBUG
#define always_assert(cond) assert(cond)
#else
// This assert will work even under debug mode
#define always_assert(cond)                                \
  do {                                                     \
    if(!(cond)) err_printf("Assertion \"" #cond "\" fails\n"); \
  } while (0); 
#endif

// If debug flag is turned on then add the statement
// NOTE: Do not add semicolon after this
#define IF_DEBUG(s) #ifndef NDEBUG {s;} #endif
#define IF_NDEBUG(s) #ifdef NDEBUG {s;} #endif