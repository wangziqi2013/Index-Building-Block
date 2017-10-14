
#include "common.h"
#include "test-util.h"
// We use fork() to test whether err printing works
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>

using namespace wangziqi2013;
using namespace index_building_block;

/*
 * TestDebugPrint() - This function tests whether debug printing works
 * 
 * This should print if MODE=DEBUGl in all other modes there is no printing
 */
void TestDebugPrint() {
  PrintTestName();
  dbg_printf("This is a debug printf\n");
  test_printf("This is a test printf\n");

  return;
}

/*
 * TestErrorPrint() - Tests whether error printing works
 */
void TestErrorPrint() {
  PrintTestName();

  test_printf("Now calling fork() to test err_printf\n");
  pid_t fork_ret = fork();
  // Did not fork, only one process
  if(fork_ret == -1) {
    err_printf("Fork() returned -1; exit\n");
  } else if(fork_ret == 0) {
    // This is child process; it will exit after printing
    test_printf("Child process executing err_printf() with always_assert\n");
    // It will print and exit
    always_assert(0 == 1);
  } else {
    int child_status = 0;
    int exit_pid = waitpid(fork_ret, &child_status, 0);
    if(exit_pid == -1) {
      err_printf("waitpid() returned -1; exit\n");
    } else {
      const int exit_status = WEXITSTATUS(child_status);
      test_printf("Child process %d returns with status %d\n", 
                  exit_pid, exit_status);
      assert(exit_status == ERROR_EXIT_STATUS);
    }
  }

  return;
}

/*
 * TestAlwaysAssert() - This function tests whether always_assert() works as 
 *                      expected even under non-debug mode
 */
void TestAlwaysAssert() {
  #ifdef NDEBUG
  test_printf("Under non-debug mode\n");
  assert(false);
  #else
  test_printf("Under debug mode\n");
  #endif

  test_printf("always_assert true\n");
  always_assert(true);

  return;
}

int main() {
  TestDebugPrint();
  TestErrorPrint();
  TestAlwaysAssert();

  return 0;
}