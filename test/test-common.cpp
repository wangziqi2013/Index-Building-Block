
#include "common.h"
#include "test-util.h"

/*
 * TestDebugPrint() - This function tests whether debug printing works
 */
void TestDebugPrint() {
  PrintTestName();
  test_printf("This is a test printf\n");

  return;
}

int main() {
  TestDebugPrint();
  return 0;
}