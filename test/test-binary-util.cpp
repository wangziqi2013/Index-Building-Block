
#include "binary-util.h"
#include "test-util.h"

using namespace wangziqi2013;
using namespace index_building_block;

void TestSetGet() {
  PrintTestName();

  uint64_t value = 0x123456789ABCDEF0;
  constexpr size_t length = sizeof(value) * 8;

  BitSequence bs1{length, &value};
  BitSequence bs2{value};
  // Empty
  BitSequence bs3{}, bs4{};
  bs3.Make(length);
  bs4 = bs3;

  bs3.SetRange(0, length, &value);
  bs4.SetRange(0, length / 2, value);
  bs4.SetRange(length / 2, length, value >> (length / 2));
  
  BitSequence::PrintTitle();
  bs1.Print();
  BitSequence::PrintTitle();
  bs2.Print();
  BitSequence::PrintTitle();
  bs3.Print();
  BitSequence::PrintTitle();
  bs4.Print();

  always_assert(bs1 == bs2);
  always_assert(bs2 == bs3);
  always_assert(bs3 == bs4);

  return;
}



int main() {
  TestSetGet();
  return 0;
}