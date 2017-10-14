
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

  BitSequence bs5{}, bs6{length - 10, &value};
  // length 54
  bs5.Make(length - 10);
  bs5.SetRange(15, 53, value >> 15);
  bs5.SetRange(53, 54, value >> 53);
  bs5.SetRange(0, 7, value);
  bs5.SetRange(7, 15, value >> 7);

  BitSequence::PrintTitle();
  bs5.Print();
  BitSequence::PrintTitle();
  bs6.Print();
  always_assert(bs5 == bs6);

  return;
}



int main() {
  TestSetGet();
  return 0;
}