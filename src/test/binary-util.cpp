
#include "binary-util.h"

namespace wangziqi2013 {
namespace index_building_block {

/*
 * operator=() - Normal assignment
 */
BitSequence &BitSequence::operator=(const BitSequence &other) {
  // First destroy then copy construct a new one
  this->~BitSequence();
  new (this) BitSequence{other};

  return *this;
}

/*
 * operator=() - Move assignment
 */
BitSequence &BitSequence::operator=(BitSequence &&other) {
  this->~BitSequence();
  data_p = other.data_p; 
  capacity = other.capacity;
  length = other.length;
  other.data_p = nullptr;
  other.capacity = other.length = 0UL;

  return *this;
}

/*
 * Make() - Get a sequence of certain size. If there is any prior data, they
 *          will be freed
 * 
 * The return storage is guaranteed to be zeroed out
 */
void BitSequence::Make(size_t new_size) {
  // If size is 0, just err and exit
  always_assert(new_size != 0UL);
  if(data_p != nullptr) {
    delete[] data_p;
  }

  size_t new_byte_size = ALLOC_SIZE(new_size);

  data_p = new uint8_t[new_byte_size];
  memset(data_p, 0x00, new_byte_size);

  // It is the bit length
  length = new_size;
  // It is the byte we allocate
  capacity = new_byte_size;

  return;
}

/*
 * operator==() - Compares two bit sequence
 */
bool BitSequence::operator==(const BitSequence &other) const {
  if(length != other.length) {
    return false;
  }

  // Number of full bytes (i.e. No partial bit)
  size_t full_byte = BYTE_OFFSET(length);
  // Note that multiple of 8 this will be 0
  size_t unused_bit = UNUSED_BITS(length, sizeof(uint8_t));

  // Compare full bytes using memcmp()
  if(memcmp(data_p, other.data_p, full_byte) != 0) {
    return false;
  }

  // Then compare partial bytes
  for(size_t i = 0;i < unused_bit;i++) {
    if(GetBit(length - i - 1) != other.GetBit(length - i - 1)) {
      return false;
    }
  }

  return true;
}

/*
 * SetBit() - This function sets a given bit in the bit sequence
 * 
 * If the index is not valid, the assertion would fail. The return
 * value indicates the previous state of the bit
 */
bool BitSequence::SetBit(size_t pos, bool value) {
  always_assert(pos < length);

  size_t byte_offset = BYTE_OFFSET(pos);
  size_t bit_offset = BIT_OFFSET(pos);

  uint8_t mask = static_cast<uint8_t>(0x01) << bit_offset;
  // If the bit is 1 this will be true
  bool ret = !!(data_p[byte_offset] & mask);

  if(value == false) {
    data_p[byte_offset] &= (~mask);
  } else {
    data_p[byte_offset] |= mask;
  }

  return ret;
}

/*
 * GetBit() - Returns a bit on given position
 */
bool BitSequence::GetBit(size_t pos) const {
  always_assert(pos < length);
  return !!(data_p[BYTE_OFFSET(pos)] & 
            (static_cast<uint8_t>(0x01) << BIT_OFFSET(pos))
           );
}

/*
 * SetRange() - Sets the a range in the bit sequence given the data
 * 
 * Note that we implicit start from the beginning point of the data. If
 * you wish to start from the middle, then you should shift the data first
 */
void BitSequence::SetRange(size_t range_start, 
                           size_t range_end, 
                           const void *range_data_p) {
  always_assert(range_start < length && range_end <= length);
  size_t range_length = range_end - range_start;

  // Construct a const temp object - note that this will copy the data
  const BitSequence bs{range_length, range_data_p};
  for(size_t i = 0;i < range_length;i++) {
    SetBit(range_start + i, bs.GetBit(i));
  }

  return;
}

/*
 * SetRange() - Sets a range in the bit sequence given a 64 bit integer
 * 
 * Since this specialized version uses 64 bit value as the source, we 
 * also check whether all bits in the 64 bit value has effect; if not
 * we return false, otherwise return true
 */
void BitSequence::SetRange(size_t range_start, 
                           size_t range_end, 
                           uint64_t value) {
  always_assert(range_start < length && range_end <= length);
  
  size_t range_length = range_end - range_start;
  always_assert(range_length <= (sizeof(value) * 8));

  for(size_t i = 0;i < range_length;i++) {
    SetBit(range_start + i, !!(value & 0x1));
    value >>= 1;
  }

  // If there is no active 1 bit in value then this is true; false otherwise
  // This can only detect part of the problem
  return;
}

/*
 * GetRange() - Returns a range specified by the parameter
 */
uint64_t BitSequence::GetRange(size_t range_start, size_t range_end) const {
  always_assert(range_start < length && range_end <= length);
  
  size_t range_length = range_end - range_start;
  always_assert(range_length <= (sizeof(uint64_t) * 8));

  uint64_t ret = 0x0UL;
  for(size_t i = 0;i < range_length;i++) {
    // Must do this before each iteration, o.w. the last bit is always 0
    ret <<= 1;
    bool bit = GetBit(i);
    ret |= bit ? 0x1UL : 0x0UL;
  }

  return ret;
}


/*
 * Print() - Prints the sequence using digit 0 and 1.
 * 
 * We print a space after every group digits, and print a line after
 * every line digits. If line is not a multiple of group we also print
 * a warning
 */
void BitSequence::Print(int group, int line) const {
  always_assert(group >= 1 && group <= line);
  always_assert(length > 0UL);

  size_t current = length - 1;
  size_t count = 0;
  size_t unused_bit = UNUSED_BITS(length, line);

  // Print padding space first
  while(count < unused_bit) {
    putchar(' ');
    count++;
    if(count % group == 0) {
      putchar(' ');
    }
  }

  while(count < length + unused_bit) {
    bool value = GetBit(current);
    putchar(value ? '1' : '0');
    current--;
    count++;

    // First check line, then check whitespace
    if(count % line == 0) {
      putchar('\n');
    } else if(count % group == 0) {
      putchar(' ');
    }
  }

  // If the last line is not finished we must add an extra new line
  if(count % line != 0) {
    putchar('\n');
  }

  return; 
}

/*
 * PrintTitle() - Prints the title of the bit array.
 */
void BitSequence::PrintTitle(int group, int line) {
  if(line % group != 0) {
    dbg_printf("Line (%d) is not a multiple of group (%d)!\n",
               line, group);
  }
  
  for(int i = 0;i < line;i++) {
    if(i % group == 0) {
      putchar('+');
    } else if(i % group == (group - 1)) {
      putchar('+');
      putchar(' ');
    } else {
      putchar('-');
    }
  }

  putchar('\n');

  return;
}

} // namespace index_building_block
} // namespace wangziqi2013