
#include "binary-util.h"

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
  size_t unused_bit = (8 - (length % 8)) % 8;

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
  always_assert(range_start < length && range_end < length);
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
bool BitSequence::SetRange(size_t range_start, 
                           size_t range_end, 
                           uint64_t value) {
  always_assert(range_start < length && range_end < length);
  size_t range_length = range_end - range_start;

  for(size_t i = 0;i < range_length;i++) {
    SetBit(range_start + i, !!(value & 0x1));
    value >>= 1;
  }

  // If there is no active 1 bit in value then this is true; false otherwise
  // This can only detect part of the problem
  return !value;
}