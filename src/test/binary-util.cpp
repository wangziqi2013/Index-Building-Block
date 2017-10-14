
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