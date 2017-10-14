
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

  size_t new_byte_size = (new_size + 7) / 8;

  data_p = new uint8_t[new_byte_size];
  memset(data_p, 0x00, new_byte_size);

  // It is the bit length
  length = new_size;

  return;
}