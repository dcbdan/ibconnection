#include "mr_bytes.h"

namespace ib_ns {

memory_region_bytes_t::memory_region_bytes_t(memory_region_bytes_t && other):
  bytes(other.bytes), bytes_mr(other.bytes_mr),
  own_bytes(other.own_bytes), own_mr(other.own_mr)
{
  other.own_bytes = false;
  other.own_mr = false;
}

memory_region_bytes_t::~memory_region_bytes_t() {
  if(own_mr && bytes_mr) {
    ibv_dereg_mr(bytes_mr);
  }
  if(own_bytes && bytes.data) {
    delete[] (char*)(bytes.data);
  }
}

bytes_t memory_region_bytes_t::get_bytes() {
  return bytes;
}

own_bytes_t memory_region_bytes_t::extract_bytes() {
  if(!own_bytes) {
    throw std::runtime_error("cannot extract bytes");
  }
  own_bytes = false;
  return own_bytes_t(bytes);
}

bool memory_region_bytes_t::setup_bytes(uint32_t size)
{
  // It is an error for this function to be called more than once
  if(own_bytes) {
    try {
      bytes.data = (void*)(new char[size]);
      bytes.size = size;
    } catch(const std::bad_alloc& e) {
      return false;
    }
  } else {
    if(!bytes.data || bytes.size < size) {
      // either a nullptr or not enough bytes were provided
      return false;
    }
  }
  return true;
}

bool memory_region_bytes_t::setup_bytes_and_mr(
  uint32_t size,
  connection_t* connection,
  int mr_flags)
{
  if(!setup_bytes(size)) {
    return false;
  }
  return setup_mr(connection, mr_flags);
}

bool memory_region_bytes_t::setup_mr(connection_t* connection, int mr_flags) {
  // It is an error for this function to be called more than once

  if(!bytes.data) {
    return false;
  }

  if(own_mr) {
    bytes_mr = ibv_reg_mr(
      connection->get_protection_domain(),
      bytes.data, bytes.size,
      mr_flags);
    if(!bytes_mr) {
      return false;
    }
  } else {
    // verify the flags we have include those in mr_flags
    // TODO
  }
  return true;
}

}
