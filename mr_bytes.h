#pragma once

#include "connection.h"

namespace ib_ns {

struct memory_region_bytes_t {
  memory_region_bytes_t(memory_region_bytes_t const&) = delete;
  memory_region_bytes_t& operator=(memory_region_bytes_t const&) = delete;

  memory_region_bytes_t(memory_region_bytes_t && other);

  ~memory_region_bytes_t();

  memory_region_bytes_t():
    bytes_mr(nullptr),
    own_bytes(true), own_mr(true)
  {}

  memory_region_bytes_t(bytes_t bytes):
    bytes(bytes), bytes_mr(nullptr),
    own_bytes(false), own_mr(true)
  {}

  memory_region_bytes_t(bytes_t bytes, ibv_mr *bytes_mr):
    bytes(bytes), bytes_mr(bytes_mr),
    own_bytes(false), own_mr(false)
  {}

  bytes_t get_bytes();

  own_bytes_t extract_bytes();

  bool setup_bytes(uint32_t size);
  bool setup_bytes_and_mr(uint32_t size, connection_t* connection, int mr_flags);
  bool setup_mr(connection_t* connection, int mr_flags);

  uint64_t get_addr() const {
    return (uint64_t)bytes.data;
  }
  uint64_t get_size() const {
    return bytes.size;
  }
  uint32_t get_remote_key() const {
    return bytes_mr->rkey;
  }
  uint32_t get_local_key() const {
    return bytes_mr->lkey;
  }
private:
  bytes_t bytes;
  ibv_mr *bytes_mr;

  bool own_bytes;
  bool own_mr;
};

}
