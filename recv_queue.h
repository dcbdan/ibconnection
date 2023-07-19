#pragma once

#include <variant>

#include "connection.h"
#include "mr_bytes.h"

namespace ib_ns {

struct recv_item_t {
  explicit recv_item_t(
    memory_region_bytes_t && b,
    promise<tuple<bool, int32_t, own_bytes_t>> && pr):
      bytes(std::move(b)), pr(std::move(pr)),
      which_state(state::wait_open_send)
  {}
  explicit recv_item_t(
    memory_region_bytes_t && b,
    promise<tuple<bool, int32_t>>              && pr):
      bytes(std::move(b)), pr(std::move(pr)),
      which_state(state::wait_open_send)
  {}
  explicit recv_item_t(
    memory_region_bytes_t && b,
    promise<tuple<bool, own_bytes_t>>          && pr):
      bytes(std::move(b)), pr(std::move(pr)),
      which_state(state::wait_open_send)
  {}
  explicit recv_item_t(
    memory_region_bytes_t && b,
    promise<bool>                              && pr):
      bytes(std::move(b)), pr(std::move(pr)),
      which_state(state::wait_open_send)
  {}


  // the bytes and the memory region and the relevant ownereship thereof
  memory_region_bytes_t bytes;

  // There are multiple futures one can get. If this recv item is attached
  // to a specific queue, then the future will not include the rank and
  // pr will refer to bytes or success. If this recv item is attached
  // to any queue, then the recv queue that advances the state from
  // opening send comes to own it
  std::variant<
    std::monostate,                             // does not have an associated promise,
                                                // which state should be waiting recv
    promise<tuple<bool, int32_t, own_bytes_t>>, // rank_bytes
    promise<tuple<bool, int32_t>>,              // rank_success
    promise<tuple<bool, own_bytes_t>>,          // bytes
    promise<bool>                               // success
  > pr;
  enum which_pr { no_pr, rank_bytes, rank_success, just_bytes, just_success };

  void set_fail(int32_t rank);
  void set_success(int32_t rank);

  // communication states
  //    -- waiting open send
  //    -- waitin to post open recv
  //    -- an open recv has posted,
  //    -- an open recv has completed
  //    -- a fail has posted
  //    -- a close recvd
  //    -- another recv acquired this item
  //    -- a fail has completed
  enum state {
    wait_open_send,
    wait_post,
    post_recv,
    post_fail
  } which_state;

  int64_t sz; // after waiting open send, this is set
};

using recv_item_ptr_t = std::shared_ptr<recv_item_t>;

struct virtual_recv_queue_t {
  virtual_recv_queue_t(connection_t* connection, int32_t rank, tag_t tag):
    connection(connection), rank(rank), tag(tag)
  {}

  void insert_item(recv_item_ptr_t item);

  void recv_open_send(int64_t sz);
  void recv_close_send();
  void recv_fail_send();

  void completed_open_recv();
  void completed_fail_recv();

  bool empty() const;
  size_t size() const;
private:
  void process_next();
  void post_open_recv();

  recv_item_t* get_head(recv_item_t::state correct_state);

  // TODO: remove state from recv_item_t and
  //       just have a single state for the head of
  //       in_process_items

  // these items are owned by this
  std::queue<recv_item_ptr_t> in_process_items;

  // these items may or may not be owned by this and
  // must be acquired before being put into in_process_items
  std::queue<recv_item_ptr_t> waiting_open_send_items;

  std::queue<int64_t> pending_sizes;

  connection_t* connection;
  int32_t rank;
  tag_t tag;
};

}
