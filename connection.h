#pragma once

#include <string>
#include <future>
#include <atomic>
#include <thread>
#include <mutex>
#include <map>
#include <vector>
#include <queue>
#include <memory>
#include <iostream>

#include <infiniband/verbs.h>

#define _DCB_COUT_(x) // std::cout << x
#define WC12_COUT(x) // std::cout << "WMWMW: " << x << std::endl
#define WC_COUT(tag, rank, x) // std::cout << "WC [ tag " << tag << ", rank " << rank << "] " << x << std::endl
#define WRID_COUT(rank, wrid) // std::cout << "[rank " << rank << "] " << wrid << std::endl

namespace ib_ns {

using std::future;
using std::promise;
using std::tuple;

using tag_t = uint64_t;
using ibv_qp_ptr = decltype(ibv_create_qp(NULL, NULL));

struct bytes_t {
  void* data;
  uint64_t size;
};

struct own_bytes_t {
  own_bytes_t():
    ptr(nullptr), size(0)
  {}

  own_bytes_t(bytes_t b):
    ptr((char*)b.data), size(b.size)
  {}

  own_bytes_t(char* data, uint64_t size):
    ptr(data), size(size)
  {}

  std::unique_ptr<char[]> ptr;
  uint64_t size;
};


template <typename T>
bytes_t to_bytes_t(T* ptr, size_t num_t_elems) {
  return {
    .data = (void*)ptr,
    .size = sizeof(T)*num_t_elems
  };
}

using tag_rank_t = tuple<tag_t, int32_t>;

// As an algebraic data type, bbts_message_t looks something like this:
//  type BbtsMessage = (Rank, Tag, Info)
//  data Info =
//       OpenFromSend  Size
//       -- ^ tell the recv node how much memory it needs to recv the message
//     | OpenFromRecv  Addr Size Key
//       -- ^ tell the sending node where it can write to
//     | CloseFromSend
//       -- ^ tell the recv node the data has been written to
//     | FailFromSend
//       -- ^ tell the recv side data will not be written to
//     | FailFromRecv
//       -- ^ tell the send side there is no data to write to
struct bbts_message_t {
  enum message_type { open_send, open_recv, close_send, fail_send, fail_recv };

  message_type type;
  int32_t rank;
  int32_t from_rank;
  // ^ In theory, for any work completion, it's easy to get the from_rank.
  //   However, there was a bug where the from rank computed in this way was incorrect
  //   and it couldn't be determiend why.
  //   Instead, the from rank is also part of the message.
  //   TODO: remove from_rank.
  tag_t tag;
  union {
    struct {
      uint64_t size;
    } open_send;
    struct {
      uint64_t addr;
      uint64_t size;
      uint32_t key;
    } open_recv;
  } m;
};

struct bbts_rdma_write_t {
  uint64_t wr_id;
  void* local_addr;
  uint32_t local_size;
  uint32_t local_key;
  uint64_t remote_addr;
  uint32_t remote_key;
};

struct virtual_send_queue_t;
struct virtual_recv_queue_t;

struct send_item_t;
struct recv_item_t;

using send_item_ptr_t = std::shared_ptr<send_item_t>;
using recv_item_ptr_t = std::shared_ptr<recv_item_t>;

// The connection_t object holds an "infinite" set of queues, each queue
// specified by a tag. The idea is that each queue must only be processing one
// item at a time.
//
// The first num_pinned_tags are not removed from virtual_send_queues or
// virtual_recv_queues when they are unused.
struct connection_t {
  connection_t(
    std::tuple<std::string, int32_t, std::vector<std::string>> items,
    uint64_t num_pinned_tags,
    bool poll = false):
      connection_t(
        std::get<0>(items),
        std::get<1>(items),
        std::get<2>(items),
        num_pinned_tags,
        poll)
  {}

  connection_t(
    std::string dev_name,
    int32_t rank,
    std::vector<std::string> ips,
    uint64_t num_pinned_tags,
    bool poll = false);

  connection_t(connection_t const&) = delete;
  connection_t& operator=(connection_t const&) = delete;

  ~connection_t();

  future<bool> send(int32_t dest_rank, tag_t tag, bytes_t bytes);
  future<bool> send(int32_t dest_rank, tag_t tag, bytes_t bytes, ibv_mr* bytes_mr);

  future<tuple<bool, int32_t, own_bytes_t> > recv(tag_t tag);

  future<tuple<bool, int32_t> > recv_with_bytes(
    tag_t tag, bytes_t bytes);
  future<tuple<bool, int32_t > > recv_with_bytes(
    tag_t tag, bytes_t bytes, ibv_mr* bytes_mr);

  future<tuple<bool, own_bytes_t>> recv_from(int32_t from_rank, tag_t tag);
  future<bool> recv_from_with_bytes(
    int32_t from_rank, tag_t tag, bytes_t bytes);
  future<bool> recv_from_with_bytes(
    int32_t from_rank, tag_t tag, bytes_t bytes, ibv_mr* bytes_mr);

  int32_t get_rank() const { return rank; }
  int32_t get_num_nodes() const { return num_rank; }

  ibv_pd* get_protection_domain() { return protection_domain; }

private:
  void post_send(int32_t dest_rank, bbts_message_t const& msg);

  // "post <do this> <from this>"
  //   post_fail_send comes from a send rank telling a recv rank a fail occured
  //   post_open_recv comes from a recv rank telling a send that a recv object is available
  //   and so on
  void post_open_send(int32_t dest_rank, tag_t tag, uint64_t size);
  void post_close_send(int32_t dest_rank, tag_t tag);
  void post_fail_send(int32_t dest_rank, tag_t tag);
  // only send side does this, recv side is not aware
  void post_rdma_write(int32_t dest_rank, bbts_rdma_write_t const& r);
  // the above post_x_send and post_rdma write are called inside this guy
  friend class virtual_send_queue_t;

  void post_open_recv(
    int32_t dest_rank, tag_t tag,
    uint64_t addr, uint64_t size, uint32_t key);
  void post_fail_recv(int32_t dest_rank, tag_t tag);
  // the boave post_x_recv are called inside this guy
  friend class virtual_recv_queue_t;

private:
  // these will make a virtual queue if there isn't already one at that tag, rank
  virtual_send_queue_t& get_send_queue(tag_t tag, int32_t dest_rank);
  virtual_recv_queue_t& get_recv_queue(tag_t tag, int32_t from_rank);

  void send_to_self(tag_t tag, send_item_ptr_t item);
  void recv_from_self(tag_t tag, recv_item_ptr_t item);
  void set_send_recv_self_items(send_item_ptr_t send_item, recv_item_ptr_t recv_item);

  void handle_work_completion(ibv_wc const& work_completion);
  void process_completion_queue();
  void _poll();

  // find out what queue pair was responsible for this work request
  int32_t get_recv_rank(ibv_wc const& wc) const;

  void _send(int32_t dest_rank, tag_t tag, send_item_ptr_t send_item);
  void _recv(int32_t from_rank, tag_t tag, recv_item_ptr_t recv_item);
  void _recv_anywhere(tag_t tag, recv_item_ptr_t recv_item);

private:
  int32_t rank;
  const bool poll;
  std::thread completion_queue_processor;
  std::atomic<int> destruct_counter;
    // ^ TODO? keep track of destruct threads here
    //         Whenever a send or recv happens, it detaches a threads.
    //         It could happen that in the destructor, there are still floating
    //         threads waiting to happen
  std::mutex _m;

  // virtual send and recv queues
  std::map<tag_rank_t, virtual_send_queue_t> virtual_send_queues;
  std::map<tag_rank_t, virtual_recv_queue_t> virtual_recv_queues;
  // for sending and recving from self
  std::map<tag_t, std::queue<send_item_ptr_t> > self_sends;
  std::map<tag_t, std::queue<recv_item_ptr_t> > self_recvs;

  std::queue<int> available_send_msgs;
  bbts_message_t* send_msgs;
  ibv_mr* send_msgs_mr;

  bbts_message_t* recv_msgs;
  ibv_mr* recv_msgs_mr;

  ibv_context *context;
  ibv_comp_channel* channel;
  ibv_cq *completion_queue;
  ibv_pd *protection_domain;
  ibv_srq *shared_recv_queue;
  std::vector<ibv_qp_ptr> queue_pairs;

  // For each msgs_recv, there should be a recv open. current_recv_msg refers to the
  // msgs_recv index that has just been written to...
  // For exmaple:
  // - connection starts of mainting 1,...,num_recv recv msgs.
  // - the completion queue gets some recv items
  // - the first recv item should be written at msgs_recv[current_recv_msg].
  // - after processing that recv item and reposting msgs_recv[current_recv_msg],
  //   increment current_recv_msg.
  // - the next recv item processed from the completion queue should be written
  //   at msgs_recv[current_recv_msg].
  // - after processing that recv item and reposting it, increment current_recv_msg
  // And so on. except for the case where increment current_recv_msg will lead to
  // n+1. In that case, it should go back to zero.
  int current_recv_msg;

  int32_t num_rank;
  uint32_t num_recv;
  uint32_t num_send_per_qp;
  uint32_t num_write_per_qp;

  uint64_t num_pinned_tags;

  // for each rank except self, contain the number of send wrs left.
  // if this hits zero, you can't post a send to the given location
  std::vector<uint32_t> send_wr_cnts;
  std::vector<std::queue<bbts_message_t> > pending_msgs;

  // same same but for rdma writes
  std::vector<uint32_t> write_wr_cnts;
  std::vector<std::queue<bbts_rdma_write_t> > pending_writes;
};

}
