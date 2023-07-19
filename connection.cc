#include "connection.h"
#include "send_queue.h"
#include "recv_queue.h"
#include "mr_bytes.h"

#include "utils.h"

#include <random>
#include <iostream>

#include <errno.h>

namespace ib_ns {

struct bbts_dest_t {
  uint16_t lid;
  uint32_t qpn;
  uint32_t psn;
};

// TODO: it'd really be better to have a set of qps dedicated to just rdma writes.
// Now rdma writes are shared with the qps that also send messages
struct bbts_context_t {
  ibv_context  *context;
  ibv_pd    *pd;
  ibv_comp_channel *ch;
  ibv_cq    *cq;
  std::vector<ibv_qp_ptr> qps;
  ibv_srq   *srq;
  ibv_port_attr portinfo;

  bbts_message_t *recv_msgs;
  ibv_mr *recv_msgs_mr;

  bbts_message_t *send_msgs;
  ibv_mr *send_msgs_mr;

  uint8_t num_rank;
  uint32_t num_send_per_qp; // the number of send messages that can be added to each
                            // queue pair
  uint32_t num_write_per_qp; // the number of rmda writes that can be added to each
                             // queue pair

  uint32_t get_num_recv() const {
    // If every other node sends as many messages as they can here,
    // that is how many num_recv there must be
    return get_num_qp()*num_send_per_qp;
  }
  uint32_t get_num_total_send() const {
    // This is the same as num_recv
    return get_num_qp()*num_send_per_qp;
  }
  uint8_t get_num_qp() const {
    // there is one qp for every rank except this rank
    return num_rank - 1;
  }
  uint32_t get_num_total_write() const {
    return get_num_qp()*num_write_per_qp;
  }
};

void bbts_post_rdma_write(
  ibv_qp *qp,
  uint64_t wr_id,
  void* local_addr, uint32_t local_size, uint32_t local_key,
  uint64_t remote_addr, uint32_t remote_key)
{
  ibv_sge list = {
    .addr  = (uint64_t)local_addr,
    .length = local_size,
    .lkey  = local_key
  };
  ibv_send_wr wr = {
    .wr_id      = wr_id,
    .sg_list    = &list,
    .num_sge    = 1,
    .opcode     = IBV_WR_RDMA_WRITE,
    .send_flags = IBV_SEND_SIGNALED,
    .wr = {
      .rdma = {
        .remote_addr = remote_addr,
        .rkey        = remote_key
      }
    }
  };

  ibv_send_wr *bad_wr;

  if(errno = ibv_post_send(qp, &wr, &bad_wr)) {
    perror("ibv_post_send for rdma write ");
    throw std::runtime_error("ibv_post_send");
  }
}

void bbts_post_rdma_write(
  ibv_qp *qp,
  bbts_rdma_write_t const& r)
{
  bbts_post_rdma_write(
    qp,
    r.wr_id,
    r.local_addr, r.local_size, r.local_key,
    r.remote_addr, r.remote_key);
}

void bbts_post_send_message(
  ibv_qp *qp,
  uint64_t wr_id,
  uint32_t local_key,
  bbts_message_t const& msg)
{
  ibv_sge list = {
    .addr  = (uint64_t)(&msg),
    .length = sizeof(bbts_message_t),
    .lkey = local_key
  };
  ibv_send_wr wr = {
    .wr_id      = wr_id,
    .sg_list    = &list,
    .num_sge    = 1,
    .opcode     = IBV_WR_SEND,
    .send_flags = IBV_SEND_SIGNALED
  };
  // INLINE:   copy the buffer, making the buffer immediately available
  // SIGNALED: make sure a work completion gets added to the completion queue

  ibv_send_wr *bad_wr;

  if(errno = ibv_post_send(qp, &wr, &bad_wr)) {
    perror("FAILED TO POST SEND MESSAGE ");
    throw std::runtime_error("ibv_post_send");
  }
}

void bbts_post_recv_message(ibv_srq *srq, uint32_t local_key, bbts_message_t& msg)
{
  ibv_sge list = {
    .addr  = (uint64_t)(&msg),
    .length = sizeof(bbts_message_t),
    .lkey  = local_key
  };
  ibv_recv_wr wr = {
    .wr_id      = 0,
    .sg_list    = &list,
    .num_sge    = 1,
  };
  ibv_recv_wr *bad_wr;

  if(ibv_post_srq_recv(srq, &wr, &bad_wr)) {
    throw std::runtime_error("Error in post recv");
  }
}

bbts_context_t* bbts_init_context(
    std::string dev_name,
    int32_t rank,
    unsigned int num_rank,
    uint8_t ib_port = 1,
    uint32_t num_send_per_qp = 0,
    uint32_t num_write_per_qp = 0)
{
  // Give default values for when num_send_per_qp is not specified
  // or given an invalid zero value
  if(num_send_per_qp == 0) {
    // Max ability to send this many messages at a tiem to a node
    num_send_per_qp = 16;
  }
  if(num_write_per_qp == 0) {
    // Max ability to do this many rdma writes
    num_write_per_qp = 4;
  }

  bbts_context_t *ctx = new bbts_context_t;
  ctx->num_rank         = num_rank;
  ctx->num_send_per_qp  = num_send_per_qp;
  ctx->num_write_per_qp = num_write_per_qp;
  ctx->qps              = std::vector<ibv_qp_ptr>(num_rank);

  uint32_t num_recv        = ctx->get_num_recv();
  uint32_t num_total_send  = ctx->get_num_total_send();
  uint32_t num_total_write = ctx->get_num_total_write();

  ibv_device  *ib_dev = NULL;
  ibv_device **dev_list = ibv_get_device_list(NULL);
  for(int i = 0; dev_list[i]; ++i) {
    const char* name = ibv_get_device_name(dev_list[i]);
    if(dev_name == name) {
      ib_dev = dev_list[i];
    }
  }
  if(!ib_dev) {
    goto clean_dev_list;
  }

  ctx->context = ibv_open_device(ib_dev);
  if(!ctx->context) {
    goto clean_ctx;
  }

  ctx->pd = ibv_alloc_pd(ctx->context);
  if (!ctx->pd) {
    goto clean_device;
  }

  ctx->recv_msgs = new bbts_message_t[num_recv];
  ctx->recv_msgs_mr = ibv_reg_mr(
      ctx->pd,
      ctx->recv_msgs, num_recv*sizeof(bbts_message_t),
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  if(!ctx->recv_msgs_mr) {
    goto clean_pd;
  }

  ctx->send_msgs = new bbts_message_t[num_total_send];
  ctx->send_msgs_mr = ibv_reg_mr(
      ctx->pd,
      ctx->send_msgs, num_total_send*sizeof(bbts_message_t),
      IBV_ACCESS_LOCAL_WRITE);
  if(!ctx->send_msgs_mr) {
    goto clean_recv_mr;
  }

  ctx->ch = ibv_create_comp_channel(ctx->context);
  if(!ctx->ch) {
    goto clean_send_mr;
  }

  ctx->cq = ibv_create_cq(
    ctx->context,
    num_total_write + num_total_send + num_recv,
    NULL,
    ctx->ch,
    0);
  if (!ctx->cq) {
    goto clean_ch;
  }

  if(ibv_req_notify_cq(ctx->cq, 0)) {
    goto clean_cq;
  }

  {
		ibv_srq_init_attr attr = {
			.attr = {
				.max_wr  = num_recv,
				.max_sge = 1
			}
		};
    ctx->srq = ibv_create_srq(ctx->pd, &attr);
    if(!ctx->srq) {
      goto clean_cq;
    }
  }

  {
    ibv_qp_init_attr init_attr = {
      .send_cq = ctx->cq,
      .recv_cq = ctx->cq,
      .srq     = ctx->srq,
      .cap     = {
        .max_send_wr  = (num_send_per_qp + num_write_per_qp),
        .max_recv_wr  = 0,
        .max_send_sge = 1,
        .max_recv_sge = 1
      },
      .qp_type = IBV_QPT_RC,
      .sq_sig_all = 0 // only generate a work request from send item when
                      // IBV_SEND_SIGNALED send flag is set
    };

    int i;
    for(i = 0; i != num_rank; ++i) {
      if(i == rank) {
        continue;
      }
      ctx->qps[i] = ibv_create_qp(ctx->pd, &init_attr);
      if(!ctx->qps[i]) {
        i -= 1;
        for(; i >= 0; --i) {
          if(i == rank)
            continue;
          ibv_destroy_qp(ctx->qps[i]);
        }
        goto clean_srq;
      }
    }
  }

  {
    ibv_qp_attr attr = {
      .qp_state        = IBV_QPS_INIT,
      // TODO: not sure if specifying access flags is necessary
      .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE,
      .pkey_index      = 0,
      .port_num        = ib_port,
    };

    for(int i = 0; i != num_rank; ++i) {
      if(i == rank) {
        continue;
      }
      if (ibv_modify_qp(ctx->qps[i], &attr,
            IBV_QP_STATE              |
            IBV_QP_PKEY_INDEX         |
            IBV_QP_PORT               |
            IBV_QP_ACCESS_FLAGS)) {
        goto clean_qps;
      }
    }
  }

  ibv_free_device_list(dev_list);
  return ctx;

clean_qps:
  for(int i = 0; i != num_rank; ++i) {
    if(i == rank)
      continue;
    ibv_destroy_qp(ctx->qps[i]);
  }

clean_srq:
  ibv_destroy_srq(ctx->srq);

clean_cq:
  ibv_destroy_cq(ctx->cq);

clean_ch:
  ibv_destroy_comp_channel(ctx->ch);

clean_send_mr:
  ibv_dereg_mr(ctx->send_msgs_mr);
  delete[] ctx->send_msgs;

clean_recv_mr:
  ibv_dereg_mr(ctx->recv_msgs_mr);
  delete[] ctx->recv_msgs;

clean_pd:
  ibv_dealloc_pd(ctx->pd);

clean_device:
  ibv_close_device(ctx->context);

clean_ctx:
  free(ctx);

clean_dev_list:
  ibv_free_device_list(dev_list);

  return nullptr;
}

// Move the queue pair through the states to get it into the
// Ready to Send state.
bool bbts_connect_context_helper(
  ibv_qp *qp,
  uint8_t ib_port,
  int my_psn,
	bbts_dest_t const& dest)
{
	ibv_qp_attr attr = {
		.qp_state		= IBV_QPS_RTR,
		.path_mtu		= IBV_MTU_1024,
		.rq_psn			= dest.psn,
		.dest_qp_num		= dest.qpn,
		.ah_attr		= {
			.dlid		= dest.lid,
			.sl		= 0,
			.src_path_bits	= 0,
			.is_global	= 0,
			.port_num	= ib_port
		},
		.max_dest_rd_atomic	= 1,
		.min_rnr_timer		= 12
	};

	if (ibv_modify_qp(qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_AV                 |
			  IBV_QP_PATH_MTU           |
			  IBV_QP_DEST_QPN           |
			  IBV_QP_RQ_PSN             |
			  IBV_QP_MAX_DEST_RD_ATOMIC |
			  IBV_QP_MIN_RNR_TIMER)) {
		return false;
	}

	attr.qp_state	    = IBV_QPS_RTS;
	attr.timeout	    = 14;
	attr.retry_cnt	    = 7;
	attr.rnr_retry	    = 7;
	attr.sq_psn	    = my_psn;
	attr.max_rd_atomic  = 1;
	if (ibv_modify_qp(qp, &attr,
			  IBV_QP_STATE              |
			  IBV_QP_TIMEOUT            |
			  IBV_QP_RETRY_CNT          |
			  IBV_QP_RNR_RETRY          |
			  IBV_QP_SQ_PSN             |
			  IBV_QP_MAX_QP_RD_ATOMIC)) {
		return false;
	}

	return true;
}

uint32_t random_psn() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<uint32_t> d;
  return d(gen);
}

bool bbts_client_exch_dest_and_connect(
  bbts_context_t *ctx,
  int qp_index,
  std::string servername,
  uint8_t ib_port, int port,
  bbts_dest_t& rem_dest)
{
  bbts_dest_t my_dest = {
    .lid = ctx->portinfo.lid,
    .qpn = ctx->qps[qp_index]->qp_num,
    .psn = random_psn()
  };
  bool success = client_exch(servername, port, my_dest, rem_dest);
  if(!success) {
    return false;
  }

  if(!bbts_connect_context_helper(ctx->qps[qp_index], ib_port, my_dest.psn, rem_dest)) {
    return false;
  }

  return true;
}

bool bbts_server_exch_dest_and_connect(
  bbts_context_t *ctx,
  int qp_index,
  uint8_t ib_port, int port,
  bbts_dest_t& rem_dest)
{
  bbts_dest_t my_dest = {
    .lid = ctx->portinfo.lid,
    .qpn = ctx->qps[qp_index]->qp_num,
    .psn = random_psn()
  };
  bool success = server_exch(
        port,
        [&]()
        {
          return bbts_connect_context_helper(
                  ctx->qps[qp_index], ib_port, my_dest.psn, rem_dest);
        },
        my_dest, rem_dest);
  if(!success) {
    return false;
  }

  return true;
}

int bbts_connect_context(
    bbts_context_t *ctx,
    int32_t rank,
    std::vector<std::string> ips,
    uint8_t ib_port = 1, int port = 18515)
{
  std::vector<bbts_dest_t> rem_dests(ctx->num_rank);

  if (ibv_query_port(ctx->context, ib_port, &ctx->portinfo)) {
    return 1;
  }

  // Both the server and the client exch functions call bbts_connect_context_helper,
  // but they do so at different points.
  // This code was built off of an experiment which was built off of with libibverbs
  // pingpong example, and that is what they did.
  // It is unclear to me why that is the case.
  //
  for(int i = 0; i != ctx->num_rank; ++i) {
    if(i == rank)
      continue;

    bool success_exch =
      (i >= rank)
      ? bbts_server_exch_dest_and_connect(ctx, i,         ib_port, port, rem_dests[i])
      : bbts_client_exch_dest_and_connect(ctx, i, ips[i], ib_port, port, rem_dests[i]);
    if(!success_exch){
      return 1;
    }
  }

  // PRINT DEST INFO HERE
  for(int i = 0; i != ctx->num_rank; ++i) {
    if(i == rank) {
      printf("local LID: 0x%04x\n",
             ctx->portinfo.lid);
    } else {
      printf("remote address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x\n",
             rem_dests[i].lid, rem_dests[i].qpn, rem_dests[i].psn);
    }
  }

  return 0;
}

connection_t::connection_t(
  std::string dev_name,
  int32_t rank,
  std::vector<std::string> ips,
  uint64_t num_pinned_tags,
  bool poll):
    rank(rank), poll(poll),
    current_recv_msg(0),
    num_pinned_tags(num_pinned_tags),
    send_wr_cnts(ips.size()),
    pending_msgs(ips.size()),
    write_wr_cnts(ips.size()),
    pending_writes(ips.size())
{
  num_rank = ips.size();
  if(num_rank == 1) {
    return;
  }

  bbts_context_t *context = bbts_init_context(
    dev_name,
    rank,
    num_rank);

  if(!context) {
    throw std::runtime_error("make_connection: couldn't init context");
  }

  // add the available send messages
  for(int i = 0; i != context->get_num_total_send(); ++i) {
    available_send_msgs.push(i);
  }

  // we can send this many items
  std::fill(send_wr_cnts.begin(), send_wr_cnts.end(), context->num_send_per_qp);
  // we can do this many rdma writes
  std::fill(write_wr_cnts.begin(), write_wr_cnts.end(), context->num_write_per_qp);

  // post a receive before setting up a connection so that
  // when the connection is started, the recving can occur
  for(int which_recv = 0; which_recv != context->get_num_recv(); ++which_recv) {
    bbts_post_recv_message(
      context->srq,
      context->recv_msgs_mr->lkey,
      context->recv_msgs[which_recv]);
  }

  if(bbts_connect_context(context, rank, ips)) {
    throw std::runtime_error("make_connection: couldn't connect context");
  }

  this->num_rank         = context->num_rank;
  this->num_recv         = context->get_num_recv();
  this->num_send_per_qp  = context->num_send_per_qp;
  this->num_write_per_qp = context->num_write_per_qp;

  this->recv_msgs      = context->recv_msgs;
  this->recv_msgs_mr   = context->recv_msgs_mr;
  this->send_msgs      = context->send_msgs;
  this->send_msgs_mr   = context->send_msgs_mr;

  this->context               = context->context;
  this->channel               = context->ch;
  this->completion_queue      = context->cq;
  this->protection_domain     = context->pd;
  this->shared_recv_queue     = context->srq;
  this->queue_pairs           = context->qps;

  delete context;

  destruct_counter = 1;
  completion_queue_processor =
    std::thread(&connection_t::process_completion_queue, this);
}

connection_t::~connection_t() {
  destruct_counter--;
  if(poll) {
    completion_queue_processor.join();
  } else {
    std::cout << "WARNING: not joining completion_queue_processor" << std::endl;
    // TODO
  }

  for(int i = 0; i != num_rank; ++i) {
    if(i == rank)
      continue;
    ibv_destroy_qp(this->queue_pairs[i]);
  }
  ibv_destroy_srq(this->shared_recv_queue);
  ibv_destroy_comp_channel(this->channel);
  ibv_destroy_cq(this->completion_queue);
  ibv_dereg_mr(this->send_msgs_mr);
  ibv_dereg_mr(this->recv_msgs_mr);
  delete[] send_msgs;
  delete[] recv_msgs;
  ibv_dealloc_pd(this->protection_domain);
  ibv_close_device(this->context);
}

void connection_t::_send(
  int32_t dest_rank, tag_t tag, send_item_ptr_t send_item)
{
  std::lock_guard<std::mutex> lk(_m);

  if(dest_rank == get_rank()) {
    send_to_self(tag, send_item);
  } else {
    auto& v_send_queue = get_send_queue(tag, dest_rank);
    v_send_queue.insert_item(send_item);
  }
}

void connection_t::_recv(
  int32_t from_rank, tag_t tag, recv_item_ptr_t recv_item)
{
  std::lock_guard<std::mutex> lk(_m);

  if(from_rank == rank) {
    recv_from_self(tag, recv_item);
  } else {
    auto& v_recv_queue = get_recv_queue(tag, from_rank);
    v_recv_queue.insert_item(recv_item);
  }
}

void connection_t::_recv_anywhere(
  tag_t tag, recv_item_ptr_t recv_item)
{
  std::lock_guard<std::mutex> lk(_m);

  for(int from_rank = 0; from_rank != num_rank; ++from_rank) {
    if(from_rank == rank) {
      recv_from_self(tag, recv_item);
    } else {
      auto& v_recv_queue = get_recv_queue(tag, from_rank);
      v_recv_queue.insert_item(recv_item);
    }
  }
}

future<bool> connection_t::send(int32_t dest_rank, tag_t tag, bytes_t bytes) {
  auto send_item = std::shared_ptr<send_item_t>(new send_item_t{
    memory_region_bytes_t(bytes),
    std::promise<bool>()});
  auto fut = send_item->pr.get_future();
  std::thread t(&connection_t::_send, this, dest_rank, tag, send_item);
  t.detach();
  return fut;
}

future<bool> connection_t::send(
  int32_t dest_rank, tag_t tag, bytes_t bytes, ibv_mr* bytes_mr)
{
  auto send_item = std::shared_ptr<send_item_t>(new send_item_t{
    memory_region_bytes_t(bytes, bytes_mr),
    std::promise<bool>()});
  auto fut = send_item->pr.get_future();
  std::thread t(&connection_t::_send, this, dest_rank, tag, send_item);
  t.detach();
  return fut;
}

future<tuple<bool, int32_t, own_bytes_t> >
  connection_t::recv(tag_t tag)
{
  auto recv_item = std::shared_ptr<recv_item_t>(new recv_item_t(
      memory_region_bytes_t(),
      promise<tuple<bool, int32_t, own_bytes_t> >()));
  auto fut = std::get<recv_item_t::which_pr::rank_bytes>(recv_item->pr).get_future();
  std::thread t(&connection_t::_recv_anywhere, this, tag, recv_item);
  t.detach();
  return fut;
}

future<tuple<bool, int32_t> >
  connection_t::recv_with_bytes(tag_t tag, bytes_t bytes)
{
  auto recv_item = std::shared_ptr<recv_item_t>(new recv_item_t(
      memory_region_bytes_t(bytes),
      promise<tuple<bool, int32_t> >()));
  auto fut = std::get<recv_item_t::which_pr::rank_success>(recv_item->pr).get_future();
  std::thread t(&connection_t::_recv_anywhere, this, tag, recv_item);
  t.detach();
  return fut;
}

future<tuple<bool, int32_t > >
  connection_t::recv_with_bytes(
  tag_t tag, bytes_t bytes, ibv_mr* bytes_mr)
{
  auto recv_item = std::shared_ptr<recv_item_t>(new recv_item_t(
      memory_region_bytes_t(bytes, bytes_mr),
      promise<tuple<bool, int32_t> >()));
  auto fut = std::get<recv_item_t::which_pr::rank_success>(recv_item->pr).get_future();
  std::thread t(&connection_t::_recv_anywhere, this, tag, recv_item);
  t.detach();
  return fut;
}

future<tuple<bool, own_bytes_t>>
  connection_t::recv_from(int32_t from_rank, tag_t tag)
{
  auto recv_item = std::shared_ptr<recv_item_t>(new recv_item_t(
      memory_region_bytes_t(),
      promise<tuple<bool, own_bytes_t> >()));
  auto fut = std::get<recv_item_t::which_pr::just_bytes>(recv_item->pr).get_future();
  std::thread t(&connection_t::_recv, this, from_rank, tag, recv_item);
  t.detach();
  return fut;
}

future<bool>
  connection_t::recv_from_with_bytes(
    int32_t from_rank, tag_t tag, bytes_t bytes)
{
  auto recv_item = std::shared_ptr<recv_item_t>(new recv_item_t(
      memory_region_bytes_t(bytes),
      promise<bool>()));
  auto fut = std::get<recv_item_t::which_pr::just_success>(recv_item->pr).get_future();
  std::thread t(&connection_t::_recv, this, from_rank, tag, recv_item);
  t.detach();
  return fut;
}

future<bool>
  connection_t::recv_from_with_bytes(
    int32_t from_rank, tag_t tag, bytes_t bytes, ibv_mr* bytes_mr)
{
  auto recv_item = std::shared_ptr<recv_item_t>(new recv_item_t(
      memory_region_bytes_t(bytes, bytes_mr),
      promise<bool>()));
  auto fut = std::get<recv_item_t::which_pr::just_success>(recv_item->pr).get_future();
  std::thread t(&connection_t::_recv, this, from_rank, tag, recv_item);
  t.detach();
  return fut;
}

void connection_t::post_send(int32_t dest_rank, bbts_message_t const& msg) {
  WC12_COUT("send_wr_cnts at " << dest_rank << ": " << send_wr_cnts[dest_rank]);
  if(send_wr_cnts[dest_rank] == 0) {
    pending_msgs[dest_rank].push(msg);
  } else {
    send_wr_cnts[dest_rank] -= 1;

    int i = available_send_msgs.front();
    available_send_msgs.pop();

    send_msgs[i] = msg;
    WRID_COUT(dest_rank, i << " send msg");
    bbts_post_send_message(queue_pairs[dest_rank], i, send_msgs_mr->lkey, send_msgs[i]);
  }
}

void connection_t::post_open_send(int32_t dest_rank, tag_t tag, uint64_t size){
  _DCB_COUT_("post open send to dest " << dest_rank << ", tag " << tag << std::endl);
  this->post_send(dest_rank,
  {
    .type = bbts_message_t::message_type::open_send,
    .rank      = dest_rank,
    .from_rank = rank,
    .tag = tag,
    .m = {
      .open_send {
        .size = size,
      }
    }
  });
}

void connection_t::post_close_send(int32_t dest_rank, tag_t tag){
  _DCB_COUT_("post close to dest " << dest_rank << ", tag " << tag << std::endl);
  this->post_send(dest_rank,
  {
    .type = bbts_message_t::message_type::close_send,
    .rank      = dest_rank,
    .from_rank = rank,
    .tag = tag
  });
}

void connection_t::post_fail_send(int32_t dest_rank, tag_t tag){
  _DCB_COUT_("post close to dest " << dest_rank << ", tag " << tag << std::endl);
  this->post_send(dest_rank,
  {
    .type = bbts_message_t::message_type::fail_send,
    .rank      = dest_rank,
    .from_rank = rank,
    .tag = tag
  });
}

void connection_t::post_rdma_write(int32_t dest_rank, bbts_rdma_write_t const& r) {
  _DCB_COUT_("connection_t::post_rdma_write" << std::endl);
  WC12_COUT("write_wr_cnts at " << dest_rank << ": " << write_wr_cnts[dest_rank]);
  if(write_wr_cnts[dest_rank] == 0) {
    pending_writes[dest_rank].push(r);
  } else {
    write_wr_cnts[dest_rank] -= 1;

    WRID_COUT(dest_rank, r.wr_id << " rdma write");
    bbts_post_rdma_write(queue_pairs[dest_rank], r);
  }
}

void connection_t::post_open_recv(
  int32_t dest_rank, tag_t tag, uint64_t addr, uint64_t size, uint32_t key)
{
  _DCB_COUT_("post open recv to dest " << dest_rank << ", tag " << tag << std::endl);
  this->post_send(dest_rank,
  {
    .type = bbts_message_t::message_type::open_recv,
    .rank      = dest_rank,
    .from_rank = rank,
    .tag = tag,
    .m = {
      .open_recv {
        .addr = addr,
        .size = size,
        .key = key
      }
    }
  });
}

void connection_t::post_fail_recv(int32_t dest_rank, tag_t tag){
  _DCB_COUT_("post close to dest " << dest_rank << ", tag " << tag << std::endl);
  this->post_send(dest_rank,
  {
    .type = bbts_message_t::message_type::fail_recv,
    .rank      = dest_rank,
    .from_rank = rank,
    .tag = tag
  });
}

void connection_t::process_completion_queue() {
  if(get_num_nodes() == 1) {
    return;
  }

  if(poll) {
    while(destruct_counter){
      // It's not important that the thread catches the destruct exactly.
      for(int i = 0; i != 1000000; ++i) {
        _poll();
      }
    }
  } else {
    ibv_cq *ev_cq;
    void *ev_ctx;
    int ret;

    while(destruct_counter) {
      // TODO: how does this know if it should destruct?
      //       can the destructore destory an ibv something?

      ret = ibv_get_cq_event(channel, &ev_cq, &ev_ctx);
      if(ret) {
        throw std::runtime_error("ibv_get_cq_event in non-poll connection_t");
      }

      ibv_ack_cq_events(ev_cq, 1);

      ret = ibv_req_notify_cq(ev_cq, 0);
      if (ret) {
        throw std::runtime_error("ibv_req_notify_cq in non-poll connection_t");
      }

      _poll();
    }
  }
}

void connection_t::_poll() {
  static ibv_wc work_completion;
  std::lock_guard<std::mutex> lk(_m);

  // process completion item
  int ne = ibv_poll_cq(completion_queue, 1, &work_completion);
  while(ne != 0) {
    if(ne < 0) {
      throw std::runtime_error("ibv_poll_cq error");
    }
    if(int err = work_completion.status) {
      _DCB_COUT_("work completion status " << err << std::endl);

      // The only valid items in a work completion with a non success
      // status are:
      //   status, wr_id, qp_num and vendor_err.
      // Printing out the first three of those...
      std::cout
        << "WR ID of failed work completion: "
        << work_completion.wr_id << std::endl;
      std::cout << "ibv wc STATUS " <<
        ibv_wc_status_str(work_completion.status) << std::endl;
      std::cout << "wc has RANK of " <<
        get_recv_rank(work_completion) << std::endl;

      throw std::runtime_error("work completion error");
    }
    handle_work_completion(work_completion);

    ne = ibv_poll_cq(completion_queue, 1, &work_completion);
  }
}

virtual_send_queue_t& connection_t::get_send_queue(tag_t tag, int32_t dest_rank) {
  auto iter = virtual_send_queues.find({tag, dest_rank});
  if(iter == virtual_send_queues.end()) {
    iter = virtual_send_queues.insert({
      {tag, dest_rank},
      virtual_send_queue_t(this, dest_rank, tag)
    }).first;
  }
  auto& [_, v_send_queue] = *iter;
  return v_send_queue;
}

virtual_recv_queue_t& connection_t::get_recv_queue(tag_t tag, int32_t from_rank) {
  auto iter = virtual_recv_queues.find({tag, from_rank});
  if(iter == virtual_recv_queues.end()) {
    iter = virtual_recv_queues.insert({
      {tag, from_rank},
      virtual_recv_queue_t(this, from_rank, tag)
    }).first;
  }
  auto& [_, v_recv_queue] = *iter;
  return v_recv_queue;
}

// TODO: send_to_self and recv_from_self could be optimized to
//       make less queries to self_sends and self_recvs
//       (but that sounds tedious and prone to error)

void connection_t::send_to_self(tag_t tag, send_item_ptr_t send_item) {
  // This will create the queue at tag if one doesn't exist
  std::queue<send_item_ptr_t>& sends = self_sends[tag];
  std::queue<recv_item_ptr_t>& recvs = self_recvs[tag];

  if(!sends.empty()) {
    // There is already something in the send queue, so we must be
    // waiting for recvs. Add to the queue and be done
    sends.push(std::move(send_item));
    return;
  }

  // sends empty, see if we can acquire a recv
  while(!recvs.empty()) {
    recv_item_ptr_t recv_item = recvs.front();

    // If we acquire it, we won't needs it in recvs.
    // If we don't acquire it, we won't need it in recvs.
    recvs.pop();

    if(recv_item->which_state == recv_item_t::state::wait_open_send) {
      // we have found a match, so acquire it
      recv_item->which_state = recv_item_t::state::wait_post;
      set_send_recv_self_items(send_item, recv_item);

      // If both queues are empty, remove em both
      if(recvs.empty() && tag >= num_pinned_tags) {
        self_recvs.erase(tag);
        self_sends.erase(tag);
      }
      return;
    }
  }

  // the recvs object will be erased when a recv item is added,
  // so don't erase it here, even though it is empty

  // no recv to use, add it to the queue
  self_sends[tag].push(std::move(send_item));
}

void connection_t::recv_from_self(tag_t tag, recv_item_ptr_t recv_item) {
  // This will create the queue at tag if one doesn't exist
  std::queue<send_item_ptr_t>& sends = self_sends[tag];
  std::queue<recv_item_ptr_t>& recvs = self_recvs[tag];

  if(!recvs.empty()) {
    // There is already something in the recv queue, so we must be
    // waiting for sends. Add to the queue and be done
    recvs.push(recv_item);
    return;
  }
  if(sends.empty()) {
    // There is no matching send, so we're waiting for sends.
    // Add to the queue and be done
    recvs.push(recv_item);
    return;
  }

  // recvs empty, sends not empty
  if(recv_item->which_state == recv_item_t::state::wait_open_send) {
    // we have a match, so acquire it
    recv_item->which_state = recv_item_t::state::wait_post;
    set_send_recv_self_items(sends.front(), recv_item);

    sends.pop();
    // If both queues are empty, remove em both
    if(sends.empty() && tag >= num_pinned_tags) {
      self_recvs.erase(tag);
      self_sends.erase(tag);
    }
  }
}

void connection_t::set_send_recv_self_items(
  send_item_ptr_t send_item,
  recv_item_ptr_t recv_item)
{
  // Invariant: the recv_item has been acquired

  bytes_t send_bs = send_item->bytes.get_bytes();
  bool setup_bytes = recv_item->bytes.setup_bytes(send_bs.size);
  if(!setup_bytes) {
    // couldn't get the memory to copy the data

    // set recv to fail
    recv_item->set_fail(get_rank());

    // set send to fail
    send_item->pr.set_value(false);
  } else {
    // copy the data
    bytes_t recv_bs = recv_item->bytes.get_bytes();
    char* send_data = (char*)send_bs.data;
    char* recv_data = (char*)recv_bs.data;
    std::copy(send_data, send_data + send_bs.size, recv_data);

    // set recv to success
    recv_item->set_success(get_rank());

    // set send to success
    send_item->pr.set_value(true);
  }
}

// There are three types of messages:
//   recv, send and rdma write
// If rdma write,
//   the corresponding tag is given by wr_id
// If send,
//   which send message has just completed is given by wr_id
// If recv,
//   which recv message is given by current_recv_message,
//   wr_id is not used
void connection_t::handle_work_completion(ibv_wc const& work_completion) {
  bool is_send       = work_completion.opcode == 0;
  bool is_recv       = work_completion.opcode == 128;
  bool is_rdma_write = work_completion.opcode == 1;
  int32_t wc_rank = get_recv_rank(work_completion);

  if(is_rdma_write) {
    // handle the completion
    // then update the pending writes queue
    auto& tag = work_completion.wr_id;
    auto& dest_rank = wc_rank;
    WC_COUT(tag, dest_rank, "finished rdma write");

    virtual_send_queues.at({tag, dest_rank}).completed_rdma_write();

    // update the write_wr_cnt
    write_wr_cnts[dest_rank] += 1;

    if(!pending_writes[dest_rank].empty()) {
      _DCB_COUT_("rdma write from queue" << std::endl);
      // making an rdma write, decement it back
      write_wr_cnts[dest_rank] -= 1;
      // get the meta data and do the write
      WRID_COUT(
        dest_rank,
        pending_writes[dest_rank].front().wr_id << " rdma write");
      bbts_post_rdma_write(
        queue_pairs[dest_rank],
        pending_writes[dest_rank].front());
      // and remove the item from the queue
      pending_writes[dest_rank].pop();
    }
  } else if(is_send) {
    // handle the message item
    // then repost it to the queue
    auto& which = work_completion.wr_id;
    auto& msg = send_msgs[which];
    tag_rank_t tag_rank = {msg.tag, msg.rank};
    auto& [tag, dest_rank] = tag_rank;

    if(msg.type == bbts_message_t::message_type::open_send) {
      WC_COUT(tag, dest_rank, "handle: sent open send");
      virtual_send_queues.at(tag_rank).completed_open_send();
    } else if(msg.type == bbts_message_t::message_type::open_recv) {
      WC_COUT(tag, dest_rank, "handle: sent open recv");
      virtual_recv_queues.at(tag_rank).completed_open_recv();
    } else if(msg.type == bbts_message_t::message_type::close_send) {
      WC_COUT(tag, dest_rank, "handle: sent close send");
      virtual_send_queue_t& v_send_queue = virtual_send_queues.at(tag_rank);
      v_send_queue.completed_close_send();
      // A message has been completed, so it could be the case that
      // v_send_queue is sitting unused in virtual_send_queues, so
      // get rid of it.
      if(tag >= this->num_pinned_tags && v_send_queue.empty()) {
        virtual_send_queues.erase(tag_rank);
      }
    } else if(msg.type == bbts_message_t::message_type::fail_send) {
      WC_COUT(tag, dest_rank, "handle: sent fail send");
      virtual_send_queue_t& v_send_queue = virtual_send_queues.at(tag_rank);
      v_send_queue.completed_fail_send();
      // A message has been completed.
      if(tag >= this->num_pinned_tags && v_send_queue.empty()) {
        virtual_send_queues.erase(tag_rank);
      }
    } else if(msg.type == bbts_message_t::message_type::fail_recv) {
      WC_COUT(tag, dest_rank, "handle: sent fail recv");
      virtual_recv_queue_t& v_recv_queue = virtual_recv_queues.at(tag_rank);
      v_recv_queue.completed_fail_recv();
      // A message has been completed.
      if(tag >= this->num_pinned_tags && v_recv_queue.empty()) {
        virtual_recv_queues.erase(tag_rank);
      }
    } else {
      throw std::runtime_error("invalid message type");
    }

    // add the index to the send msg now that this send_msg is available
    available_send_msgs.push(which);

    // update the send_wr_cnt
    send_wr_cnts[dest_rank] += 1;

    // check the queue
    if(!pending_msgs[dest_rank].empty()) {
      // making another send, decrement the send_wr_cnt
      send_wr_cnts[dest_rank] -= 1;

      // get an open send_msg object
      // (which is why it was important to add to available_send_msgs
      //  above before getting here)
      int i = available_send_msgs.front();
      available_send_msgs.pop();

      // write the message to be sent to the avilable send_msgs memory
      send_msgs[i] = pending_msgs[dest_rank].front();
      pending_msgs[dest_rank].pop();

      WRID_COUT(dest_rank, i << " send msg");
      bbts_post_send_message(
        queue_pairs[dest_rank],
        i,
        send_msgs_mr->lkey,
        send_msgs[i]);
    }
  } else if(is_recv) {
    // a message has been recvd
    // so post a recv back and handle the message

    // copy the message here so msg_recv can be used again
    // in the post recv
    bbts_message_t msg = recv_msgs[current_recv_msg];

    bbts_post_recv_message(
      shared_recv_queue, recv_msgs_mr->lkey, recv_msgs[current_recv_msg]);

    current_recv_msg++;
    if(current_recv_msg == num_recv) {
      current_recv_msg = 0;
    }

    // now handle the message
    tag_rank_t tag_rank = {msg.tag, msg.from_rank};
    auto& [tag, from_rank] = tag_rank;
    if(msg.type == bbts_message_t::message_type::open_send) {
      WC_COUT(tag, from_rank, "handle: recv open send");
      // Note: this can recv an open send without there being an open channel
      virtual_recv_queue_t& recv_queue = get_recv_queue(msg.tag, msg.from_rank);
      recv_queue.recv_open_send(msg.m.open_send.size);
    } else if(msg.type == bbts_message_t::message_type::open_recv) {
      WC_COUT(tag, from_rank, "handle: recv open recv");
      virtual_send_queues.at(tag_rank).recv_open_recv(
        msg.m.open_recv.addr,
        msg.m.open_recv.size,
        msg.m.open_recv.key);
    } else if(msg.type == bbts_message_t::message_type::close_send) {
      WC_COUT(tag, from_rank, "handle: recv close send");
      virtual_recv_queue_t& v_recv_queue = virtual_recv_queues.at(tag_rank);
      v_recv_queue.recv_close_send();
      // A message has been completed
      if(tag >= this->num_pinned_tags && v_recv_queue.empty()) {
        virtual_recv_queues.erase(tag_rank);
      }
    } else if(msg.type == bbts_message_t::message_type::fail_send) {
      WC_COUT(tag, from_rank, "handle: recv fail send");
      virtual_recv_queue_t& v_recv_queue = virtual_recv_queues.at(tag_rank);
      v_recv_queue.recv_fail_send();
      // A message has been completed
      if(tag >= this->num_pinned_tags && v_recv_queue.empty()) {
        virtual_recv_queues.erase(tag_rank);
      }
    } else if(msg.type == bbts_message_t::message_type::fail_recv) {
      WC_COUT(tag, from_rank, "handle: recv fail recv");
      virtual_send_queue_t& v_send_queue = virtual_send_queues.at(tag_rank);
      v_send_queue.recv_fail_recv();
      // A message has been completed.
      if(tag >= this->num_pinned_tags && v_send_queue.empty()) {
        virtual_send_queues.erase(tag_rank);
      }
    } else {
      throw std::runtime_error("invalid message type");
    }
  } else {
    throw std::runtime_error("unhandled work completion");
  }
}

int connection_t::get_recv_rank(ibv_wc const& wc) const {
  for(int i = 0; i != num_rank; ++i) {
    if(i == rank)
      continue;
    if(queue_pairs[i]->qp_num == wc.qp_num) {
      return i;
    }
  }
  throw std::runtime_error("could not get rank from work completion");
}

}
