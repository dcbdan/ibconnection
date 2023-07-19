#pragma once

#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <unistd.h>

#include <infiniband/verbs.h>

#include <stdexcept>
#include <string>

namespace ib_ns {

template <typename T>
struct memory_region_wrapper_t {
  memory_region_wrapper_t(ibv_pd *pd, bool remote_write=false){
    ptr = new T;
    mr = ibv_reg_mr(
        pd, ptr, sizeof(T),
        remote_write
          ? IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
          : IBV_ACCESS_LOCAL_WRITE);
    if(!mr) {
      throw std::runtime_error("couldn't register memory region!");
    }
  }

  ~memory_region_wrapper_t(){
    if(ibv_dereg_mr(mr)) {
      //throw std::runtime_error("couldn't deregister memory region object");
    }
    delete ptr;
  }

  uint32_t local_key() const {
    return mr->lkey;
  }
  uint32_t remote_key() const {
    return mr->rkey;
  }

  ibv_mr *mr;
  T* ptr;
};

// send the bytes of T over and recieve another bytes of T...
// T must be entirely laid out in it's own memory--i.e. have no pointers inside of it
template <typename T>
bool client_exch(std::string servername, int port, T const& local_item, T& remote_item)
{
 	addrinfo *res, *t;
	addrinfo hints = {
		.ai_family   = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM
	};
	char *service;
	int sockfd = -1;

	if (asprintf(&service, "%d", port) < 0)
		return false;

	int n = getaddrinfo(servername.c_str(), service, &hints, &res);

	if (n < 0) {
		free(service);
		return false;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
		return false;
	}

  if(write(sockfd, (char*)(&local_item), sizeof(T)) != sizeof(T)) {
    goto out;
  }

	if (read(sockfd, (char*)(&remote_item), sizeof(T)) != sizeof(T) ||
	    write(sockfd, "done", sizeof "done") != sizeof "done") {
    goto out;
  }

  return true;

out:
  close(sockfd);
  return false;
}

// This is the same as client_exch, except after recving the remote item
// from the client, call after_recv_remote() which should return true on success
template <typename T, typename F>
bool server_exch(int port, F && after_recv_remote, T const& local_item, T& remote_item)
{
	addrinfo *res, *t;
	addrinfo hints = {
		.ai_flags    = AI_PASSIVE,
		.ai_family   = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM
	};
  int n;
	char *service;

	int sockfd = -1, connfd;

	if (asprintf(&service, "%d", port) < 0)
		return false;

	 n = getaddrinfo(NULL, service, &hints, &res);

	if (n < 0) {
		return false;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			n = 1;

			setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

			if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
    return false;
	}

	listen(sockfd, 1);
	connfd = accept(sockfd, NULL, NULL);
	close(sockfd);

	if (connfd < 0) {
    return false;
  }

	if(read(connfd, (char*)&remote_item, sizeof(T)) != sizeof(T)) {
    goto out;
  }

  if(!after_recv_remote()) {
    goto out;
  }

  char finish_msg[4];
  if(write(connfd, (char*)(&local_item), sizeof(T)) != sizeof(T) ||
      read(connfd, finish_msg, 4) != 4) {
    goto out;
  }

  return true;
out:
	close(connfd);
	return false;
}

}
