#include "connection.h"
#include <fstream>

using namespace ib_ns;

using std::vector;
using std::tuple;
using std::string;

using connection_info_t = tuple<string, int32_t, vector<string>>;

tuple<int, connection_info_t> parse_connection_args(int argc, char** argv) {
  string usage = "usage: " + string(argv[0]) + " <rank> <device name> <hosts file>";
  if(argc < 4) {
    throw std::runtime_error(usage);
  }

  int rank = std::stoi(argv[1]);

  vector<string> ips;
  {
    std::ifstream s = std::ifstream(argv[3]);
    if(!s.is_open()) {
      usage = "Coud not open '" + string(argv[3]) + "'\n" + usage;
      throw std::runtime_error(usage);
    }

    string l;
    while(std::getline(s, l)) {
      if(l.size() < 7) {
        usage = "Invalid hosts file\n" + usage;
        throw std::runtime_error(usage);
      }
      ips.push_back(l);
    }
    for(auto i: ips){
      std::cout << "IP: " << i << std::endl;
    }
  }

  return {3, {string(argv[2]), rank, ips}};
}


int main(int argc, char** argv) {
  auto [num_used, connection_info] = parse_connection_args(argc, argv);
  argc -= num_used;
  argv += num_used;

  connection_t connection(connection_info, 10);

  int tag = 0;

  int rank = connection.get_rank();
  bool is_server = rank == 0;
  if(is_server) {
    string message = "Hello, World";
    bytes_t bytes {
      reinterpret_cast<void*>(message.data()),
      sizeof(string::value_type)*message.size()
    };
    connection.send(1, tag, bytes).get();
    std::cout << "sent " << message << std::endl;
  } else {
    auto [_, bytes] = connection.recv_from(0, tag).get();
    string message(bytes.ptr.get(), bytes.ptr.get() + bytes.size);
    std::cout << "recvd " << message << std::endl;
  }
}
