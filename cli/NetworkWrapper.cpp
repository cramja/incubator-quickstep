/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include "cli/NetworkWrapper.hpp"

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "glog/logging.h"

using std::string;

namespace quickstep {

  NetworkWrapper::NetworkWrapper(int port)
    : port_(port),
      open_connection_(false),
      server_addr_(),
      client_addr_(),
      client_socket_fd_(-1) {
    int error_code = 0;

    socket_fd_ = socket(AF_INET, SOCK_STREAM, 0); // internet domain, stream: TCP, OS flag: keep NULL
    CHECK(socket_fd_ > 0) << "error on call to socket, code " << socket_fd_;

    // set up the server address structure
    bzero((char *) &server_addr_, sizeof(server_addr_));
    server_addr_.sin_family = AF_INET;
    server_addr_.sin_addr.s_addr = INADDR_ANY; // sets the address to the address of the host
    server_addr_.sin_port = htons(port_);      // converts the port number to the network byte order
    // tells the system which address/port to bind to
    if ((error_code = bind(socket_fd_, (struct sockaddr *) &server_addr_, sizeof(server_addr_))) < 0) {
      CHECK(false) << "error on binding to port, code " << error_code;
    } else {
      LOG(INFO) << "listening on " << server_addr_.sin_addr.s_addr << ":" << server_addr_.sin_port;
    }

    int const MAX_WAITERS = 5; // maximum number of processes that can wait on a socket
    error_code = listen(socket_fd_, MAX_WAITERS);
    CHECK(error_code == 0) << "error on listen call, code " << error_code;
  }

  NetworkWrapper::~NetworkWrapper() {
    close(socket_fd_);
  }

  std::string NetworkWrapper::getNextCommand() {
    CHECK(!open_connection_) << "call to get next command when a client request in flight";
    open_connection_ = true;

    socklen_t client_len = sizeof(client_addr_);
    // accept a connection. Blocks until a connection is made.
    client_socket_fd_ = accept(socket_fd_,
                       (struct sockaddr *) &client_addr_,
                       &client_len);
    CHECK(client_socket_fd_ > 0) << "error on accepting connection, code: " << client_socket_fd_;

    int const BUFF_SIZE = 4096;
    ssize_t bytes_read = 0;
    char buff[BUFF_SIZE];
    bzero(buff, BUFF_SIZE);

    bytes_read = read(client_socket_fd_, buff, BUFF_SIZE - 1);
    CHECK(bytes_read) << "error reading from client, code: " << bytes_read;
    CHECK(bytes_read != BUFF_SIZE - 1) << "overflowed buffer, dropped full message"; // TODO(dynamic)
    std::string message(buff);
    return message;
  }

  void NetworkWrapper::returnResult(std::string result) {
    CHECK(open_connection_);
    ssize_t bytes_written = write(client_socket_fd_, result.c_str(), result.length());
    CHECK(bytes_written == result.length()) << "error writing to client, code: " << bytes_written;
    int error_code = close(client_socket_fd_);
    CHECK(error_code == 0) << "error closing client socket";
    client_socket_fd_ = -1;

    open_connection_ = false;
  }

}  // namespace quickstep
