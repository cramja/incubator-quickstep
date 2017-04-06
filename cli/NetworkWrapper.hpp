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

#ifndef QUICKSTEP_CLI_NETWORK_WRAPPER_HPP_
#define QUICKSTEP_CLI_NETWORK_WRAPPER_HPP_

#include <netinet/in.h>
#include <stdio.h>
#include <string.h>

#include "cli/cli_proto_gen/Cli.grpc.pb.h"
#include "cli/cli_proto_gen/Cli.pb.h"

#include "glog/logging.h"
#include "gflags/gflags.h"
#include "grpc/grpc.h"

namespace quickstep {

DECLARE_int32(port);

/** \addtogroup CLI
 *  @{
 */

class NetworkWrapper {
public:
  NetworkWrapper(){}

  virtual ~NetworkWrapper(){}
  /**
   * Blocking. Must not be called if a result was not returned.
   * @return
   */
  virtual std::string getNextCommand() = 0;

  virtual void returnResult(std::string result) = 0;
};

class TCPWrapper : public NetworkWrapper {
public:
  TCPWrapper(int port);

  ~TCPWrapper();

  /**
   * Blocking. Must not be called if a result was not returned.
   * @return
   */
  std::string getNextCommand() override;

  void returnResult(std::string result) override;

  /**
   * @return True if getNextCommand was called and a result has not yet been returned.
   */
  bool handlingRequest() const {
    return open_connection_;
  }

private:
  const int port_;
  bool open_connection_;

  int socket_fd_;
  struct sockaddr_in server_addr_, client_addr_;
  int client_socket_fd_;
};

class GrpcWrapper : public NetworkWrapper {
public:
  GrpcWrapper(int port);

  ~GrpcWrapper();

  /**
   * Blocking. Must not be called if a result was not returned.
   * @return
   */
  std::string getNextCommand() override;

  void returnResult(std::string result) override;
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_CLI_NETWORK_WRAPPER_HPP_
