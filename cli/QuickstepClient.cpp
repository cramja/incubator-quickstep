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

/* Interacts with the QuickstepServer */

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "grpc++/grpc++.h"

#include "cli/SingleNodeServer.grpc.pb.h"
#include "cli/SingleNodeServer.pb.h"

#include "glog/logging.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace quickstep {
  class QuickstepClient {
  public:
    QuickstepClient(std::shared_ptr<Channel> channel)
      : stub_(SingleNodeServerRequest::NewStub(channel)) {}

    // Assembles the client's payload, sends it and presents the response back
    // from the server.
    std::string SendQuery(const std::string &user) {
      // Data we are sending to the server.
      QueryRequest request;
      request.set_query(user);

      // Container for the data we expect from the server.
      QueryResponse response;

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // The actual RPC.
      Status status = stub_->SendQuery(&context, request, &response);

      // Act upon its status.
      if (status.ok()) {
        return response.query_result();
      } else {
        std::cout << status.error_code() << ": " << status.error_message()
                  << std::endl;
        return "RPC failed";
      }
    }

  private:
    std::unique_ptr<SingleNodeServerRequest::Stub> stub_;
  };
}

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  quickstep::QuickstepClient qs_client(
    grpc::CreateChannel(
    "localhost:50051",
    grpc::InsecureChannelCredentials()));
  std::string user_query;
  std::cin >> user_query;
  std::string reply = qs_client.SendQuery(user_query);
  std::cout << "Client received: " << reply << std::endl;

  return 0;
}