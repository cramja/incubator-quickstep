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

#include <cstddef>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "cli/Flags.hpp"
#include "cli/LineReaderBuffered.hpp"
#include "cli/NetworkCliClient.hpp"
#include "cli/NetworkIO.hpp"
#include "threading/ConditionVariable.hpp"
#include "threading/Mutex.hpp"
#include "threading/Thread.hpp"

#include "glog/logging.h"
#include "gtest/gtest.h"

using std::unique_ptr;

namespace quickstep {

static std::string const kQueryRequest = "O Captain! My Captain!";
static std::string const kQueryResponse = "Our fearful trip is done,";

/**
 * Contains a server instance for testing.
 */
class TestNetworkIO {
 public:
  TestNetworkIO()
      : service_(),
        server_address_("localhost:" + std::to_string(FLAGS_port)),
        server_(nullptr) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    CHECK(server_) << "Failed to start server";
    LOG(INFO) << "TestSingleNodeServer listening on " << server_address_;
  }

  // Gets a message from the Service worker.
  std::string getSentMessage() {
    networkio_internal::RequestState& requestState = service_.getRequestState();
    std::unique_lock<std::mutex> lock(requestState.mutex_);
    while (!requestState.request_ready_)
      requestState.condition_.wait(lock);

    EXPECT_EQ(requestState.request_ready_, true);
    EXPECT_EQ(requestState.response_ready_, false);

    return requestState.request_buffer_;
  }

  // Sets the response message of the Service worker. Alerts it that the request is ready.
  void setResponse(std::string response) {
    networkio_internal::RequestState& requestState = service_.getRequestState();
    requestState.response_message_.set_query_result(response);

    EXPECT_EQ(requestState.response_ready_, false);

    requestState.responseReady();
  }

  NetworkCliServiceImpl& getService() {
    return service_;
  }

  ~TestNetworkIO() {
    service_.kill();
    server_->Shutdown();
    server_->Wait();
  }

 private:
  NetworkCliServiceImpl service_;
  std::string server_address_;
  std::unique_ptr<grpc::Server> server_;
};

/**
 * Tests that killing the service will cancel requests.
 */
TEST(NetworkIOTest, TestShutdown) {
  TestNetworkIO server;

  server.getService().kill();

  NetworkCliClient client(
    grpc::CreateChannel("localhost:" + std::to_string(FLAGS_port),
                        grpc::InsecureChannelCredentials()));
  QueryRequest request;
  request.set_query(kQueryRequest);
  QueryResponse response;
  Status status = client.SendQuery(request, &response);
  ASSERT_EQ(status.error_code(), grpc::CANCELLED);
}

/**
 * Tests a simple call and response to the Service.
 */
TEST(NetworkIOTest, TestNetworkIOCommandInteraction) {
  NetworkIO networkIO;
  std::string const query_stmt = kQueryRequest + ";" + kQueryRequest;

  // This thread will handle the response from the client in a similar way as the quickstep cli will.
  std::thread server_handler([&networkIO, &query_stmt]() {
    std::string command = networkIO.getNextCommand();
    EXPECT_EQ(command, query_stmt);

    // Set some output for the main test thread, return.
    fprintf(networkIO.out(), "%s", kQueryResponse.c_str());
    networkIO.notifyCommandComplete();
  });

  NetworkCliClient client(
    grpc::CreateChannel("localhost:" + std::to_string(FLAGS_port),
                        grpc::InsecureChannelCredentials()));
  QueryRequest request;
  request.set_query(query_stmt);
  QueryResponse response;
  Status status = client.SendQuery(request, &response);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(response.query_result(), kQueryResponse);
  EXPECT_EQ(response.error_result(), std::string(""));

  server_handler.join();
}

TEST(NetworkIOTest, TestLineReaderBuffered) {
  // The buffered line reader is used exclusively by the NetworkIO's client.
  LineReaderBuffered linereader;
  EXPECT_TRUE(linereader.bufferEmpty());

  std::string stmt = "select * from foo;";
  std::string stmts = stmt + "select 1; select 2; quit;";
  auto const num_stmts = std::count(stmts.begin(), stmts.end(), ';');
  linereader.setBuffer(stmts);
  ASSERT_FALSE(linereader.bufferEmpty());

  std::vector<std::string> stmts_vec;
  int parsed_commands;
  for (parsed_commands = 0;
      parsed_commands < num_stmts + 1 && !linereader.bufferEmpty();
      parsed_commands++) {
    std::string command = linereader.getNextCommand();
    if (command != "") {
      stmts_vec.push_back(command);
    }
  }

  EXPECT_EQ(stmt, stmts_vec.front());
  EXPECT_EQ(num_stmts, stmts_vec.size());
  EXPECT_TRUE(linereader.bufferEmpty());
}

}  // namespace quickstep

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
