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

#ifndef QUICKSTEP_CLI_NETWORK_IO_HPP_
#define QUICKSTEP_CLI_NETWORK_IO_HPP_

#include <grpc++/grpc++.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "cli/Flags.hpp"
#include "cli/IOInterface.hpp"
#include "cli/NetworkCli.grpc.pb.h"
#include "cli/NetworkCli.pb.h"
#include "threading/ConditionVariable.hpp"
#include "threading/Mutex.hpp"
#include "utility/MemStream.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::Status;

namespace quickstep {

namespace networkio_internal {

/**
 * Contains state and helper methods for managing interactions between a producer/consumer (requestor/requestee)
 * thread.
 */
class RequestState {
 public:
  RequestState() :
    request_ready_(false),
    response_ready_(false),
    request_buffer_(""),
    mutex_(),
    condition_() {}

  /**
   * Notifies waiter that a piece of work has been created and added to the buffer.
   * @param to_consume The arguments for the consuming thread.
   */
  void requestReady(std::string to_consume) {
    request_ready_ = true;
    response_ready_ = false;
    request_buffer_ = to_consume;
    condition_.notify_one();
  }

  /**
   * Notifies that the consumer has finished consuming and that a response is ready.
   * To be called after the consumer has executed.
   */
  void responseReady() {
    request_ready_ = false;
    response_ready_ = true;
    condition_.notify_one();
  }

  bool request_ready_;
  bool response_ready_;
  std::string request_buffer_;
  QueryResponse response_message_;
  std::mutex mutex_;
  std::condition_variable condition_;
};
}  // namespace networkio_internal

class NetworkCliServiceImpl final : public NetworkCli::Service {
 public:
  NetworkCliServiceImpl()
    : NetworkCli::Service(),
      worker_exclusive_mtx_(),
      running_(true),
      request_state_() { }

  /**
   * Handles gRPC request. Sets the buffer in the RequestState, notifies the main thread, then waits for a response.
   * @param context
   * @param request
   * @param response
   * @return
   */
  Status SendQuery(grpc::ServerContext *context,
                   const QueryRequest *request,
                   QueryResponse *response) override {
    auto service_ex_lock = enter();
    if (!service_ex_lock) {
      return Status::CANCELLED;
    }

    // Service thread sets a notification for the main thread.
    // Because of service thread exclusivity, we have rights to this data structure.
    request_state_.requestReady(request->query());

    // Service thread - main thread critical section:
    // Service thread waits for the buffered message response from the main thread. The main thread will set
    // consumer_ready_ when it is finished and released its exclusive lock on the communication data structure.
    std::unique_lock<std::mutex> lock(request_state_.mutex_);
    while (!request_state_.response_ready_)
      request_state_.condition_.wait(lock);

    *response = request_state_.response_message_;

    return Status::OK;
  }

  void kill() {
    running_ = false;
  }

  networkio_internal::RequestState& getRequestState() {
    return request_state_;
  }

 private:
  /**
   * When a worker enters, it gains exclusive access to the main thread. That is, no other worker of this service
   * is allowed to interact with main thread.
   * @return A lock which grants the worker mutual exclusion.
   */
  std::unique_lock<std::mutex> enter() {
    std::unique_lock<std::mutex> lock(worker_exclusive_mtx_);
    if (!running_)
      lock.unlock();
    return lock;
  }

  std::mutex worker_exclusive_mtx_;
  bool running_;
  networkio_internal::RequestState request_state_;
};

class NetworkIO : public IOInterface {
 public:
  NetworkIO()
      : IOInterface(),
        out_stream_(),
        err_stream_(),
        server_(nullptr),
        service_() {
    std::string server_address("0.0.0.0:" + std::to_string(FLAGS_port));

    // Starts a server.
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    server_ = builder.BuildAndStart();
    LOG(INFO) << "Listening on port " << std::to_string(FLAGS_port);
  }

  FILE* out() override {
    return out_stream_.file();
  }

  FILE* err() override {
    return err_stream_.file();
  }

  std::string getNextCommand() override {
    // critical section: wait for a command
    networkio_internal::RequestState& request_state = service_.getRequestState();
    std::unique_lock<std::mutex> lock(request_state.mutex_);
    while (!request_state.request_ready_) {
      request_state.condition_.wait(lock);
      // calling wait releases the mutex, and re-gets it on the call return.
      // Even though we release the lock at the end of this method, the Service thread is waiting on the condition
      // variable which is triggered in commandComplete().
    }

    return request_state.request_buffer_;
  }

  void notifyCommandComplete() override {
    // All the commands from the last network interaction have completed, return our response.
    networkio_internal::RequestState& request_state = service_.getRequestState();
    request_state.response_message_.set_query_result(out_stream_.str());
    request_state.response_message_.set_error_result(err_stream_.str());
    request_state.responseReady();

    resetForNewClient();
  }

  void resetForNewClient() {
    out_stream_.reset();
    err_stream_.reset();
  }

  void notifyShutdown() override {
    service_.kill();
    server_->Shutdown();
    server_->Wait();
  }

 private:
  MemStream out_stream_, err_stream_;
  std::unique_ptr<Server> server_;
  NetworkCliServiceImpl service_;
};

}  // namespace quickstep

#endif  // QUICKSTEP_CLI_NETWORK_IO_HPP_
