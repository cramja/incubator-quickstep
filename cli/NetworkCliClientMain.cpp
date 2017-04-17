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

#include <grpc++/grpc++.h>

#include <iostream>
#include <istream>
#include <memory>
#include <string>

#include "cli/Flags.hpp"
#include "cli/LineReaderBuffered.hpp"
#include "cli/NetworkCliClient.hpp"

#include "gflags/gflags.h"

DECLARE_int32(port);

using quickstep::LineReaderBuffered;
using quickstep::NetworkCliClient;

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Attempts to send a single query retrieved from stdin to the Quickstep Server.
  NetworkCliClient qs_client(
    grpc::CreateChannel("localhost:" + std::to_string(quickstep::FLAGS_port),
                        grpc::InsecureChannelCredentials()));

  // Read stdin until EOF, then we use a Line reader to divide query into parts.
  std::cin >> std::noskipws;
  std::istream_iterator<char> it(std::cin), end;
  std::string user_queries(it, end);

  LineReaderBuffered linereader;
  linereader.setBuffer(user_queries);
  while (!linereader.bufferEmpty()) {
    std::string query = linereader.getNextCommand();
    if (query.size() > 1) {
      std::cout << query << std::endl;
      std::cout << qs_client.Query(query) << std::endl;
    }
  }
  return 0;
}
