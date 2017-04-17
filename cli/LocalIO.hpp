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

#ifndef QUICKSTEP_CLI_LOCAL_IO_HPP_
#define QUICKSTEP_CLI_LOCAL_IO_HPP_

#include <cstdio>
#include <string>

#include "cli/CliConfig.h"
#include "cli/IOInterface.hpp"
#include "cli/LineReader.hpp"

#ifdef QUICKSTEP_USE_LINENOISE
#include "cli/LineReaderLineNoise.hpp"
typedef quickstep::LineReaderLineNoise LineReaderImpl;
#else
#include "cli/LineReaderDumb.hpp"
typedef quickstep::LineReaderDumb LineReaderImpl;
#endif

namespace quickstep {

class LocalIO : public IOInterface {
 public:
  LocalIO() : IOInterface(), line_reader_("quickstep> ", "      ...> ") {}

  /**
   * @return A file handle for standard output.
   */
  FILE *out() override {
    return stdout;
  }

  /**
   * @return A file handle for error output.
   */
  FILE *err() override {
    return stderr;
  }

  /**
   * Requests a complete SQL command. This call may block until user input is given.
   * @note When the command is complete, commandComplete() should be called so that certain implementations can clean
   *    up state related to sending the command.
   * @return A string containing a quickstep command.
   */
  std::string getNextCommand() override {
    return line_reader_.getNextCommand();
  }

 private:
  LineReaderImpl line_reader_;
};

}  // namespace quickstep

#endif  // QUICKSTEP_CLI_LOCAL_IO_HPP_
