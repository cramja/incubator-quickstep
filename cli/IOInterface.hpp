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

#ifndef QUICKSTEP_CLI_IO_INTERFACE_HPP_
#define QUICKSTEP_CLI_IO_INTERFACE_HPP_

#include <string>

/**
 * Virtual base defines a generic, file-based interface around IO.
 */
class IOInterface {
 public:
  IOInterface() {}

  /**
   * @return A file handle for standard output.
   */
  virtual FILE* out() = 0;

  /**
   * @return A file handle for error output.
   */
  virtual FILE* err() = 0;

  /**
   * @brief Requests a complete SQL command.
   * This call may block until user input is given.
   * @note When the command is complete, commandComplete() should be called so that certain implementations can clean
   *    up state related to sending the command.
   * @return A string containing a quickstep command.
   */
  virtual std::string getNextCommand() = 0;

  /**
   * @brief Notifies the IO system that the previously acquired command is complete.
   */
  virtual void notifyCommandComplete() {}

  /**
   * @brief Notifies the IO system that quickstep is shutting down.
   */
  virtual void notifyShutdown() {}
};

#endif  // QUICKSTEP_CLI_IO_INTERFACE_HPP_
