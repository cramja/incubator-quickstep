/**
 *   Copyright 2016 Pivotal Software, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/

#ifndef QUICKSTEP_EXPRESSIONS_TABLE_GENERATOR_GENERATOR_FUNCTION_HANDLE_HPP_
#define QUICKSTEP_EXPRESSIONS_TABLE_GENERATOR_GENERATOR_FUNCTION_HANDLE_HPP_

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expressions/table_generator/GeneratorFunction.pb.h"
#include "types/containers/ColumnVectorsValueAccessor.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Expressions
 *  @{
 */

class GeneratorFunctionHandle;
typedef std::shared_ptr<const GeneratorFunctionHandle> GeneratorFunctionHandlePtr;

/**
 * @brief Abstract representation of a concrete generator function.
 *
 * @note This class provides facilities for both the frontend (e.g. output
 *       column type information, which is needed by the query planner) and the
 *       backend (e.g. the populateColumns method). It is also an option, if
 *       needed later, to abstract one more GeneratorFunctionDescriptor class
 *       to split out the functionalities purely for the frontend.
 **/
class GeneratorFunctionHandle {
 public:
  /**
   * @brief Get the number of output columns of this generator function.
   *
   * @return The number of output columns.
   */
  virtual int getNumberOfOutputColumns() const = 0;

  /**
   * @brief Get the default name of the specified output column.
   *
   * @param index The index of the output column.
   * @return The name of the specified output column.
   */
  virtual std::string getOutputColumnName(int index) const {
    std::ostringstream oss;
    oss << "attr" << (index+1);
    return oss.str();
  }

  /**
   * @brief Get the type of the specified output column.
   *
   * @param index The index of the output column.
   * @param The type of the specified output column
   */
  virtual const Type &getOutputColumnType(int index) const = 0;

  /**
   * @brief Populate the given ColumnVectorsValueAccessor with data.
   *
   * @param results A mutable ColumnVectorsValueAccessor object that will be
   *        populated with data columns.
   */
  virtual void populateColumns(ColumnVectorsValueAccessor *results) const = 0;

  /**
   * @brief Get the name of this generator function.
   *
   * @return The name of this generator function.
   **/
  const std::string &getName() const {
    return func_name_;
  }

  /**
   * @brief Get the serialized protocol buffer representation of this generator
   *        function handle.
   * @note To alleviate the burden of writing serialization for each
   *       implemenation of generator functions, the serialization will only
   *       take the function name and the original arguments. Then, the
   *       function handle will be regenerated by GeneratorFunctionFactory
   *       from the function name and arguemtns at the destination site.
   *
   * @return A serialized protocol buffer representation of this generator
   *         function handle.
   **/
  serialization::GeneratorFunctionHandle getProto() const {
    serialization::GeneratorFunctionHandle proto;
    proto.set_function_name(func_name_);

    for (const TypedValue &arg : orig_args_) {
      proto.add_args()->CopyFrom(arg.getProto());
    }
    return proto;
  }

 protected:
  GeneratorFunctionHandle(const std::string &func_name,
                          const std::vector<TypedValue> &orig_args)
      : func_name_(func_name),
        orig_args_(orig_args) {
  }

 private:
  std::string func_name_;
  std::vector<TypedValue> orig_args_;

  DISALLOW_COPY_AND_ASSIGN(GeneratorFunctionHandle);
};

/** @} */

}  // namespace quickstep

#endif /* QUICKSTEP_EXPRESSIONS_TABLE_GENERATOR_GENERATOR_FUNCTION_HANDLE_HPP_ */
