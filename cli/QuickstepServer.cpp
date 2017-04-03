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

/* A simple server interface to QuickStep */

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cli/CliConfig.h"  // For QUICKSTEP_USE_LINENOISE, QUICKSTEP_ENABLE_GOOGLE_PROFILER.

#include "cli/CommandExecutor.hpp"
#include "cli/DropRelation.hpp"


#include "cli/LineReaderDumb.hpp"

#ifdef QUICKSTEP_ENABLE_GOOGLE_PROFILER
#include <gperftools/profiler.h>
#endif

#include "cli/DefaultsConfigurator.hpp"
#include "cli/Flags.hpp"
#include "cli/InputParserUtil.hpp"
#include "cli/NetworkWrapper.hpp"
#include "cli/PrintToScreen.hpp"
#include "parser/ParseStatement.hpp"
#include "parser/SqlParserWrapper.hpp"
#include "query_execution/ForemanSingleNode.hpp"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_execution/QueryExecutionUtil.hpp"
#include "query_execution/Worker.hpp"
#include "query_execution/WorkerDirectory.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "query_optimizer/QueryProcessor.hpp"
#include "storage/Flags.hpp"
#include "storage/StorageConfig.h"  // For QUICKSTEP_HAVE_FILE_MANAGER_HDFS.

#ifdef QUICKSTEP_HAVE_FILE_MANAGER_HDFS
#include "storage/FileManagerHdfs.hpp"
#endif

#include "storage/PreloaderThread.hpp"
#include "storage/StorageConstants.hpp"
#include "storage/StorageManager.hpp"
#include "threading/ThreadIDBasedMap.hpp"
#include "utility/ExecutionDAGVisualizer.hpp"
#include "utility/Macros.hpp"
#include "utility/MemStream.hpp"
#include "utility/PtrVector.hpp"
#include "utility/SqlError.hpp"
#include "utility/StringUtil.hpp"

#include "gflags/gflags.h"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"
#include "tmb/message_bus.h"

namespace quickstep {
  class CatalogRelation;
}

using std::fflush;
using std::fprintf;
using std::printf;
using std::string;
using std::vector;

using quickstep::CatalogRelation;
using quickstep::DefaultsConfigurator;
using quickstep::DropRelation;
using quickstep::FLAGS_num_workers;
using quickstep::FLAGS_storage_path;
using quickstep::ForemanSingleNode;
using quickstep::InputParserUtil;
using quickstep::MemStream;
using quickstep::MessageBusImpl;
using quickstep::ParseResult;
using quickstep::ParseStatement;
using quickstep::PrintToScreen;
using quickstep::PtrVector;
using quickstep::QueryExecutionUtil;
using quickstep::QueryHandle;
using quickstep::QueryProcessor;
using quickstep::SqlParserWrapper;
using quickstep::Worker;
using quickstep::WorkerDirectory;
using quickstep::kAdmitRequestMessage;
using quickstep::kCatalogFilename;
using quickstep::kPoisonMessage;
using quickstep::kWorkloadCompletionMessage;

using tmb::client_id;

namespace quickstep {

  DEFINE_string(profile_file_name, "",
                "If nonempty, enable profiling using GOOGLE CPU Profiler, and write "
                  "its output to the given file name. This flag has no effect if "
                  "ENABLE_GOOGLE_PROFILER CMake flag was not set during build. "
                  "The profiler only starts collecting samples after the first query, "
                  "so that it runs against a warm buffer pool and caches. If you want to profile "
                  "everything, including the first query run, set the "
                  "environment variable CPUPROFILE instead of passing this flag.");
  // Here's a detailed explanation of why we skip the first query run
  // during profiling:
  // Unless you’ve preloaded the buffer pool (which is not always a good
  // idea), the first run of the query results in disk I/O and other overhead
  // that significantly skews the profiling results. It’s the same reason we don’t
  // include the first run time in our benchmarking: when profiling query
  // execution, it makes more sense to get numbers using a warm buffer pool and
  // warm caches. This is not *always* the right thing to do: it’s obviously
  // wrong for profiling the TextScan operator. In those cases, you might want
  // to put in your own Profiler probes (just follow the start/stop pattern used
  // in this file) or just run quickstep with the CPUPROFILE environment variable
  // set (as per gperftools documentation) to get the full profile for the
  // entire execution.
  // To put things in perspective, the first run is, in my experiments, about 5-10
  // times more expensive than the average run. That means the query needs to be
  // run at least a hundred times to make the impact of the first run small (< 5 %).

  DECLARE_bool(profile_and_report_workorder_perf);
  DECLARE_bool(visualize_execution_dag);

}  // namespace quickstep

// returns a pair: <quitting, reset_parser>
std::pair<bool,bool> handleValidCommand(MessageBusImpl &bus,
                        const client_id main_thread_client_id,
                        quickstep::StorageManager &storage_manager,
                        std::unique_ptr<QueryProcessor> &query_processor,
                        const ForemanSingleNode &foreman,
                        ParseResult const &result,
                        FILE* outfile,
                        FILE* errfile,
                        std::string* command_string) {
  std::chrono::time_point<std::chrono::steady_clock> start, end;
  bool quitting = false;
  bool reset_parser = false;

  const ParseStatement &statement = *result.parsed_statement;
  switch (statement.getStatementType()) {
    case ParseStatement::kQuit: {
      quitting = true;
      break;
    }
    case ParseStatement::kCommand: {
      try {
        quickstep::cli::executeCommand(
          statement,
          *(query_processor->getDefaultDatabase()),
          main_thread_client_id,
          foreman.getBusClientID(),
          &bus,
          &storage_manager,
          query_processor.get(),
          outfile);
      } catch (const quickstep::SqlError &sql_error) {
        fprintf(errfile, "%s",
                sql_error.formatMessage(*command_string).c_str());
        reset_parser = true;
      }
      break;
    }
    default: {
      const size_t query_id = query_processor->query_id();
      const CatalogRelation *query_result_relation = nullptr;
      try {
        auto query_handle = std::__1::make_unique<QueryHandle>(query_id,
                                                               main_thread_client_id,
                                                               statement.getPriority());
        query_processor->generateQueryHandle(statement, query_handle.get());
        DCHECK(query_handle->getQueryPlanMutable() != nullptr);

        query_result_relation = query_handle->getQueryResultRelation();

        start = std::__1::chrono::steady_clock::now();
        QueryExecutionUtil::ConstructAndSendAdmitRequestMessage(
          main_thread_client_id,
          foreman.getBusClientID(),
          query_handle.release(),
          &bus);
      } catch (const quickstep::SqlError &sql_error) {
        fprintf(errfile, "%s", sql_error.formatMessage(*command_string).c_str());
        reset_parser = true;
        break;
      }

      try {
        QueryExecutionUtil::ReceiveQueryCompletionMessage(
          main_thread_client_id, &bus);
        end = std::__1::chrono::steady_clock::now();

        if (query_result_relation) {
          PrintToScreen::PrintRelation(*query_result_relation,
                                       &storage_manager,
                                       outfile);
          PrintToScreen::PrintOutputSize(
            *query_result_relation,
            &storage_manager,
            outfile);

          DropRelation::Drop(*query_result_relation,
                             query_processor->getDefaultDatabase(),
                             &storage_manager);
        }

        query_processor->saveCatalog();
        std::__1::chrono::duration<double, std::__1::milli> time_ms = end - start;
        fprintf(outfile,
                "Time: %s ms\n",
                quickstep::DoubleToStringWithSignificantDigits(
                  time_ms.count(), 3).c_str());
      } catch (const std::exception &e) {
        fprintf(errfile, "QUERY EXECUTION ERROR: %s\n", e.what());
        break;
      }
    }
  }
  return std::pair<bool,bool>(quitting, reset_parser);
}

void loopUntilQuit(MessageBusImpl &bus,
                   const client_id main_thread_client_id,
                   quickstep::StorageManager &storage_manager,
                   std::unique_ptr<QueryProcessor> &query_processor,
                   const ForemanSingleNode &foreman,
                   quickstep::NetworkWrapper &network_wrapper,
                   std::unique_ptr<SqlParserWrapper> &parser_wrapper) {
  for (;;) {
    string *command_string = new string();
    *command_string = network_wrapper.getNextCommand();
    if (command_string->size() == 0) {
      // empty line signals exit
      // TODO handle the network connection in this case
      delete command_string;
      break;
    }

    // each command gets a file. These files are in-memory, and the interface is not
    // POSIX standard. Therefore, we should change the interface in the future.
    LOG(INFO) << "Query received: " << command_string;
    MemStream outfile;
    MemStream errfile;
    parser_wrapper->feedNextBuffer(command_string);

    bool quitting = false;
    // A parse error should reset the parser. This is because the thrown quickstep
    // SqlError does not do the proper reset work of the YYABORT macro.
    bool reset_parser = false;
    for (;;) {
      ParseResult result = parser_wrapper->getNextStatement();
      if (result.condition == ParseResult::kSuccess) {
        std::pair<bool, bool> execution_result =
          handleValidCommand(bus,
                             main_thread_client_id,
                             storage_manager,
                             query_processor,
                             foreman,
                             result,
                             outfile.file(),
                             errfile.file(),
                             command_string);
        quitting = execution_result.first;
        reset_parser = execution_result.second;
      } else {
        // parse failed, or end of input
        if (result.condition == ParseResult::kError) {
          fprintf(errfile.file(), "%s", result.error_message.c_str());
        }
        reset_parser = true;
      }
      if (reset_parser || quitting)
        break;
    }

    // TODO formatting
    std::string output_message(outfile.str());
    std::string err_message(errfile.str());
    network_wrapper.returnResult(output_message + (err_message.length() > 1 ? "\n" + err_message : ""));

    if (quitting) {
      break;
    } else if (reset_parser) {
      parser_wrapper.reset(new SqlParserWrapper());
      reset_parser = false;
    }

    //delete command_string; // TODO Smart pointer
  }
}

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  printf("Starting Quickstep with %d worker thread(s) and a %.2f GB buffer pool.\n",
         FLAGS_num_workers,
         (static_cast<double>(quickstep::FLAGS_buffer_pool_slots) * quickstep::kSlotSizeBytes)/quickstep::kAGigaByte);

#ifdef QUICKSTEP_HAVE_FILE_MANAGER_HDFS
  if (quickstep::FLAGS_use_hdfs) {
    LOG(INFO) << "Using HDFS as the default persistent storage, with namenode at "
              << quickstep::FLAGS_hdfs_namenode_host << ":"
              << quickstep::FLAGS_hdfs_namenode_port << " and block replication factor "
              << quickstep::FLAGS_hdfs_num_replications << "\n";
  }
#endif

  // Initialize the thread ID based map here before the Foreman and workers are
  // constructed because the initialization isn't thread safe.
  quickstep::ClientIDMap::Instance();

  MessageBusImpl bus;
  bus.Initialize();

  // The TMB client id for the main thread, used to kill workers at the end.
  const client_id main_thread_client_id = bus.Connect();
  bus.RegisterClientAsSender(main_thread_client_id, kAdmitRequestMessage);
  bus.RegisterClientAsSender(main_thread_client_id, kPoisonMessage);
  bus.RegisterClientAsReceiver(main_thread_client_id, kWorkloadCompletionMessage);

  quickstep::StorageManager storage_manager(FLAGS_storage_path);

  string catalog_path(FLAGS_storage_path);
  catalog_path.append(kCatalogFilename);
  if (quickstep::FLAGS_initialize_db) {  // Initialize the database
    DefaultsConfigurator::InitializeDefaultDatabase(FLAGS_storage_path, catalog_path);
  }

  // Setup QueryProcessor, including CatalogDatabase.
  std::unique_ptr<QueryProcessor> query_processor;
  try {
    query_processor = std::make_unique<QueryProcessor>(std::move(catalog_path));
  } catch (const std::exception &e) {
    LOG(FATAL) << "FATAL ERROR DURING STARTUP: "
               << e.what()
               << "\nIf you intended to create a new database, "
               << "please use the \"-initialize_db=true\" command line option.";
  } catch (...) {
    LOG(FATAL) << "NON-STANDARD EXCEPTION DURING STARTUP";
  }

  // Parse the CPU affinities for workers and the preloader thread, if enabled
  // to warm up the buffer pool.
  const vector<int> worker_cpu_affinities =
    InputParserUtil::ParseWorkerAffinities(FLAGS_num_workers,
                                           quickstep::FLAGS_worker_affinities);

  const std::size_t num_numa_nodes_system = DefaultsConfigurator::GetNumNUMANodes();

  if (quickstep::FLAGS_preload_buffer_pool) {
    std::chrono::time_point<std::chrono::steady_clock> preload_start, preload_end;
    preload_start = std::chrono::steady_clock::now();
    printf("Preloading the buffer pool ... ");
    fflush(stdout);
    quickstep::PreloaderThread preloader(*query_processor->getDefaultDatabase(),
                                         &storage_manager,
                                         worker_cpu_affinities.front());

    preloader.start();
    preloader.join();
    preload_end = std::chrono::steady_clock::now();
    printf("in %g seconds\n",
           std::chrono::duration<double>(preload_end - preload_start).count());
  }

  // Get the NUMA affinities for workers.
  vector<int> cpu_numa_nodes = InputParserUtil::GetNUMANodesForCPUs();
  if (cpu_numa_nodes.empty()) {
    // libnuma is not present. Assign -1 as the NUMA node for every worker.
    cpu_numa_nodes.assign(worker_cpu_affinities.size(), -1);
  }

  vector<int> worker_numa_nodes;
  PtrVector<Worker> workers;
  vector<client_id> worker_client_ids;

  // Initialize the worker threads.
  DCHECK_EQ(static_cast<std::size_t>(FLAGS_num_workers),
            worker_cpu_affinities.size());
  for (std::size_t worker_thread_index = 0;
       worker_thread_index < worker_cpu_affinities.size();
       ++worker_thread_index) {
    int numa_node_id = -1;
    if (worker_cpu_affinities[worker_thread_index] >= 0) {
      // This worker can be NUMA affinitized.
      numa_node_id = cpu_numa_nodes[worker_cpu_affinities[worker_thread_index]];
    }
    worker_numa_nodes.push_back(numa_node_id);

    workers.push_back(
      new Worker(worker_thread_index, &bus, worker_cpu_affinities[worker_thread_index]));
    worker_client_ids.push_back(workers.back().getBusClientID());
  }

  WorkerDirectory worker_directory(worker_cpu_affinities.size(),
                                   worker_client_ids,
                                   worker_numa_nodes);

  ForemanSingleNode foreman(
    main_thread_client_id,
    &worker_directory,
    &bus,
    query_processor->getDefaultDatabase(),
    &storage_manager,
    -1,  // Don't pin the Foreman thread.
    num_numa_nodes_system);

  // Start the worker threads.
  for (Worker &worker : workers) {
    worker.start();
  }

  foreman.start();

  quickstep::NetworkWrapper network_wrapper(3000);

  std::unique_ptr<SqlParserWrapper> parser_wrapper(new SqlParserWrapper());

  loopUntilQuit(bus,
                main_thread_client_id,
                storage_manager,
                query_processor,
                foreman,
                network_wrapper,
                parser_wrapper);

  // Kill the foreman and workers.
  QueryExecutionUtil::BroadcastPoisonMessage(main_thread_client_id, &bus);

  for (Worker &worker : workers) {
    worker.join();
  }

  foreman.join();

  return 0;
}
