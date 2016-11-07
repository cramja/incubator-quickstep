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

#ifndef QUICKSTEP_STORAGE_INSERT_DESTINATION_HPP_
#define QUICKSTEP_STORAGE_INSERT_DESTINATION_HPP_

#include <cstddef>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogRelationSchema.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "catalog/PartitionSchemeHeader.hpp"
#include "query_execution/QueryExecutionMessages.pb.h"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_execution/QueryExecutionUtil.hpp"
#include "storage/InsertDestinationInterface.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "threading/SpinMutex.hpp"
#include "threading/ThreadIDBasedMap.hpp"
#include "types/containers/Tuple.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

#include "gtest/gtest_prod.h"

#include "tmb/id_typedefs.h"
#include "tmb/tagged_message.h"

namespace tmb { class MessageBus; }

namespace quickstep {

class StorageManager;
class ValueAccessor;

namespace merge_run_operator {
class RunCreator;
}  // namespace merge_run_operator

namespace serialization { class InsertDestination; }

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief Base class for different strategies for getting blocks to insert
 *        tuples into.
 **/
class InsertDestination : public InsertDestinationInterface {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation to insert tuples into.
   * @param layout The layout to use for any newly-created blocks. If NULL,
   *        defaults to relation's default layout.
   * @param storage_manager The StorageManager to use.
   * @param relational_op_index The index of the relational operator in the
   *        QueryPlan DAG that has outputs.
   * @param query_id The ID of this query.
   * @param scheduler_client_id The TMB client ID of the scheduler thread.
   * @param bus A pointer to the TMB.
   **/
  InsertDestination(const CatalogRelationSchema &relation,
                    const StorageBlockLayout *layout,
                    StorageManager *storage_manager,
                    const std::size_t relational_op_index,
                    const std::size_t query_id,
                    const tmb::client_id scheduler_client_id,
                    tmb::MessageBus *bus);

  /**
   * @brief Virtual destructor.
   **/
  virtual ~InsertDestination() {
  }

  /**
   * @brief A factory method to generate the InsertDestination from the
   *        serialized Protocol Buffer representation.
   *
   * @param query_id The ID of this query.
   * @param proto A serialized Protocol Buffer representation of an
   *        InsertDestination, originally generated by the optimizer.
   * @param relation The relation to insert tuples into.
   * @param storage_manager The StorageManager to use.
   * @param scheduler_client_id The TMB client ID of the scheduler thread.
   * @param bus A pointer to the TMB.
   *
   * @return The constructed InsertDestination.
   */
  static InsertDestination* ReconstructFromProto(
      const std::size_t query_id,
      const serialization::InsertDestination &proto,
      const CatalogRelationSchema &relation,
      StorageManager *storage_manager,
      const tmb::client_id scheduler_client_id,
      tmb::MessageBus *bus);

  /**
   * @brief Check whether a serialized InsertDestination is fully-formed and
   *        all parts are valid.
   *
   * @param proto A serialized Protocol Buffer representation of an
   *        InsertDestination, originally generated by the optimizer.
   * @param relation The relation to insert tuples into.
   *
   * @return Whether proto is fully-formed and valid.
   **/
  static bool ProtoIsValid(const serialization::InsertDestination &proto,
                           const CatalogRelationSchema &relation);

  const CatalogRelationSchema& getRelation() const override {
    return relation_;
  }

  attribute_id getPartitioningAttribute() const override {
    return -1;
  }

  void insertTuple(const Tuple &tuple) override;

  void insertTupleInBatch(const Tuple &tuple) override;

  void bulkInsertTuples(ValueAccessor *accessor, bool always_mark_full = false) override;

  void bulkInsertTuplesWithRemappedAttributes(
      const std::vector<attribute_id> &attribute_map,
      ValueAccessor *accessor,
      bool always_mark_full = false) override;

  void bulkInsertTuplesFromValueAccessors(
      const std::vector<std::pair<ValueAccessor *, std::vector<attribute_id>>> &accessor_attribute_map,
      bool always_mark_full = false) override;

  void insertTuplesFromVector(std::vector<Tuple>::const_iterator begin,
                              std::vector<Tuple>::const_iterator end) override;

  /**
   * @brief Get the set of blocks that were used by clients of this
   *        InsertDestination for insertion.
   * @warning Should only be called AFTER this InsertDestination will no longer
   *          be used, and all blocks have been returned to it via
   *          returnBlock().
   *
   * @return A reference to a vector of block_ids of blocks that were used for
   *         insertion.
   **/
  const std::vector<block_id>& getTouchedBlocks() {
    SpinMutexLock lock(mutex_);
    return getTouchedBlocksInternal();
  }

  /**
   * @brief Get the set of blocks that were partially filled by clients of this
   *        InsertDestination for insertion.
   * @warning Should only be called AFTER this InsertDestination will no longer
   *          be used, and all blocks have been returned to it via
   *          returnBlock() and BEFORE getTouchedBlocks() is called, at all.
   *
   * @param partial_blocks A pointer to the vector of block IDs in which the
   *                       partially filled block IDs will be added.
   **/
  virtual void getPartiallyFilledBlocks(std::vector<MutableBlockReference> *partial_blocks) = 0;

 protected:
  /**
   * @brief Get a block to use for insertion.
   *
   * @return A block to use for inserting tuples.
   **/
  virtual MutableBlockReference getBlockForInsertion() = 0;

  /**
   * @brief Release a block after done using it for insertion.
   * @note This should ALWAYS be called when done inserting into a block.
   *
   * @param block A block, originally supplied by getBlockForInsertion(),
   *        which the client is finished using.
   * @param full If true, the client ran out of space when trying to insert
   *        into block. If false, all inserts were successful.
   **/
  virtual void returnBlock(MutableBlockReference &&block, const bool full) = 0;

  // TODO(chasseur): Once StorageManager is threadsafe, it will be safe to use
  // this without holding the mutex.
  virtual MutableBlockReference createNewBlock() = 0;

  virtual const std::vector<block_id>& getTouchedBlocksInternal() = 0;

  /**
   * @brief When a StorageBlock becomes full, pipeline the block id to the
   *        scheduler.
   *
   * @param id The id of the StorageBlock to be pipelined.
   **/
  void sendBlockFilledMessage(const block_id id) const {
    serialization::DataPipelineMessage proto;
    proto.set_operator_index(relational_op_index_);
    proto.set_block_id(id);
    proto.set_relation_id(relation_.getID());
    proto.set_query_id(query_id_);

    // NOTE(zuyu): Using the heap memory to serialize proto as a c-like string.
    const std::size_t proto_length = proto.ByteSize();
    char *proto_bytes = static_cast<char*>(std::malloc(proto_length));
    CHECK(proto.SerializeToArray(proto_bytes, proto_length));

    tmb::TaggedMessage tagged_message(static_cast<const void *>(proto_bytes),
                                      proto_length,
                                      kDataPipelineMessage);
    std::free(proto_bytes);

    // The reason we use the ClientIDMap is as follows:
    // InsertDestination needs to send data pipeline messages to scheduler. To
    // send a TMB message, we need to know the sender and receiver's TMB client
    // ID. In this case, the sender thread is the worker thread that executes
    // this function. To figure out the TMB client ID of the executing thread,
    // there are multiple ways :
    // 1. Trickle down the worker's client ID all the way from Worker::run()
    // method until here.
    // 2. Use thread-local storage - Each worker saves its TMB client ID in the
    // local storage.
    // 3. Use a globally accessible map whose key is the caller thread's
    // process level ID and value is the TMB client ID.
    //
    // Option 1 involves modifying the signature of several functions across
    // different modules. Option 2 was difficult to implement given that Apple's
    // Clang doesn't allow C++11's thread_local keyword. Therefore we chose
    // option 3.
    DCHECK(bus_ != nullptr);

    DLOG(INFO) << "InsertDestination sent DataPipelineMessage (typed '" << kDataPipelineMessage
               << "') to Scheduler with TMB client ID " << scheduler_client_id_;
    const tmb::MessageBus::SendStatus send_status =
        QueryExecutionUtil::SendTMBMessage(bus_,
                                           thread_id_map_.getValue(),
                                           scheduler_client_id_,
                                           std::move(tagged_message));
    CHECK(send_status == tmb::MessageBus::SendStatus::kOK);
  }

  inline const std::size_t getQueryID() const {
    return query_id_;
  }

  const ClientIDMap &thread_id_map_;

  StorageManager *storage_manager_;
  const CatalogRelationSchema &relation_;

  std::unique_ptr<const StorageBlockLayout> layout_;
  const std::size_t relational_op_index_;
  const std::size_t query_id_;

  tmb::client_id scheduler_client_id_;
  tmb::MessageBus *bus_;

  // TODO(chasseur): If contention is high, finer-grained locking of internal
  // data members in subclasses is possible.
  SpinMutex mutex_;

 private:
  // TODO(shoban): Workaround to support sort. Sort needs finegrained control of
  // blocks being used to insert, since inserting in an arbitrary block could
  // lead to unsorted results. InsertDestination API changed while sort was
  // being implemented.
  friend class merge_run_operator::RunCreator;

  DISALLOW_COPY_AND_ASSIGN(InsertDestination);
};

/**
 * @brief Implementation of InsertDestination that always creates new blocks,
 *        leaving some blocks potentially very underfull.
 **/
class AlwaysCreateBlockInsertDestination : public InsertDestination {
 public:
  AlwaysCreateBlockInsertDestination(const CatalogRelationSchema &relation,
                                     const StorageBlockLayout *layout,
                                     StorageManager *storage_manager,
                                     const std::size_t relational_op_index,
                                     const std::size_t query_id,
                                     const tmb::client_id scheduler_client_id,
                                     tmb::MessageBus *bus)
      : InsertDestination(relation,
                          layout,
                          storage_manager,
                          relational_op_index,
                          query_id,
                          scheduler_client_id,
                          bus) {}

  ~AlwaysCreateBlockInsertDestination() override {
  }

  void bulkInsertTuplesFromValueAccessors(
      const std::vector<std::pair<ValueAccessor *, std::vector<attribute_id>>> &accessor_attribute_map,
      bool always_mark_full = false) override  {
    LOG(FATAL) << "bulkInsertTuplesFromValueAccessors is not implemented for AlwaysCreateBlockInsertDestination";
  }

 protected:
  MutableBlockReference getBlockForInsertion() override;

  void returnBlock(MutableBlockReference &&block, const bool full) override;

  MutableBlockReference createNewBlock() override;

  const std::vector<block_id>& getTouchedBlocksInternal() override {
    return returned_block_ids_;
  }

  void getPartiallyFilledBlocks(std::vector<MutableBlockReference> *partial_blocks) override {
  }

 private:
  std::vector<block_id> returned_block_ids_;

  DISALLOW_COPY_AND_ASSIGN(AlwaysCreateBlockInsertDestination);
};

/**
 * @brief Implementation of InsertDestination that keeps a pool of
 *        partially-full blocks. Creates new blocks as necessary when
 *        getBlockForInsertion() is called and there are no partially-full
 *        blocks from the pool which are not "checked out" by workers.
 **/
class BlockPoolInsertDestination : public InsertDestination {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation to insert tuples into.
   * @param layout The layout to use for any newly-created blocks. If NULL,
   *        defaults to relation's default layout.
   * @param storage_manager The StorageManager to use.
   * @param relational_op_index The index of the relational operator in the
   *        QueryPlan DAG that has outputs.
   * @param scheduler_client_id The TMB client ID of the scheduler thread.
   * @param query_id The ID of the query.
   * @param bus A pointer to the TMB.
   **/
  BlockPoolInsertDestination(const CatalogRelationSchema &relation,
                             const StorageBlockLayout *layout,
                             StorageManager *storage_manager,
                             const std::size_t relational_op_index,
                             const std::size_t query_id,
                             const tmb::client_id scheduler_client_id,
                             tmb::MessageBus *bus)
      : InsertDestination(relation,
                          layout,
                          storage_manager,
                          relational_op_index,
                          query_id,
                          scheduler_client_id,
                          bus) {}

  /**
   * @brief Constructor.
   *
   * @param relation The relation to insert tuples into.
   * @param layout The layout to use for any newly-created blocks. If NULL,
   *        defaults to relation's default layout.
   * @param storage_manager The StorageManager to use.
   * @blocks The existing blocks used for insertions.
   * @param relational_op_index The index of the relational operator in the
   *        QueryPlan DAG that has outputs.
   * @param scheduler_client_id The TMB client ID of the scheduler thread.
   * @param bus A pointer to the TMB.
   **/
  BlockPoolInsertDestination(const CatalogRelationSchema &relation,
                             const StorageBlockLayout *layout,
                             StorageManager *storage_manager,
                             std::vector<block_id> &&blocks,
                             const std::size_t relational_op_index,
                             const std::size_t query_id,
                             const tmb::client_id scheduler_client_id,
                             tmb::MessageBus *bus)
      : InsertDestination(relation,
                          layout,
                          storage_manager,
                          relational_op_index,
                          query_id,
                          scheduler_client_id,
                          bus),
        available_block_ids_(std::move(blocks)) {
    // TODO(chasseur): Once block fill statistics are available, replace this
    // with something smarter.
  }

  ~BlockPoolInsertDestination() override {
  }

 protected:
  MutableBlockReference getBlockForInsertion() override;

  void returnBlock(MutableBlockReference &&block, const bool full) override;

  void getPartiallyFilledBlocks(std::vector<MutableBlockReference> *partial_blocks) override;

  const std::vector<block_id>& getTouchedBlocksInternal() override;

  MutableBlockReference createNewBlock() override;

 private:
  FRIEND_TEST(QueryManagerTest, TwoNodesDAGPartiallyFilledBlocksTest);

  // A vector of references to blocks which are loaded in memory.
  std::vector<MutableBlockReference> available_block_refs_;
  // A vector of blocks from the relation that are not loaded in memory yet.
  std::vector<block_id> available_block_ids_;
  // A vector of fully filled blocks.
  std::vector<block_id> done_block_ids_;

  DISALLOW_COPY_AND_ASSIGN(BlockPoolInsertDestination);
};


class PartitionAwareInsertDestination : public InsertDestination {
 public:
  /**
   * @brief Constructor.
   *
   * @note PartitionAwareInsertDestination takes ownership of \c
   *       partition_scheme_header.
   *
   * @param partition_scheme_header The partitioned scheme header information.
   * @param storage_manager The StorageManager to use.
   * @param relation The relation to insert tuples into.
   * @param layout The layout to use for any newly-created blocks. If NULL,
   *        defaults to relation's default layout.
   * @param partitions The blocks in partitions.
   * @param relational_op_index The index of the relational operator in the
   *        QueryPlan DAG that has outputs.
   * @param query_id The ID of the query.
   * @param scheduler_client_id The TMB client ID of the scheduler thread.
   * @param bus A pointer to the TMB.
   **/
  PartitionAwareInsertDestination(
      PartitionSchemeHeader *partition_scheme_header,
      const CatalogRelationSchema &relation,
      const StorageBlockLayout *layout,
      StorageManager *storage_manager,
      std::vector<std::vector<block_id>> &&partitions,
      const std::size_t relational_op_index,
      const std::size_t query_id,
      const tmb::client_id scheduler_client_id,
      tmb::MessageBus *bus);

  ~PartitionAwareInsertDestination() override {
    delete[] mutexes_for_partition_;
  }

  /**
   * @brief Manually add a block to the pool.
   * @warning Call only ONCE for each block to add to the pool.
   *
   * @param bid The ID of the block to add to the pool.
   * @part_id The partition to add the block to.
   **/
  void addBlockToPool(const block_id bid, const partition_id part_id) {
    SpinMutexLock lock(mutexes_for_partition_[part_id]);
    available_block_ids_[part_id].push_back(bid);
  }

  void getPartiallyFilledBlocks(std::vector<MutableBlockReference> *partial_blocks) override {
    // Iterate through each partition and return the partially filled blocks
    // in each partition.
    for (partition_id part_id = 0; part_id < partition_scheme_header_->getNumPartitions(); ++part_id) {
      getPartiallyFilledBlocksInPartition(partial_blocks, part_id);
    }
  }

  /**
   * @brief Get the set of blocks that were partially filled by clients of this
   *        InsertDestination for insertion.
   * @warning Should only be called AFTER this InsertDestination will no longer
   *          be used, and all blocks have been returned to it via
   *          returnBlock() and BEFORE getTouchedBlocks() is called, at all.
   *
   * @param partial_blocks A pointer to the vector of block IDs in which the
   *                       partially filled block IDs will be added.
   * @param part_id The partition id for which we want the partially filled blocks.
   **/
  void getPartiallyFilledBlocksInPartition(std::vector<MutableBlockReference> *partial_blocks, partition_id part_id) {
    SpinMutexLock lock(mutexes_for_partition_[part_id]);
    for (std::vector<MutableBlockReference>::size_type i = 0; i < available_block_refs_[part_id].size(); ++i) {
      partial_blocks->push_back((std::move(available_block_refs_[part_id][i])));
    }
    available_block_refs_[part_id].clear();
  }

  attribute_id getPartitioningAttribute() const override;

  void insertTuple(const Tuple &tuple) override;

  void insertTupleInBatch(const Tuple &tuple) override;

  void bulkInsertTuples(ValueAccessor *accessor, bool always_mark_full = false) override;

  void bulkInsertTuplesWithRemappedAttributes(
      const std::vector<attribute_id> &attribute_map,
      ValueAccessor *accessor,
      bool always_mark_full = false) override;

  void bulkInsertTuplesFromValueAccessors(
      const std::vector<std::pair<ValueAccessor *, std::vector<attribute_id>>> &accessor_attribute_map,
      bool always_mark_full = false) override  {
    LOG(FATAL) << "bulkInsertTuplesFromValueAccessors is not implemented for PartitionAwareInsertDestination";
  }

  void insertTuplesFromVector(std::vector<Tuple>::const_iterator begin,
                              std::vector<Tuple>::const_iterator end) override;

 protected:
  MutableBlockReference getBlockForInsertion() override;

  /**
   * @brief Get a block to use for insertion from a partition.
   *
   * @param part_id The partition id for which the client requests a block from.
   *
   * @return A block to use for inserting tuples belonging to a particular partition.
   **/
  MutableBlockReference getBlockForInsertionInPartition(const partition_id part_id);

  void returnBlock(MutableBlockReference &&block, const bool full) override;

  /**
   * @brief Release a block after done using it for insertion.
   * @note This should ALWAYS be called when done inserting into a block.
   *
   * @param block A block, originally supplied by getBlockForInsertion(),
   *        which the client is finished using.
   * @param full If true, the client ran out of space when trying to insert
   *        into block. If false, all inserts were successful.
   * @param part_id The partition id into which we should return the block into.
   **/
  void returnBlockInPartition(MutableBlockReference &&block, const bool full, const partition_id part_id);

  MutableBlockReference createNewBlock() override;
  MutableBlockReference createNewBlockInPartition(const partition_id part_id);

  const std::vector<block_id>& getTouchedBlocksInternal() override;
  const std::vector<block_id>& getTouchedBlocksInternalInPartition(partition_id part_id);

 private:
  std::unique_ptr<const PartitionSchemeHeader> partition_scheme_header_;

  // A vector of available block references for each partition.
  std::vector< std::vector<MutableBlockReference> > available_block_refs_;
  // A vector of available block ids for each partition.
  std::vector< std::vector<block_id> > available_block_ids_;
  // A vector of done block ids for each partition.
  std::vector< std::vector<block_id> > done_block_ids_;
  // Done block ids across all partitions.
  std::vector<block_id> all_partitions_done_block_ids_;
  // Mutex for locking each partition separately.
  SpinMutex *mutexes_for_partition_;

  DISALLOW_COPY_AND_ASSIGN(PartitionAwareInsertDestination);
};
/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_INSERT_DESTINATION_HPP_
