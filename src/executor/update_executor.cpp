//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.cpp
//
// Identification: src/executor/update_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/update_executor.h"
#include "catalog/manager.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "expression/container_tuple.h"
#include "planner/update_plan.h"
#include "storage/data_table.h"
#include "storage/rollback_segment.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for update executor.
 * @param node Update node corresponding to this executor.
 */
UpdateExecutor::UpdateExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DInit() {
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(target_table_ == nullptr);
  PL_ASSERT(project_info_ == nullptr);

  // Grab settings from node
  const planner::UpdatePlan &node = GetPlanNode<planner::UpdatePlan>();
  target_table_ = node.GetTable();
  project_info_ = node.GetProjectInfo();

  PL_ASSERT(target_table_);
  PL_ASSERT(project_info_);

  return true;
}

/**
 * @brief Do in place update for a specific tuple.
 * @details This function will update the tuple in place. The steps to perform
 * an inplace update will be:
 * 1. Evaluate the project_info to get the updated tuple
 * 2. Remove index entries for old tuple
 * 3. Copy the data of the updated tuple to the old tuple in place
 * 4. Add index entries for the new tuple
 *
 * Problems of this implementation:
 * 1. When the index entries of the old tuple are deleted, and before the index
 * for the new tuple is inserted, neither the new tuple or old tuple are
 * visible.
 * 2. When index tuner is building the index on the fly, it does not know that
 * some of the tuples might be already updated and hence in the index, so it
 * will still try to build index for these tuples. This will lead to multiple
 * index entries pointing to the same tuple.
 *
 * @param location [description]
 * @return [description]
 */
void UpdateExecutor::InplaceUpdate(storage::TileGroup *tile_group,
                                   ItemPointer location) {
  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();
  // Make a copy of the original tuple and allocate a new tuple
  expression::ContainerTuple<storage::TileGroup> old_tuple(tile_group,
                                                           location.offset);
  // Create a temp copy
  std::unique_ptr<storage::Tuple> new_tuple(
      new storage::Tuple(target_table_->GetSchema(), true));
  // Execute the projections
  // FIXME: reduce memory copy by doing inplace update
  project_info_->Evaluate(new_tuple.get(), &old_tuple, nullptr,
                          executor_context_);

  // Remove index entries for the old tuple and add index entries for the
  // new tuple.
  target_table_->DeleteInIndexes(&old_tuple, location);
  tile_group->CopyTuple(new_tuple.get(), location.offset);
  target_table_->InsertInIndexes(new_tuple.get(), location);

  transaction_manager.PerformUpdate(location);
}

/**
 * @brief updates a set of columns
 * @return true on success, false otherwise.
 */
bool UpdateExecutor::DExecute() {
  PL_ASSERT(children_.size() == 1);
  PL_ASSERT(executor_context_);

  // We are scanning over a logical tile.
  LOG_TRACE("Update executor :: 1 child ");

  if (!children_[0]->Execute()) {
    return false;
  }

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

  auto &pos_lists = source_tile.get()->GetPositionLists();
  storage::Tile *tile = source_tile->GetBaseTile(0);
  storage::TileGroup *tile_group = tile->GetTileGroup();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();
  auto tile_group_id = tile_group->GetTileGroupId();

  // Update tuples in given table
  for (oid_t visible_tuple_id : *source_tile) {
    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group_id, physical_tuple_id);

    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ",
              visible_tuple_id, physical_tuple_id);

    if (transaction_manager.IsOwner(tile_group_header, physical_tuple_id) ==
        true) {
      InplaceUpdate(tile_group, old_location);
    } else if (transaction_manager.IsOwnable(tile_group_header,
                                             physical_tuple_id) == true) {
      // if the tuple is not owned by any transaction and is visible to current
      // transaction.

      if (transaction_manager.AcquireOwnership(tile_group_header, tile_group_id,
                                               physical_tuple_id) == false) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
        return false;
      }
      // WARNING !!!!!!!!: DOING INPLACE UPDATE ONLY FOR INC BRANCH
      //////////////////////////////////////////////////////////////////
      InplaceUpdate(tile_group, old_location);
    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(Result::RESULT_FAILURE);
      return false;
    }
  }
  return true;
}

}  // namespace executor
}  // namespace peloton
