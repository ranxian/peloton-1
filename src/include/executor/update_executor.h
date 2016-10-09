//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.h
//
// Identification: src/include/executor/update_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/types.h"
#include "executor/abstract_executor.h"
#include "expression/abstract_expression.h"
#include "planner/update_plan.h"
#include "storage/tile.h"
#include "storage/tile_group_header.h"

namespace peloton {
namespace executor {

class UpdateExecutor : public AbstractExecutor {
  UpdateExecutor(const UpdateExecutor &) = delete;
  UpdateExecutor &operator=(const UpdateExecutor &) = delete;

 public:
  explicit UpdateExecutor(const planner::AbstractPlan *node,
                          ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

  void InplaceUpdate(storage::TileGroup *tile_group, ItemPointer location);

 private:
  storage::DataTable *target_table_ = nullptr;
  const planner::ProjectInfo *project_info_ = nullptr;
};

}  // namespace executor
}  // namespace peloton
