//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_tuner.h
//
// Identification: src/include/brain/index_tuner.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "common/types.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class DataTable;
}

namespace brain {

class Sample;

//===--------------------------------------------------------------------===//
// Index Tuner
//===--------------------------------------------------------------------===//

class IndexTuner {
 public:
  IndexTuner(const IndexTuner &) = delete;
  IndexTuner &operator=(const IndexTuner &) = delete;
  IndexTuner(IndexTuner &&) = delete;
  IndexTuner &operator=(IndexTuner &&) = delete;

  IndexTuner();

  ~IndexTuner();

  // Singleton
  static IndexTuner &GetInstance();

  // Start tuning
  void Start();

  // Tune layout
  void Tune();

  // Stop tuning
  void Stop();

  // Add table to list of tables whose layout must be tuned
  void AddTable(storage::DataTable *table);

  // Add indexes to table
  void AddIndexes(storage::DataTable* table,
                  const std::vector<std::vector<double>>& suggested_indices);

  // Clear list
  void ClearTables();

  void SetSampleCountThreshold(const oid_t &sample_count_threshold_) {
    sample_count_threshold = sample_count_threshold_;
  }

  void SetMaxTileGroupsIndexed(const oid_t &max_tile_groups_indexed_) {
    max_tile_groups_indexed = max_tile_groups_indexed_;
  }

  void SetIndexUtilityThreshold(const double& index_utility_threshold_){
    index_utility_threshold = index_utility_threshold_;
  }

  void SetIndexCountThreshold(const oid_t& index_count_threshold_){
    index_count_threshold = index_count_threshold_;
  }

  void SetWriteRatioThreshold(const double& write_ratio_threshold_){
    write_ratio_threshold = write_ratio_threshold_;
  }

  // Get # of indexes in managed tables
  oid_t GetIndexCount() const;

 protected:
  // Index tuning helper
  void IndexTuneHelper(storage::DataTable *table);

  void BuildIndex(storage::DataTable *table,
                  std::shared_ptr<index::Index> index);

  void BuildIndices(storage::DataTable *table);

  void Analyze(storage::DataTable *table);

  double ComputeWorkloadWriteRatio(const std::vector<brain::Sample> &samples);

  void DropIndexes(storage::DataTable *table);

 private:
  // Tables whose indices must be tuned
  std::vector<storage::DataTable *> tables;

  std::mutex index_tuner_mutex;

  // Stop signal
  std::atomic<bool> index_tuning_stop;

  // Tuner thread
  std::thread index_tuner_thread;

  //===--------------------------------------------------------------------===//
  // Tuner Parameters
  //===--------------------------------------------------------------------===//

  // Sleeping period (in us)
  oid_t sleep_duration = 10;

  // Threshold sample count
  // LEARNING RATE
  oid_t sample_count_threshold = 10;

  // # of tile groups to be indexed per iteration
  // INDEX CONSTRUCTION SPEED
  oid_t max_tile_groups_indexed = 20;

  // alpha (weight for old samples)
  double alpha = 0.2;

  // average write ratio
  double average_write_ratio = INVALID_RATIO;

  //===--------------------------------------------------------------------===//
  // DROP Thresholds
  //===--------------------------------------------------------------------===//

  // index utility threshold below which it will be dropped
  double index_utility_threshold = 0.2;

  // maximum # of indexes per table
  oid_t index_count_threshold = 10;

  // write intensive workload ratio threshold
  double write_ratio_threshold = 0.8;

};

}  // End brain namespace
}  // End peloton namespace
