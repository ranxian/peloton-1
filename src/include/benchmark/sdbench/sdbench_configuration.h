//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_configuration.h
//
// Identification: src/include/benchmark/sdbench/sdbench_configuration.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <getopt.h>
#include <sys/time.h>
#include <iostream>
#include <string>
#include <vector>

#include "storage/data_table.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

enum IndexUsageType {
  INDEX_USAGE_TYPE_INVALID = 0,

  INDEX_USAGE_TYPE_AGGRESSIVE = 1,    // use partial indexes aggressively
  INDEX_USAGE_TYPE_BALANCED = 2,      // use partial indexes balanced
  INDEX_USAGE_TYPE_CONSERVATIVE = 3,  // use partial indexes conservatively
  INDEX_USAGE_TYPE_NEVER = 4,         // don't use indexes (no online tuning)

  INDEX_USAGE_TYPE_FULL = 5,  // only use full indexes
};

enum QueryComplexityType {
  QUERY_COMPLEXITY_TYPE_INVALID = 0,

  QUERY_COMPLEXITY_TYPE_SIMPLE = 1,
  QUERY_COMPLEXITY_TYPE_MODERATE = 2,
  QUERY_COMPLEXITY_TYPE_COMPLEX = 3

};

enum WriteComplexityType {
  WRITE_COMPLEXITY_TYPE_INVALID = 0,

  WRITE_COMPLEXITY_TYPE_SIMPLE = 1,
  WRITE_COMPLEXITY_TYPE_COMPLEX = 2
};

// Copy from types.h for reference
// typedef enum LayoutType {
//   LAYOUT_TYPE_INVALID = 0,
//   LAYOUT_TYPE_ROW = 1,    /* Pure row layout */
//   LAYOUT_TYPE_COLUMN = 2, /* Pure column layout */
//   LAYOUT_TYPE_HYBRID = 3  /* Hybrid layout */
// } LayoutType;

extern int orig_scale_factor;

static const int generator_seed = 50;

class configuration {
 public:
  // What kind of indexes can be used ?
  IndexUsageType index_usage_type;

  // Complexity of the query.
  QueryComplexityType query_complexity_type;

  // Complexity of update.
  WriteComplexityType write_complexity_type;

  // size of the table
  int scale_factor;

  int tuples_per_tilegroup;

  // tile group layout
  LayoutType layout_mode;

  double selectivity;

  double projectivity;

  // column count
  int attribute_count;

  // write ratio
  double write_ratio;

  // # of times to run operator
  std::size_t phase_length;

  // total number of ops
  long total_ops;

  // Verbose output
  bool verbose;

  // Convergence test?
  bool convergence;

  // INDEX TUNER PARAMETERS

  // sample count threshold after which "a" tuning iteration takes place
  oid_t sample_count_threshold;

  // max tile groups indexed per tuning iteration per table
  oid_t max_tile_groups_indexed;

  // CONVERGENCE PARAMETER

  // number of queries for which index configuration must remain stable
  oid_t convergence_query_threshold;

  // VARIABILITY PARAMETER
  oid_t variability_threshold;

  // DROP PARAMETER

  // index utility threshold
  double index_utility_threshold;

  // maximum # of indexes per table
  oid_t index_count_threshold;

  // write intensive workload ratio threshold
  double write_ratio_threshold;
};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void GenerateSequence(oid_t column_count);

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
