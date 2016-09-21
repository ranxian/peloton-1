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

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "storage/data_table.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

enum IndexUsageType {
  INDEX_USAGE_TYPE_INVALID = 0,

  INDEX_USAGE_TYPE_INCREMENTAL = 1, // use partial indexes
  INDEX_USAGE_TYPE_FULL = 2, // use full indexes
  INDEX_USAGE_TYPE_NEVER = 3 // don't use indexes (no online tuning)
};

enum QueryComplexityType {
  QUERY_COMPLEXITY_TYPE_INVALID = 0,

  QUERY_COMPLEXITY_TYPE_SIMPLE = 1,
  QUERY_COMPLEXITY_TYPE_MODERATE = 2,
  QUERY_COMPLEXITY_TYPE_COMPLEX = 3

};

extern int orig_scale_factor;

class configuration {
 public:
  // What kind of indexes can be used ?
  IndexUsageType index_usage_type;

  // Complexity of the query.
  QueryComplexityType query_complexity_type;

  // size of the table
  int scale_factor;

  int tuples_per_tilegroup;

  // tile group layout
  LayoutType layout_mode;

  double selectivity;

  double projectivity;

  // column count
  int attribute_count;

  // update ratio
  double write_ratio;

  // # of times to run operator
  std::size_t phase_length;

  // total number of ops
  std::size_t total_ops;

  // Adapt the layout ?
  bool adapt_layout;

  // Adapt the indexes ?
  bool adapt_indexes;

  // scan type
  HybridScanType hybrid_scan_type;

};

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void GenerateSequence(oid_t column_count);

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
