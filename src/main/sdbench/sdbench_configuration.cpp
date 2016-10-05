//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sdbench_configuration.cpp
//
// Identification: src/main/sdbench/sdbench_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>

#include "benchmark/sdbench/sdbench_configuration.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

void Usage() {
  LOG_INFO("\n"
      "Command line options : sdbench <options>\n"
      "   -h --help                  :  Print help message\n"
      "   -f --index_usage_type      :  Types of indexes used\n"
      "   -c --query_complexity_type :  Complexity of query\n"
      "   -k --scale-factor          :  # of tile groups\n"
      "   -a --attribute_count       :  # of attributes\n"
      "   -w --write_ratio           :  Fraction of writes\n"
      "   -g --tuples_per_tg         :  # of tuples per tilegroup\n"
      "   -t --phase_length          :  Length of a phase\n"
      "   -q --total_ops             :  Total # of ops, specify -1 to run until converge\n"
      "   -s --selectivity           :  Selectivity\n"
      "   -p --projectivity          :  Projectivity\n"
      "   -l --layout                :  Layout\n"
      "   -v --verbose               :  Output verbosity\n"
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"index_usage_type", optional_argument, NULL, 'f'},
    {"query_complexity_type", optional_argument, NULL, 'c'},
    {"scale-factor", optional_argument, NULL, 'k'},
    {"attribute_count", optional_argument, NULL, 'a'},
    {"write_ratio", optional_argument, NULL, 'w'},
    {"tuples_per_tg", optional_argument, NULL, 'g'},
    {"phase_length", optional_argument, NULL, 't'},
    {"total_ops", optional_argument, NULL, 'q'},
    {"selectivity", optional_argument, NULL, 's'},
    {"projectivity", optional_argument, NULL, 'p'},
    {"layout", optional_argument, NULL, 'l'},
    {"verbose", optional_argument, NULL, 'v'},
    {NULL, 0, NULL, 0}
};

void GenerateSequence(oid_t column_count) {
  // Reset sequence
  sdbench_column_ids.clear();

  // Generate sequence
  for (oid_t column_id = 1; column_id <= column_count; column_id++)
    sdbench_column_ids.push_back(column_id);

  std::random_shuffle(sdbench_column_ids.begin(), sdbench_column_ids.end());
}


static void ValidateIndexUsageType(const configuration &state) {
  if (state.index_usage_type < 1 || state.index_usage_type > 3) {
      LOG_ERROR("Invalid index_usage_type :: %d", state.index_usage_type);
      exit(EXIT_FAILURE);
  }
  else {
      switch (state.index_usage_type) {
        case INDEX_USAGE_TYPE_INCREMENTAL:
          LOG_INFO("%s : INCREMENTAL", "index_usage_type ");
          break;
        case INDEX_USAGE_TYPE_FULL:
          LOG_INFO("%s : FULL", "index_usage_type ");
          break;
        case INDEX_USAGE_TYPE_NEVER:
          LOG_INFO("%s : NEVER", "index_usage_type ");
          break;
        default:
          break;
      }
    }
}

static void ValidateQueryComplexityType(const configuration &state) {
  if (state.query_complexity_type < 1 || state.query_complexity_type > 3) {
      LOG_ERROR("Invalid query_complexity_type :: %d", state.query_complexity_type);
      exit(EXIT_FAILURE);
    } else {
      switch (state.query_complexity_type) {
        case QUERY_COMPLEXITY_TYPE_SIMPLE:
          LOG_INFO("%s : SIMPLE", "query_complexity_type ");
          break;
        case QUERY_COMPLEXITY_TYPE_MODERATE:
          LOG_INFO("%s : MODERATE", "query_complexity_type ");
          break;
        case QUERY_COMPLEXITY_TYPE_COMPLEX:
          LOG_INFO("%s : COMPLEX", "query_complexity_type ");
          break;
        default:
          break;
      }
    }
}

static void ValidateScaleFactor(const configuration &state) {
  if (state.scale_factor <= 0) {
    LOG_ERROR("Invalid scale_factor :: %d", state.scale_factor);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "scale_factor", state.scale_factor);
}

static void ValidateLayout(const configuration &state) {
  if (state.layout_mode < 1 || state.layout_mode > 3) {
    LOG_ERROR("Invalid layout :: %d", state.layout_mode);
    exit(EXIT_FAILURE);
  } else {
    switch (state.layout_mode) {
      case LAYOUT_TYPE_ROW:
        LOG_INFO("%s : ROW", "layout ");
        break;
      case LAYOUT_TYPE_COLUMN:
        LOG_INFO("%s : COLUMN", "layout ");
        break;
      case LAYOUT_TYPE_HYBRID:
        LOG_INFO("%s : HYBRID", "layout ");
        break;
      default:
        break;
    }
  }
}

static void ValidateProjectivity(const configuration &state) {
  if (state.projectivity < 0 || state.projectivity > 1) {
    LOG_ERROR("Invalid projectivity :: %.1lf", state.projectivity);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %.3lf", "projectivity", state.projectivity);
}

static void ValidateSelectivity(const configuration &state) {
  if (state.selectivity < 0 || state.selectivity > 1) {
    LOG_ERROR("Invalid selectivity :: %.1lf", state.selectivity);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %.3lf", "selectivity", state.selectivity);
}

static void ValidateAttributeCount(const configuration &state) {
  if (state.attribute_count <= 0) {
    LOG_ERROR("Invalid column_count :: %d", state.attribute_count);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "column_count", state.attribute_count);
}

static void ValidateWriteRatio(const configuration &state) {
  if (state.write_ratio < 0 || state.write_ratio > 1) {
    LOG_ERROR("Invalid write_ratio :: %.1lf", state.write_ratio);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %.1lf", "write_ratio", state.write_ratio);
}

static void ValidateTotalOps(const configuration &state) {
  if (state.total_ops <= 0 && state.total_ops != -1) {
    LOG_ERROR("Invalid total_ops :: %lu", state.total_ops);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %ld", "total_ops", state.total_ops);
}

static void ValidatePhaseLength(const configuration &state) {
  if (state.phase_length <= 0) {
    LOG_ERROR("Invalid phase_length :: %lu", state.phase_length);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %lu", "phase_length", state.phase_length);
}

static void ValidateTuplesPerTileGroup(const configuration &state) {
  if (state.tuples_per_tilegroup <= 0) {
    LOG_ERROR("Invalid tuples_per_tilegroup :: %d", state.tuples_per_tilegroup);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %d", "tuples_per_tilegroup", state.tuples_per_tilegroup);
}

void ParseArguments(int argc, char *argv[], configuration &state) {

  // Default Values
  state.index_usage_type = INDEX_USAGE_TYPE_INCREMENTAL;
  state.query_complexity_type = QUERY_COMPLEXITY_TYPE_SIMPLE;

  state.scale_factor = 100.0;
  state.attribute_count = 20;

  state.write_ratio = 0.0;
  state.tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP;

  state.total_ops = 1;
  state.phase_length = 1;

  state.selectivity = 0.001;
  state.projectivity = 1.0;
  state.layout_mode = LAYOUT_TYPE_ROW;

  state.adapt_layout = false;
  state.adapt_indexes = true;

  state.verbose = false;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "hf:c:k:a:w:g:y:q:t:s:p:l:v:", opts, &idx);

    if (c == -1) break;

    switch (c) {
      case 'f':
        state.index_usage_type = (IndexUsageType) atoi(optarg);
        break;
      case 'c':
        state.query_complexity_type = (QueryComplexityType) atoi(optarg);
        break;
      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 'a':
        state.attribute_count = atoi(optarg);
        break;
      case 'w':
        state.write_ratio = atof(optarg);
        break;
      case 'g':
        state.tuples_per_tilegroup = atoi(optarg);
        break;
      case 'q':
        state.total_ops = atol(optarg);
        break;
      case 't':
        state.phase_length = atol(optarg);
        break;
      case 's':
        state.selectivity = atof(optarg);
        break;
      case 'p':
        state.projectivity = atof(optarg);
        break;
      case 'l':
        state.layout_mode = (LayoutType)atoi(optarg);
        break;
      case 'v':
        state.verbose = atoi(optarg);
        break;

      case 'h':
        Usage();
        break;

      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage();
    }
  }

  ValidateIndexUsageType(state);
  ValidateQueryComplexityType(state);
  ValidateScaleFactor(state);
  ValidateAttributeCount(state);
  ValidateWriteRatio(state);
  ValidateTuplesPerTileGroup(state);
  ValidateTotalOps(state);
  ValidatePhaseLength(state);
  ValidateSelectivity(state);
  ValidateProjectivity(state);
  ValidateLayout(state);

}

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
