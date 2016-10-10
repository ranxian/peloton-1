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

#include <algorithm>
#include <iomanip>

#include "benchmark/sdbench/sdbench_configuration.h"
#include "common/logger.h"

namespace peloton {
namespace benchmark {
namespace sdbench {

void Usage() {
  LOG_INFO(
      "\n"
      "Command line options : sdbench <options>\n"
      "   -a --attribute_count               :  # of attributes\n"
      "   -b --convergence_query_threshold   :  # of queries for convergence\n"
      "   -c --query_complexity_type         :  Complexity of query\n"
      "   -d --variability_threshold         :  Variability threshold\n"
      "   -e --sample_count_threshold        :  Sample count threshold\n"
      "   -f --index_usage_type              :  Types of indexes used\n"
      "   -g --tuples_per_tg                 :  # of tuples per tilegroup\n"
      "   -h --help                          :  Print help message\n"
      "   -k --scale-factor                  :  # of tile groups\n"
      "   -l --layout                        :  Layout\n"
      "   -m --max_tile_groups_indexed       :  Max tile groups indexed\n"
      "   -o --convergence                   :  Convergence\n"
      "   -p --projectivity                  :  Projectivity\n"
      "   -q --total_ops                     :  # of operations\n"
      "   -s --selectivity                   :  Selectivity\n"
      "   -t --phase_length                  :  Length of a phase\n"
      "   -u --write_complexity_type         :  Complexity of write\n"
      "   -v --verbose                       :  Output verbosity\n"
      "   -w --write_ratio                   :  Fraction of writes\n"
      "   -x --index_utility_threshold       :  Index utility threshold\n"
      "   -y --index_count_threshold         :  Index count threshold\n"
      "   -z --write_ratio_threshold         :  Write ratio threshold\n"
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    {"attribute_count", optional_argument, NULL, 'a'},
    {"convergence_query_threshold", optional_argument, NULL, 'b'},
    {"query_complexity_type", optional_argument, NULL, 'c'},
    {"variability_threshold", optional_argument, NULL, 'd'},
    {"sample_count_threshold", optional_argument, NULL, 'e'},
    {"index_usage_type", optional_argument, NULL, 'f'},
    {"tuples_per_tg", optional_argument, NULL, 'g'},
    {"scale-factor", optional_argument, NULL, 'k'},
    {"layout", optional_argument, NULL, 'l'},
    {"max_tile_groups_indexed", optional_argument, NULL, 'm'},
    {"convergence", optional_argument, NULL, 'o'},
    {"projectivity", optional_argument, NULL, 'p'},
    {"total_ops", optional_argument, NULL, 'q'},
    {"selectivity", optional_argument, NULL, 's'},
    {"phase_length", optional_argument, NULL, 't'},
    {"write_complexity_type", optional_argument, NULL, 'u'},
    {"verbose", optional_argument, NULL, 'v'},
    {"write_ratio", optional_argument, NULL, 'w'},
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
  if (state.index_usage_type < 1 || state.index_usage_type > 5) {
    LOG_ERROR("Invalid index_usage_type :: %d", state.index_usage_type);
    exit(EXIT_FAILURE);
  } else {
    switch (state.index_usage_type) {
      case INDEX_USAGE_TYPE_CONSERVATIVE:
        LOG_INFO("%s : CONSERVATIVE", "index_usage_type ");
        break;
      case INDEX_USAGE_TYPE_BALANCED:
        LOG_INFO("%s : BALANCED", "index_usage_type ");
        break;
      case INDEX_USAGE_TYPE_AGGRESSIVE:
        LOG_INFO("%s : AGGRESSIVE", "index_usage_type ");
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
    LOG_ERROR("Invalid query_complexity_type :: %d",
              state.query_complexity_type);
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

static void ValidateWriteComplexityType(const configuration &state) {
  if (state.write_complexity_type < 1 || state.write_complexity_type > 2) {
    LOG_ERROR("Invalid write_complexity_type :: %d",
              state.write_complexity_type);
    exit(EXIT_FAILURE);
  } else {
    switch (state.write_complexity_type) {
      case WRITE_COMPLEXITY_TYPE_SIMPLE:
        LOG_INFO("write_complexity_type : SIMPLE");
        break;
      case WRITE_COMPLEXITY_TYPE_COMPLEX:
        LOG_INFO("write_complexity_type : COMPLEX");
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

  if (state.write_ratio == 0) {
    LOG_INFO("%s : READ_ONLY", "write_ratio");
  }
  else if (state.write_ratio == 0.1) {
    LOG_INFO("%s : READ_HEAVY", "write_ratio");
  }
  else if (state.write_ratio == 0.5) {
    LOG_INFO("%s : BALANCED", "write_ratio");
  }
  else if (state.write_ratio == 0.9) {
    LOG_INFO("%s : WRITE_HEAVY", "write_ratio");
  }
  else {
    LOG_INFO("%s : %.1lf", "write_ratio", state.write_ratio);
  }

}

static void ValidateTotalOps(const configuration &state) {
  if (state.total_ops <= 0) {
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

static void ValidateSampleCountThreshold(const configuration &state) {
  if (state.sample_count_threshold <= 0) {
    LOG_ERROR("Invalid sample_count_threshold :: %u",
              state.sample_count_threshold);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %u", "sample_count_threshold", state.sample_count_threshold);
}

static void ValidateMaxTileGroupsIndexed(const configuration &state) {
  if (state.max_tile_groups_indexed <= 0) {
    LOG_ERROR("Invalid max_tile_groups_indexed :: %u",
              state.max_tile_groups_indexed);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %u", "max_tile_groups_indexed", state.max_tile_groups_indexed);
}

static void ValidateConvergence(const configuration &state) {
  if (state.convergence == true) {
    LOG_INFO("%s : %s", "convergence", "true");
  }
}

static void ValidateQueryConvergenceThreshold(const configuration &state) {
  if (state.convergence_query_threshold <= 0) {
    LOG_ERROR("Invalid convergence_query_threshold :: %u",
              state.convergence_query_threshold);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %u", "convergence_query_threshold",
           state.convergence_query_threshold);
}

static void ValidateVariabilityThreshold(const configuration &state) {
  if (state.variability_threshold <= 0 || state.variability_threshold > 25) {
    LOG_ERROR("Invalid variability_threshold :: %u",
              state.variability_threshold);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %u", "variability_threshold", state.variability_threshold);
}

static void ValidateIndexUtilityThreshold(const configuration &state) {
  if (state.index_utility_threshold < 0 || state.index_utility_threshold > 1) {
    LOG_ERROR("Invalid index_utility_threshold :: %.1lf",
              state.index_utility_threshold);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %.1lf", "index_utility_threshold", state.index_utility_threshold);
}

static void ValidateIndexCountThreshold(const configuration &state) {
  if (state.index_count_threshold == 0) {
    LOG_ERROR("Invalid index_count_threshold :: %u",
              state.index_count_threshold);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %u", "index_count_threshold", state.index_count_threshold);
}

static void ValidateWriteRatioThreshold(const configuration &state) {
  if (state.write_ratio_threshold < 0 || state.write_ratio_threshold > 1) {
    LOG_ERROR("Invalid write_ratio_threshold :: %.1lf",
              state.write_ratio_threshold);
    exit(EXIT_FAILURE);
  }

  LOG_INFO("%s : %.1lf", "write_ratio_threshold", state.write_ratio_threshold);
}

void ParseArguments(int argc, char *argv[], configuration &state) {

  state.verbose = false;

  // Default Values
  state.index_usage_type = INDEX_USAGE_TYPE_AGGRESSIVE;
  state.query_complexity_type = QUERY_COMPLEXITY_TYPE_SIMPLE;
  state.write_complexity_type = WRITE_COMPLEXITY_TYPE_SIMPLE;

  // Scale and attribute count
  state.scale_factor = 100.0;
  state.attribute_count = 20;

  state.write_ratio = 0.0;
  state.tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP;

  // Phase parameters
  state.total_ops = 1;
  state.phase_length = 1;

  // Query parameters
  state.selectivity = 0.001;
  state.projectivity = 1.0;

  // Layout parameter
  state.layout_mode = LAYOUT_TYPE_ROW;

  // Adapt parameters
  state.adapt_layout = false;
  state.adapt_indexes = true;

  // Learning rate
  state.sample_count_threshold = 10;
  state.max_tile_groups_indexed = 10;

  // Convergence parameters
  state.convergence = false;
  state.convergence_query_threshold = 200;

  // Variability parameters
  state.variability_threshold = 25;

  // Drop parameters
  state.index_utility_threshold = 0.25;
  state.index_count_threshold = 10;
  state.write_ratio_threshold = 0.75;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "a:b:c:d:e:f:g:hk:l:m:o:p:q:s:t:u:v:w:x:y:z:",
                        opts, &idx);

    if (c == -1) break;

    switch (c) {
      // AVAILABLE FLAGS: ijnrABCDEFGHIJKLMNOPQRSTUVWXYZ
      case 'a':
        state.attribute_count = atoi(optarg);
        break;
      case 'b':
        state.convergence_query_threshold = atoi(optarg);
        break;
      case 'c':
        state.query_complexity_type = (QueryComplexityType)atoi(optarg);
        break;
      case 'd':
        state.variability_threshold = atoi(optarg);
        break;
      case 'e':
        state.sample_count_threshold = atoi(optarg);
        break;
      case 'f':
        state.index_usage_type = (IndexUsageType)atoi(optarg);
        break;
      case 'g':
        state.tuples_per_tilegroup = atoi(optarg);
        break;
      case 'h':
        Usage();
        break;

      case 'k':
        state.scale_factor = atoi(optarg);
        break;
      case 'l':
        state.layout_mode = (LayoutType)atoi(optarg);
        break;
      case 'm':
        state.max_tile_groups_indexed = atoi(optarg);
        break;

      case 'o':
        state.convergence = atoi(optarg);
        break;
      case 'p':
        state.projectivity = atof(optarg);
        break;
      case 'q':
        state.total_ops = atol(optarg);
        break;

      case 's':
        state.selectivity = atof(optarg);
        break;
      case 't':
        state.phase_length = atol(optarg);
        break;
      case 'u':
        state.write_complexity_type = (WriteComplexityType)atoi(optarg);
        break;
      case 'v':
        state.verbose = atoi(optarg);
        break;
      case 'w':
        state.write_ratio = atof(optarg);
        break;
      case 'x':
        state.index_utility_threshold = atof(optarg);
        break;
      case 'y':
        state.index_count_threshold = atoi(optarg);
        break;
      case 'z':
        state.write_ratio_threshold = atof(optarg);
        break;

      default:
        LOG_ERROR("Unknown option: -%c-", c);
        Usage();
    }
  }

  ValidateIndexUsageType(state);
  ValidateWriteRatio(state);
  ValidateQueryComplexityType(state);
  ValidateWriteComplexityType(state);
  ValidateScaleFactor(state);
  ValidateAttributeCount(state);
  ValidateTuplesPerTileGroup(state);
  ValidateTotalOps(state);
  ValidatePhaseLength(state);
  ValidateSelectivity(state);
  ValidateProjectivity(state);
  ValidateLayout(state);
  ValidateIndexUtilityThreshold(state);
  ValidateIndexCountThreshold(state);
  ValidateWriteRatioThreshold(state);

  // Setup learning rate based on index usage type.
  // With a smaller sample_count_threashold, the index tuner will
  // be more aggressive to adapt a new index.
  if (state.index_usage_type == INDEX_USAGE_TYPE_CONSERVATIVE) {
    state.sample_count_threshold = 50;
  } else if (state.index_usage_type == INDEX_USAGE_TYPE_BALANCED) {
    state.sample_count_threshold = 10;
  } else if (state.index_usage_type == INDEX_USAGE_TYPE_AGGRESSIVE) {
    state.sample_count_threshold = 5;
  }

  // Setup max tile groups indexed based on scale factor
  state.max_tile_groups_indexed = state.scale_factor / 10;

  ValidateSampleCountThreshold(state);
  ValidateMaxTileGroupsIndexed(state);
  ValidateConvergence(state);
  ValidateQueryConvergenceThreshold(state);
  ValidateVariabilityThreshold(state);
}

}  // namespace sdbench
}  // namespace benchmark
}  // namespace peloton
