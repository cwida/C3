#include "StringStats.hpp"
// -------------------------------------------------------------------------------------
namespace btrblocks {

StringStats StringStats::generateStatsBasic(
                                       u32 tuple_count,
                                       SIZE column_data_size) {
  // Collect stats
  StringStats stats;
  stats.tuple_count = tuple_count;
  stats.total_size = column_data_size;
  return stats;
}

// -------------------------------------------------------------------------------------
StringStats StringStats::generateStats(const btrblocks::StringArrayViewer src,
                                       const BITMAP* nullmap,
                                       u32 tuple_count,
                                       SIZE column_data_size) {
  // -------------------------------------------------------------------------------------
  // Collect stats
  StringStats stats;
  // -------------------------------------------------------------------------------------
  stats.tuple_count = tuple_count;
  stats.total_size = column_data_size;
  stats.total_length = 0;
  stats.total_unique_length = 0;
  stats.null_count = 0;
  stats.unique_count = 0;
  // -------------------------------------------------------------------------------------
  for (u64 row_i = 0; row_i < tuple_count; row_i++) {
    if (nullmap == nullptr || nullmap[row_i]) {
      // if(stats.first_non_null_idx == -1){
      //   stats.first_non_null_idx = row_i;
      // }
      auto current_value = src(row_i);
      if (stats.distinct_values.find(current_value) == stats.distinct_values.end()) {
        stats.distinct_values.insert(current_value);
        stats.total_unique_length += current_value.length();
        stats.distinct_values_counter[current_value] = stats.unique_count;
        stats.unique_count++;
      }
      // else{
      //   stats.distinct_values_counter[current_value]++;
      // //   stats.max_distinct_value_occurence = std::max(stats.max_distinct_value_occurence, stats.distinct_values_counter[current_value]);
      // }
      stats.total_length += current_value.size();
    } else {
      stats.distinct_values.insert("");
      stats.null_count++;
      continue;
    }
  }
  // -------------------------------------------------------------------------------------
  // stats.unique_count = stats.distinct_values.size();
  stats.set_count = stats.tuple_count - stats.null_count;
  return stats;
}
// -------------------------------------------------------------------------------------
}  // namespace btrblocks