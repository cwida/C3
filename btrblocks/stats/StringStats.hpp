#pragma once
// -------------------------------------------------------------------------------------
#include "common/Units.hpp"
// -------------------------------------------------------------------------------------
#include "storage/StringArrayViewer.hpp"
// -------------------------------------------------------------------------------------
#include <set>
#include <map>
// -------------------------------------------------------------------------------------
namespace btrblocks {
// -------------------------------------------------------------------------------------
struct StringStats {
  std::set<str> distinct_values;
  // -------------------------------------------------------------------------------------
  u32 total_size;           // everything in the column including slots
  u32 total_length;         // only string starting from slots end
  u32 total_unique_length;  // only the unique (dict) strings
  u32 tuple_count;
  // -------------------------------------------------------------------------------------
  u32 null_count;
  u32 unique_count;
  u32 set_count;

  // C3
  std::map<str, u32> distinct_values_counter;
  // int first_non_null_idx = -1;
  // u32 max_distinct_value_occurence = 1;

  // -------------------------------------------------------------------------------------
  static StringStats generateStats(const StringArrayViewer src,
                                   const BITMAP* nullmap,
                                   u32 tuple_count,
                                   SIZE column_data_size);

  static StringStats generateStatsBasic(u32 tuple_count,
                                   SIZE column_data_size);
};
// -------------------------------------------------------------------------------------
}  // namespace btrblocks
// -------------------------------------------------------------------------------------
