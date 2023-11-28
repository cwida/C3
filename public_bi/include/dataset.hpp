#pragma once

#include "string"
#include <cstdint>
#include <vector>

namespace c3_bench {

inline std::string public_bi_path = "../../public_bi/";
inline std::string public_bi_small_path = public_bi_path + "PublicBIbenchmark_small/";
inline std::string result_path = public_bi_path + "result/";

struct Dataset {
	int         dataset_id;
	std::string dataset_name;
	std::string btr_dir_out;
	std::string binary_dir_out;
	std::string stats_file_out;
	std::string schema_yaml_in;
	std::string table_csv_in;
	// std::string compressionout; // use stats out to store compression times
	std::string typefilter;
	bool        create_binary;
	bool        create_btr;
	bool        verify_correctness;
	int         chunk;
	int         threads;
	int         btrblocks_doubles_max_cascading_level;
	int         btrblocks_integers_max_cascading_level;
	int         btrblocks_strings_max_cascading_level;
	int         c3_btrblocks_doubles_max_cascading_level;
	int         c3_btrblocks_integers_max_cascading_level;
	int         c3_btrblocks_strings_max_cascading_level;
};

struct paths {
	std::string btr_dir_out    = public_bi_small_path; // "../public_bi/";
	std::string binary_dir_out = public_bi_small_path; // "../public_bi/";
	std::string stats_file_out = public_bi_small_path; // "../public_bi/";
	std::string schema_yaml_in = public_bi_small_path; // "../public_bi/";
	std::string table_csv_in   = public_bi_small_path; // "../public_bi/";

	explicit paths() {
		if (auto v = std::getenv("BTR_OUTPUT_DIRECTORY")) { btr_dir_out = v; }
		if (auto v = std::getenv("BINARY_OUTPUT_DIRECTORY")) { binary_dir_out = v; }
		if (auto v = std::getenv("STATS_OUTPUT_DIRECTORY")) { stats_file_out = v; }
		if (auto v = std::getenv("SCHEMA_OUTPUT_DIRECTORY")) { schema_yaml_in = v; }
		if (auto v = std::getenv("TABLE_OUTPUT_DIRECTORY")) { table_csv_in = v; }
	}
};

inline paths PATHS;

} // namespace c3_bench