#pragma once
#include <filesystem>
#include <fstream>

#include "storage/Relation.hpp"
#include "c3/C3.hpp"
#include "c3/CompressionSchemes.hpp"
#include "storage/InputChunk.hpp"
#include "datasets.hpp"

namespace c3{

struct DecompressedColumn {
	bool requires_copy;
	std::vector<uint8_t> nullmap;
	std::vector<uint8_t> data;
	size_t tuple_count;

	DecompressedColumn() = default;

	DecompressedColumn(bool requires_copy, std::vector<uint8_t> nullmap, std::vector<uint8_t> data, size_t tuple_count)
	:requires_copy(requires_copy), nullmap(nullmap), data(data), tuple_count(tuple_count)
	{}
};

class C3Compressor{

public:
	C3Compressor(){
		c3_log_stream = std::ofstream(c3_bench::result_path + "c3_log.txt");
		relations_stats_stream = std::ofstream(c3_bench::result_path + "relation_stats.csv");
		row_group_stats_stream = std::ofstream(c3_bench::result_path + "row_group_stats.csv");
		columns_stats_stream = std::ofstream(c3_bench::result_path + "column_stats.csv");
		schemes_stats_stream = std::ofstream(c3_bench::result_path + "scheme_stats.csv");
		// azim_numeric_string_columns_stream = std::ofstream(c3_bench::result_path + "int_string_cols.csv");
	};

	c3_bench::Dataset get_dataset(int dataset_id);

	std::vector<std::vector<size_t>> compress_btrblocks(btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, c3_bench::Dataset dataset, std::vector<std::vector<size_t>>& original_sizes);

	void verify_or_die(bool verify, const std::string& filename, const std::vector<btrblocks::InputChunk>& input_chunks);
	
	std::vector<std::vector<size_t>> compress_table_c3(btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, c3_bench::Dataset dataset, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_info);
	
	std::vector<std::vector<uint8_t>> compress_rowGroup_c3(btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, int row_group_i, c3_bench::Dataset dataset, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_info);

	std::pair<btrblocks::Relation, std::vector<btrblocks::Range>> get_btrblocks_relation(c3_bench::Dataset dataset);
    
	int compress_and_verify_c3(std::ofstream& log_stream, btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, c3_bench::Dataset dataset);
	int compress_and_verify_c3_old(std::ofstream& log_stream, btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, c3_bench::Dataset dataset);

	std::vector<std::vector<std::tuple<std::shared_ptr<CompressionScheme>, size_t, size_t>>> test_c3_ecr(btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info);

	std::vector<DecompressedColumn> decompress_row_group(std::vector<std::vector<uint8_t>> compressed_cols);


	// logging
	void log_config();
	double log_relation_stats(btrblocks::Relation& relation, c3_bench::Dataset dataset, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, int& num_ecr_computed, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats);
	void log_row_group_stats(btrblocks::Relation& relation, c3_bench::Dataset dataset, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats);
	void log_dataset_stats(btrblocks::Relation& relation, std::ofstream& dataset_stats_stream, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats);
	void log_column_stats(btrblocks::Relation& relation, c3_bench::Dataset dataset, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats);
	void log_ecr_test_scheme_stats(btrblocks::Relation& relation, c3_bench::Dataset& dataset, std::vector<std::vector<std::tuple<std::shared_ptr<c3::CompressionScheme>, size_t, size_t>>>& scheme_infos, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats);
	void log_scheme_stats(btrblocks::Relation& relation, c3_bench::Dataset& dataset, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats);

	std::ofstream c3_log_stream;
	std::ofstream relations_stats_stream;
	std::ofstream row_group_stats_stream;
	std::ofstream columns_stats_stream;
	std::ofstream schemes_stats_stream;
	// std::ofstream azim_numeric_string_columns_stream;

	double total_get_rg_time = 0;
	double total_get_bb_time = 0;
	double total_get_c3_time = 0;
	double total_compress_time = 0;

	private:
		std::vector<std::shared_ptr<c3::CompressionScheme>> finalized_schemes;
		std::shared_ptr<CorrelationGraph> original_c3_graph = nullptr;


};

} // namespace c3