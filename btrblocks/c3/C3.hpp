#pragma once

#include "CompressionSchemes.hpp"
#include "CorrelationGraph.hpp"
#include "c3/storage/RowGroup.hpp"

namespace c3 {

struct C3LoggingInfo {
	std::shared_ptr<CorrelationGraph> graph;
	std::vector<ColumnStatus> column_status;
	int compute_ecr_counter;
	int add_to_graph_counter;
	int final_scheme_counter;
	double average_schemes_per_source;
	// original column types
	std::vector<btrblocks::ColumnType> original_column_types;
};

class C3 {
public:
	explicit C3(std::shared_ptr<RowGroup> row_group);

	// for each column, get stats and ECR for all BB schemes
	// void get_btrblocks_schemes();

	// for each column, get stats and compute exact compression ratios by compressing entire column chunk (no sampling)
	void get_btrblocks_schemes_exact_ECR();

	// for each column, get stats and ECR for all BB schemes
	std::vector<std::shared_ptr<ColumnStats>> btrBlocksSchemes;

	// fills compressionSchemes with C3 schemes to be used
	void get_compression_schemes(std::ofstream& log_stream, bool finalize=true, bool ignore_bb_ecr=false, std::vector<std::shared_ptr<c3::CompressionScheme>> force_schemes = {}, std::shared_ptr<CorrelationGraph> c3_graph = nullptr);

    // go through pairs of columns, compute ECR, add correlation to graph if better than BB
    void find_correlations(bool ignore_bb_ecr);

	// compresses all columns with C3 or BB schemes
	std::vector<std::vector<uint8_t>> compress(std::ofstream& log_stream);
	
	// compress columns using scheme
	std::vector<std::vector<btrblocks::u8>> apply_scheme(std::shared_ptr<CompressionScheme> scheme);
	
	// vector of all C3 compression schemes used
	std::vector<std::shared_ptr<CompressionScheme>> compressionSchemes;

	// maps best scheme found by C3 to column index
	std::vector<ColumnStatus> column_status; // each column matching to compression scheme used for column

	// RowGroup of uncompressed columns
	std::shared_ptr<RowGroup> row_group;

	// graph of correlations between columns
	std::shared_ptr<CorrelationGraph> graph;

	// // brute force all C3 schemes to find the best
	// void get_compression_schemes_optimum();

	std::shared_ptr<C3LoggingInfo> get_logging_info();

	std::vector<int> get_numeric_string_columns();

private:
	//logging info
	std::shared_ptr<C3LoggingInfo> logging_info;

	int compute_ecr_counter = 0;
	int add_to_graph_counter = 0;
	int final_scheme_counter = 0;
	double average_schemes_per_source = 0;

	double equality_compress_time = 0; 
	double dict_compress_time = 0; 
	double dict_1toN_compress_time = 0; 
	double numerical_compress_time = 0; 
	double dfor_compress_time = 0; 

	int equality_counter = 0; 
	int dict_counter = 0; 
	int dict_1toN_counter = 0; 
	int numerical_counter = 0; 
	int dfor_counter = 0; 
	

	void get_ignore_columns();

	void force_compression_schemes(std::vector<std::shared_ptr<c3::CompressionScheme>> schemes);

	void find_equality_correlation(int i, int j, bool ignore_bb_ecr=false);
	void find_dict1to1_correlation(int i, int j, bool ignore_bb_ecr=false);
	void find_dict1toN_correlation(int i, int j, bool ignore_bb_ecr=false);
	void find_numerical_correlation(int i, int j, bool ignore_bb_ecr=false);
	void find_dfor_correlation(int i, int j, bool ignore_bb_ecr=false);
	void find_dictShare_correlation(int i, int j, bool ignore_bb_ecr=false);     

	void graph_given_find_correlations(std::shared_ptr<CorrelationGraph> given_graph);

};

} // namespace c3
