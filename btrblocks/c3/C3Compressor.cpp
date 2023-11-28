#include "C3Compressor.hpp"
#include "c3/Utils.hpp"
#include "btrfiles.hpp"
#include "compression/BtrReader.hpp"
#include "compression/Datablock.hpp"
#include "common/Utils.hpp"
#include "scheme/SchemePool.hpp"
#include "schemes/multi_col/dfor.hpp"
#include "schemes/multi_col/dict_1to1.hpp"
#include "schemes/multi_col/dict_1toN.hpp"
#include "schemes/multi_col/equality.hpp"
#include "schemes/multi_col/numerical.hpp"
#include "schemes/single_col/dictionary.hpp"

namespace c3{

void C3Compressor::log_dataset_stats(btrblocks::Relation& relation, std::ofstream& dataset_stats_stream, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats){

	// for each column sum C3 sizes over all row groups

	auto num_columns = relation.columns.size();
	size_t total_uncompressed = 0;
	size_t total_c3_compressed = 0;
	size_t total_btrblocks_compressed = 0;

	std::vector<size_t> c3_compressed_sizes(num_columns, 0);
	std::vector<size_t> bb_compressed_sizes(num_columns, 0);
	std::vector<size_t> uncompressed_sizes(num_columns, 0);
	for(size_t row_group=0; row_group<c3_compressed.size(); row_group++){
		for(size_t col=0; col<num_columns; col++){
			c3_compressed_sizes[col] += c3_compressed[row_group][col];
			bb_compressed_sizes[col] += bb_stats[row_group][col]->get_BB_compressed_size();
			uncompressed_sizes[col] += bb_stats[row_group][col]->uncompressed_size;
		}
	}

    dataset_stats_stream << "Column|uncompressed|bb_compressed|c3_compressed|compression_ratio_improvement" << std::endl;

	for(size_t col=0; col<num_columns; col++){
		dataset_stats_stream << relation.columns[col].name << "|" 
				<< uncompressed_sizes[col] << "|" 
				<< c3_compressed_sizes[col] << "|" 
				<< c3_compressed_sizes[col] << "|"
				<< 1.0 * c3_compressed_sizes[col] / c3_compressed_sizes[col] << std::endl;

		total_uncompressed += uncompressed_sizes[col];
		total_c3_compressed += c3_compressed_sizes[col];
		total_btrblocks_compressed += bb_compressed_sizes[col];
	}

	dataset_stats_stream << "total: " <<  total_uncompressed << "| " << total_btrblocks_compressed << "|" << total_c3_compressed << "|" << 1.0 * total_btrblocks_compressed / total_c3_compressed << std::endl;
}

void C3Compressor::log_scheme_stats(btrblocks::Relation& relation, c3_bench::Dataset& dataset, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats){
	
	for(size_t row_group=0; row_group<c3_compressed.size(); row_group++){
		for(const auto& columnNode: c3_rg_log_info[row_group]->graph->columnNodes){
			for(auto final_edge: columnNode->final_outgoingEdges){
				auto scheme = final_edge->scheme;
				auto c3_source_size = c3_compressed[row_group][scheme->columns[0]];
				auto c3_target_size = c3_compressed[row_group][scheme->columns[1]];

				schemes_stats_stream << dataset.dataset_name << "|"
				<< row_group << "|"
				<< Utils::scheme_to_string(scheme->type) << "|"
				<< scheme->columns[0] << "|"
				<< scheme->columns[1] << "|"
				<< relation.columns[scheme->columns[0]].name + "|"
				<< relation.columns[scheme->columns[1]].name + "|"
				<< Utils::bb_ColumnType_to_string(relation.columns[scheme->columns[0]].type) + "|"
				<< Utils::bb_ColumnType_to_string(relation.columns[scheme->columns[1]].type) + "|"
				<< bb_stats[row_group][scheme->columns[0]]->uncompressed_size << "|"
				<< bb_stats[row_group][scheme->columns[1]]->uncompressed_size << "|"
				<< c3_source_size << "|"
				<< c3_target_size << "|"
				<< 1.0 * bb_stats[row_group][scheme->columns[0]]->uncompressed_size / c3_source_size << "|" // c3 source cr
				<< 1.0 * bb_stats[row_group][scheme->columns[1]]->uncompressed_size / c3_target_size << "|" // c3 target cr
				<< bb_stats[row_group][scheme->columns[0]]->get_BB_compressed_size() << "|"
				<< bb_stats[row_group][scheme->columns[1]]->get_BB_compressed_size() << "|"
				<< 1.0 * bb_stats[row_group][scheme->columns[0]]->uncompressed_size / bb_stats[row_group][scheme->columns[0]]->get_BB_compressed_size() << "|" // bb source cr
				<< 1.0 * bb_stats[row_group][scheme->columns[1]]->uncompressed_size / bb_stats[row_group][scheme->columns[1]]->get_BB_compressed_size() << "|" // bb target cr
				<< 1.0 * (bb_stats[row_group][scheme->columns[0]]->get_BB_compressed_size() + bb_stats[row_group][scheme->columns[1]]->get_BB_compressed_size()) / (c3_source_size + c3_target_size) << "|" // scheme total cr_improvement
				<< scheme->print_log(c3_rg_log_info[row_group]->original_column_types)
				<< std::endl;
				;
			}
		}
	}
}

void C3Compressor::log_ecr_test_scheme_stats(btrblocks::Relation& relation, c3_bench::Dataset& dataset, std::vector<std::vector<std::tuple<std::shared_ptr<c3::CompressionScheme>, size_t, size_t>>>& scheme_infos, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats){
	for(size_t row_group=0; row_group<scheme_infos.size(); row_group++){
		for(auto& scheme_info: scheme_infos[row_group]){
			auto scheme = std::get<0>(scheme_info);
			auto c3_source_size = std::get<1>(scheme_info); 
			auto c3_target_size = std::get<2>(scheme_info);
			schemes_stats_stream << dataset.dataset_name << "|"
				<< row_group << "|"
				<< Utils::scheme_to_string(scheme->type) << "|"
				<< scheme->columns[0] << "|"
				<< scheme->columns[1] << "|"
				<< relation.columns[scheme->columns[0]].name + "|"
				<< relation.columns[scheme->columns[1]].name + "|"
				<< Utils::bb_ColumnType_to_string(relation.columns[scheme->columns[0]].type) + "|"
				<< Utils::bb_ColumnType_to_string(relation.columns[scheme->columns[1]].type) + "|"
				<< bb_stats[row_group][scheme->columns[0]]->uncompressed_size << "|"
				<< bb_stats[row_group][scheme->columns[1]]->uncompressed_size << "|"
				<< c3_source_size << "|" // c3 compressed source size
				<< c3_target_size << "|" // c3 compressed target size
				<< 1.0 * bb_stats[row_group][scheme->columns[0]]->uncompressed_size / c3_source_size << "|" // source cr
				<< 1.0 * bb_stats[row_group][scheme->columns[1]]->uncompressed_size / c3_target_size << "|" // target cr
				<< bb_stats[row_group][scheme->columns[0]]->get_BB_compressed_size() << "|"
				<< bb_stats[row_group][scheme->columns[1]]->get_BB_compressed_size() << "|"
				<< 1.0 * bb_stats[row_group][scheme->columns[0]]->uncompressed_size / bb_stats[row_group][scheme->columns[0]]->get_BB_compressed_size() << "|" // bb source cr
				<< 1.0 * bb_stats[row_group][scheme->columns[1]]->uncompressed_size / bb_stats[row_group][scheme->columns[1]]->get_BB_compressed_size() << "|" // bb target cr
				<< 1.0 * (bb_stats[row_group][scheme->columns[0]]->get_BB_compressed_size() + bb_stats[row_group][scheme->columns[1]]->get_BB_compressed_size()) / (c3_source_size + c3_target_size) << "|" // scheme total cr_improvement
				<< scheme->print_log(c3_rg_log_info[row_group]->original_column_types)
				<< std::endl;
		}
	}
}

std::string extract_column_id(std::string col_name){
	size_t end = 0;
	for(int i=0; i<col_name.size(); i++){
		if(col_name[i]=='_'){
			break;
		}
		end++;
	}
	return std::to_string(std::stoi(col_name.substr(0, end))-1);
}

void C3Compressor::log_column_stats(btrblocks::Relation& relation, c3_bench::Dataset dataset, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats){
	// all stats aggregate per row group
	for(size_t row_group_i=0; row_group_i<bb_stats.size(); row_group_i++){
		for(size_t col=0; col<bb_stats[row_group_i].size(); col++){
			columns_stats_stream << dataset.dataset_name << "|"
								<< row_group_i << "|"
								<< relation.columns[col].name << "|" 
								<< bb_stats[row_group_i][col]->uncompressed_size << "|" 
								<< bb_stats[row_group_i][col]->get_BB_compressed_size() << "|" 
								<< c3_compressed[row_group_i][col] << "|"
								<< 1.0 * bb_stats[row_group_i][col]->get_BB_compressed_size() / c3_compressed[row_group_i][col] << "|"
								<< 1.0 * bb_stats[row_group_i][col]->uncompressed_size / bb_stats[row_group_i][col]->get_BB_compressed_size() << "|"
								<< 1.0 * bb_stats[row_group_i][col]->uncompressed_size / c3_compressed[row_group_i][col] << "|"
								<< Utils::bb_ColumnType_to_string(relation.columns[col].type) << "|";		

			if(c3_rg_log_info[row_group_i]->column_status[col] == ColumnStatus::None){
				columns_stats_stream << "None|"
					<< CompressionScheme::print_empty_log() << std::endl;
			}
			else{
				std::shared_ptr<c3::CompressionScheme> scheme;
				if(c3_rg_log_info[row_group_i]->graph->columnNodes[col]->final_incomingEdge == nullptr){
					columns_stats_stream << "source|";
					scheme = c3_rg_log_info[row_group_i]->graph->columnNodes[col]->final_outgoingEdges[0]->scheme;
				}
				else{
					columns_stats_stream << "target|";
					scheme = c3_rg_log_info[row_group_i]->graph->columnNodes[col]->final_incomingEdge->scheme;
				}
				columns_stats_stream << scheme->print_log(c3_rg_log_info[row_group_i]->original_column_types); // << std::endl;
			
				columns_stats_stream << "|" << extract_column_id(relation.columns[scheme->columns[0]].name);//
				columns_stats_stream << "|" << extract_column_id(relation.columns[scheme->columns[1]].name) << std::endl;
			}
		}
	}
}

double C3Compressor::log_relation_stats(btrblocks::Relation& relation, c3_bench::Dataset dataset, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, int& num_ecr_computed, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats){
	// all stats aggregate over all row groups

	assert(c3_compressed.size() > 0);

	size_t total_uncompressed = 0;
	size_t total_c3_compressed = 0;
	size_t total_bb_compressed = 0;
	size_t total_compute_ecr_counter = 0;
	size_t total_add_to_graph_counter = 0;
	size_t total_final_scheme_counter = 0;
	size_t total_columns = relation.columns.size() * c3_compressed.size();
	double average_schemes_per_source = 0;

	for(size_t row_group=0; row_group<c3_compressed.size(); row_group++){
		for(size_t col=0; col<relation.columns.size(); col++){
			total_uncompressed += bb_stats[row_group][col]->uncompressed_size;
			total_c3_compressed += c3_compressed[row_group][col];
			total_bb_compressed += bb_stats[row_group][col]->get_BB_compressed_size();
		}
		total_compute_ecr_counter += c3_rg_log_info[row_group]->compute_ecr_counter;
		total_add_to_graph_counter += c3_rg_log_info[row_group]->add_to_graph_counter;
		total_final_scheme_counter += c3_rg_log_info[row_group]->final_scheme_counter;
		average_schemes_per_source += c3_rg_log_info[row_group]->average_schemes_per_source;
	}
	average_schemes_per_source /= c3_compressed.size();

	num_ecr_computed = total_compute_ecr_counter;

	double total_compression_ratio_improvement = 1.0 * total_bb_compressed / total_c3_compressed;
	
	relations_stats_stream << dataset.dataset_name << "|" << total_uncompressed 
		<< "|" << total_bb_compressed
		<< "|" << total_c3_compressed 
		<< "|" << total_compression_ratio_improvement
		<< "|" << total_columns
		<< "|" << total_compute_ecr_counter 
		<< "|" << total_add_to_graph_counter 
		<< "|" << total_final_scheme_counter 
		<< "|" << average_schemes_per_source 
		<< std::endl;

	return total_compression_ratio_improvement;
} 

void C3Compressor::log_row_group_stats(btrblocks::Relation& relation, c3_bench::Dataset dataset, std::vector<std::vector<size_t>>& c3_compressed, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info, std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_stats){
	// all stats aggregate over each row groups
	assert(c3_compressed.size() > 0);

	for(size_t row_group=0; row_group<c3_compressed.size(); row_group++){

		size_t total_uncompressed = 0;
		size_t total_c3_compressed = 0;
		size_t total_bb_compressed = 0;

		for(size_t col=0; col<relation.columns.size(); col++){
			total_uncompressed += bb_stats[row_group][col]->uncompressed_size;
			total_c3_compressed += c3_compressed[row_group][col];
			total_bb_compressed +=bb_stats[row_group][col]->get_BB_compressed_size();
		}

		double compression_ratio_improvement = 1.0 * total_bb_compressed / total_c3_compressed;
	
		row_group_stats_stream << dataset.dataset_name 
			<< "|" << row_group 
			<< "|" << total_uncompressed 
			<< "|" << total_bb_compressed
			<< "|" << total_c3_compressed 
			<< "|" << compression_ratio_improvement
			<< "|" << c3_rg_log_info[row_group]->compute_ecr_counter 
			<< "|" << c3_rg_log_info[row_group]->add_to_graph_counter 
			<< "|" << c3_rg_log_info[row_group]->final_scheme_counter << std::endl;
	}
} 

std::vector<std::vector<size_t>> C3Compressor::compress_btrblocks(btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, c3_bench::Dataset dataset, std::vector<std::vector<size_t>>& original_sizes){
	
	std::vector<std::vector<size_t>> compressed_sizes(ranges.size());

	for (btrblocks::SIZE row_group_i = 0; row_group_i < ranges.size(); row_group_i++) {
		original_sizes[row_group_i].resize(relation.columns.size());
		compressed_sizes[row_group_i].resize(relation.columns.size());
		for (size_t column_i = 0; column_i < relation.columns.size(); column_i++) {		
			auto input_chunk = relation.getInputChunk(ranges[row_group_i], row_group_i, column_i);
			original_sizes[row_group_i][column_i] = input_chunk.size;	

			std::vector<btrblocks::u8> data = btrblocks::Datablock::compress(input_chunk);
			compressed_sizes[row_group_i][column_i] = data.size();
		}
	}
	
	return compressed_sizes;
}

std::vector<std::vector<std::tuple<std::shared_ptr<CompressionScheme>, size_t, size_t>>> C3Compressor::test_c3_ecr(btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info){
	
	// scheme, compressed source size, compressed target size
	std::vector<std::vector<std::tuple<std::shared_ptr<CompressionScheme>, size_t, size_t>>> scheme_infos(ranges.size());

	c3_rg_log_info.resize(ranges.size());
	for (size_t row_group_i=0; row_group_i<ranges.size(); row_group_i++) {
		auto row_group = relation.getRowGroup(ranges[row_group_i]);
		auto original_row_group = row_group->deep_copy();

		auto c3_row_group = std::make_unique<c3::C3>(row_group);
		c3_rg_log_info[row_group_i] = c3_row_group->get_logging_info();
		c3_row_group->get_btrblocks_schemes_exact_ECR();
		c3_row_group->get_compression_schemes(c3_log_stream, false, true);
		// For dictionary and DFOR, need to reset columns to original after applying scheme, since scheme alters source columns

		// 1. equality && numerical
		for(const auto& scheme: c3_row_group->compressionSchemes){
			if(Utils::is_non_dict_scheme(scheme->type)){
				std::vector<std::vector<uint8_t>> compressed_columns = c3_row_group->apply_scheme(scheme);
				assert(compressed_columns.size()==2);
				std::tuple<std::shared_ptr<CompressionScheme>,size_t,size_t> info = {scheme, compressed_columns[0].size(),compressed_columns[1].size()};
				scheme_infos[row_group_i].push_back(info);
			}
		}
		// 2. dictionary && DFOR && Dict_1toN (they modify original columns, need to reset after applying scheme)
		for(const auto& scheme: c3_row_group->compressionSchemes){
			if(Utils::is_dict_scheme(scheme->type)){
				c3_row_group->row_group = std::make_shared<RowGroup>(std::move(original_row_group.deep_copy()));
				std::vector<std::vector<uint8_t>> compressed_columns = c3_row_group->apply_scheme(scheme);
				assert(compressed_columns.size()==2);
				std::tuple<std::shared_ptr<CompressionScheme>,size_t,size_t> info = {scheme, compressed_columns[0].size(),compressed_columns[1].size()};
				scheme_infos[row_group_i].push_back(info);
			}
		}

		// c3_row_groups.push_back(std::move(c3_row_group));

	}

	return std::move(scheme_infos);
}

std::vector<std::vector<size_t>> C3Compressor::compress_table_c3(btrblocks::Relation& relation, 
		std::vector<btrblocks::Range> ranges, 
		c3_bench::Dataset dataset,
		std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info,
		std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_info)
		{

	finalized_schemes.clear();
	original_c3_graph = nullptr;
	c3_rg_log_info.resize(ranges.size());
	bb_info.resize(ranges.size());
	std::vector<std::vector<size_t>> rowgroup_col_sizes(ranges.size(), std::vector<size_t>(relation.columns.size()));
	for(size_t rg_i=0; rg_i<ranges.size(); rg_i++){
		c3_log_stream << "Compressing: " << dataset.dataset_name << "; Row Group " << rg_i << std::endl;
		
		auto compressed_cols = compress_rowGroup_c3(relation, ranges, rg_i, dataset, c3_rg_log_info, bb_info);
		for(int col_i=0; col_i<compressed_cols.size(); col_i++){
			rowgroup_col_sizes[rg_i][col_i] = compressed_cols[col_i].size();
		}
	}
	return rowgroup_col_sizes;
}

// get best BB schemes, get best C3 schemes, compress
std::vector<std::vector<uint8_t>> C3Compressor::compress_rowGroup_c3(btrblocks::Relation& relation, 
		std::vector<btrblocks::Range> ranges,
		int row_group_i, 
		c3_bench::Dataset dataset,
		std::vector<std::shared_ptr<c3::C3LoggingInfo>>& c3_rg_log_info,
		std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>>& bb_info)
		{

	switch(static_cast<RowGroupsShareScheme>(config.SHARE_ROW_GROUP_SCHEME)){
		case RowGroupsShareScheme::None:{
			std::vector<std::vector<uint8_t>> compressed_row_group;

			auto c3_start_time = std::chrono::steady_clock::now();
			auto row_group = relation.getRowGroup(ranges[row_group_i]);
			auto c3_end_time = std::chrono::steady_clock::now();
			total_get_rg_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
			
			auto c3_row_group = std::make_unique<c3::C3>(row_group);

			// get BB schemes
			c3_start_time = std::chrono::steady_clock::now();
			c3_row_group->get_btrblocks_schemes_exact_ECR();
			c3_end_time = std::chrono::steady_clock::now();
			total_get_bb_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
			
			// get C3 schemes
			c3_start_time = std::chrono::steady_clock::now();
			c3_row_group->get_compression_schemes(c3_log_stream);
			c3_end_time = std::chrono::steady_clock::now();
			total_get_c3_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
			
			// compress
			c3_start_time = std::chrono::steady_clock::now();
			std::vector<std::vector<uint8_t>> compressed_columns = c3_row_group->compress(c3_log_stream);
			c3_end_time = std::chrono::steady_clock::now();
			total_compress_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
			
			c3_rg_log_info[row_group_i] = c3_row_group->get_logging_info();
			bb_info[row_group_i] = c3_row_group->btrBlocksSchemes;
			return compressed_columns;
		}
		case RowGroupsShareScheme::ShareFinalized:{
			
			auto c3_start_time = std::chrono::steady_clock::now();
			auto row_group = relation.getRowGroup(ranges[row_group_i]);
			auto c3_end_time = std::chrono::steady_clock::now();
			total_get_rg_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
	
			auto c3_row_group = std::make_unique<c3::C3>(row_group);
			
			// get BB schemes
			c3_start_time = std::chrono::steady_clock::now();
			c3_row_group->get_btrblocks_schemes_exact_ECR();
			c3_end_time = std::chrono::steady_clock::now();
			total_get_bb_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
			
			// // azim request: get string columns which are actually int
			// auto int_cols = c3_row_group->get_numeric_string_columns();
			// for(auto int_col: int_cols){
			// 	if(c3_row_group->btrBlocksSchemes[int_col]->get_null_count() < relation.tuple_count){
			// 		azim_numeric_string_columns_stream << dataset.dataset_name << "|" << relation.columns[int_col].name << "|" << extract_column_id(relation.columns[int_col].name) << std::endl;
			// 	}
			// }

			// get C3 schemes
			c3_start_time = std::chrono::steady_clock::now();				
			c3_row_group->get_compression_schemes(c3_log_stream, true, false, finalized_schemes);
			c3_end_time = std::chrono::steady_clock::now();
			total_get_c3_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
			
			// store finalized schemes of first row group to reuse
			if(row_group_i == 0){
				finalized_schemes = c3_row_group->compressionSchemes;
			}
			
			// compress
			c3_start_time = std::chrono::steady_clock::now();				
			std::vector<std::vector<uint8_t>> compressed_columns = c3_row_group->compress(c3_log_stream);
			c3_end_time = std::chrono::steady_clock::now();
			total_compress_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
				
			c3_rg_log_info[row_group_i] = c3_row_group->get_logging_info();
			bb_info[row_group_i] = c3_row_group->btrBlocksSchemes;
			return compressed_columns;
		}
		case RowGroupsShareScheme::ShareGraph:{
			auto c3_start_time = std::chrono::steady_clock::now();
			auto row_group = relation.getRowGroup(ranges[row_group_i]);
			auto c3_end_time = std::chrono::steady_clock::now();
			total_get_rg_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
						
			auto c3_row_group = std::make_unique<c3::C3>(row_group);

			// get BB schemes
			c3_start_time = std::chrono::steady_clock::now();
			c3_row_group->get_btrblocks_schemes_exact_ECR();
			c3_end_time = std::chrono::steady_clock::now();
			total_get_bb_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
			
			// get C3 schemes
			// store finalized schemes of first row group to reuse
			c3_start_time = std::chrono::steady_clock::now();				
			if(row_group_i == 0){
				c3_row_group->get_compression_schemes(c3_log_stream);
				original_c3_graph = std::make_shared<CorrelationGraph>(c3_row_group->graph->get_deep_copy_edges());
			}
			else{
				c3_row_group->get_compression_schemes(c3_log_stream, true, false, {}, original_c3_graph);
			}
			c3_end_time = std::chrono::steady_clock::now();
			total_get_c3_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();
			
			// compress
			c3_start_time = std::chrono::steady_clock::now();				
			std::vector<std::vector<uint8_t>> compressed_columns = c3_row_group->compress(c3_log_stream);
			c3_end_time = std::chrono::steady_clock::now();
			total_compress_time += std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time).count();

			c3_rg_log_info[row_group_i] = c3_row_group->get_logging_info();
			bb_info[row_group_i] = c3_row_group->btrBlocksSchemes;
			return compressed_columns;
		}
	}
}

c3_bench::Dataset C3Compressor::get_dataset(int dataset_id){
	std::vector<c3_bench::Dataset> datasets_all = c3_bench::datasets_public_bi_small;

	for(size_t i=0; i<datasets_all.size(); i++){
		if(datasets_all[i].dataset_id == dataset_id){
			return datasets_all[i];
		}
	}
	
	throw std::invalid_argument( "dataset id not found." );
}

std::pair<btrblocks::Relation, std::vector<btrblocks::Range>> C3Compressor::get_btrblocks_relation(c3_bench::Dataset dataset){

	btrblocks::SchemePool::refresh();

	// Load schema
	const auto schema = YAML::LoadFile(dataset.schema_yaml_in);

	uint64_t binary_creation_time = 0;
	if (dataset.create_binary || !std::filesystem::is_directory(dataset.binary_dir_out)) {
		c3_log_stream << "Creating binary files in " + dataset.binary_dir_out << std::endl;
		// Load and parse CSV
		std::ifstream csv(dataset.table_csv_in);
		if (!csv.good()) { throw Generic_Exception("Unable to open specified csv file"); }
		// parse writes the binary files
		btrblocks::files::convertCSV(dataset.table_csv_in, schema, dataset.binary_dir_out);
	}

	btrblocks::ColumnType typefilter;
	if (dataset.typefilter.empty()) {
		typefilter = btrblocks::ColumnType::UNDEFINED;
	} else if (dataset.typefilter == "integer") {
		typefilter = btrblocks::ColumnType::INTEGER;
	} else if (dataset.typefilter == "double") {
		typefilter = btrblocks::ColumnType::DOUBLE;
	} else if (dataset.typefilter == "string") {
		typefilter = btrblocks::ColumnType::STRING;
	} else {
		throw std::runtime_error("typefilter must be one of [integer, double, string]");
	}

	if (typefilter != btrblocks::ColumnType::UNDEFINED) {
		c3_log_stream << "Only considering columns with type " + dataset.typefilter << std::endl;
	}

	// Create relation
	btrblocks::Relation   relation  = btrblocks::files::readDirectory(schema, dataset.binary_dir_out);
	std::filesystem::path yaml_path = dataset.schema_yaml_in;
	relation.name                   = yaml_path.stem();

	// Prepare datastructures for btr compression
	// auto ranges = relation.getRanges(static_cast<SplitStrategy>(1), 9999);
	auto ranges = relation.getRanges(btrblocks::SplitStrategy::SEQUENTIAL, 9999);

	if (dataset.chunk != -1) { ranges = {ranges[dataset.chunk]}; }

	assert(ranges.size() > 0);

	std::filesystem::create_directory(dataset.btr_dir_out);

	return {std::move(relation), std::move(ranges)};

}

void C3Compressor::log_config(){
	c3_log_stream << "Config:" << std::endl;
	c3_log_stream << "C3_WINDOW_SIZE: " << std::to_string(config.C3_WINDOW_SIZE) << std::endl;
	c3_log_stream << "BYTES_SAVED_MARGIN: " << std::to_string(config.BYTES_SAVED_MARGIN) << std::endl;
	c3_log_stream << "C3_GRAPH_SHARE_SOURCE_NODES: " << std::to_string(config.C3_GRAPH_SHARE_SOURCE_NODES) << std::endl;
	c3_log_stream << "REVERT_BB_IF_C3_BAD: " << std::to_string(config.REVERT_BB_IF_C3_BAD) << std::endl;
	c3_log_stream << "USE_PRUNING_RULES: " << std::to_string(config.USE_PRUNING_RULES) << std::endl;
	c3_log_stream << "FINALIZE_GRAPH_RESORT_EDGES: " << std::to_string(config.FINALIZE_GRAPH_RESORT_EDGES) << std::endl;
	c3_log_stream << "USE_RANDOM_GENERATOR_SAMPLES: " << std::to_string(config.USE_RANDOM_GENERATOR_SAMPLES) << std::endl;
	c3_log_stream << "IGNORE_BB_ECR: " << std::to_string(config.IGNORE_BB_ECR) << std::endl;

	c3_log_stream << "C3_SAMPLE_NUM_RUNS: " << std::to_string(config.C3_SAMPLE_NUM_RUNS) << std::endl;
	c3_log_stream << "C3_SAMPLE_RUN_SIZE: " << std::to_string(config.C3_SAMPLE_RUN_SIZE) << std::endl;
	
	c3_log_stream << "ENABLE_EQUALITY: " << std::to_string(config.ENABLE_EQUALITY) << std::endl;
	c3_log_stream << "ENABLE_DICT_1TO1: " << std::to_string(config.ENABLE_DICT_1TO1) << std::endl;
	c3_log_stream << "ENABLE_DICT_1TON: " << std::to_string(config.ENABLE_DICT_1TON) << std::endl;
	c3_log_stream << "ENABLE_NUMERICAL: " << std::to_string(config.ENABLE_NUMERICAL) << std::endl;
	c3_log_stream << "ENABLE_DFOR: " << std::to_string(config.ENABLE_DFOR) << std::endl;
	c3_log_stream << "ENABLE_DICT_SHARING: " << std::to_string(config.ENABLE_DICT_SHARING) << std::endl;

	c3_log_stream << "DICT_EXCEPTION_RATIO_THRESHOLD: " << std::to_string(config.DICT_EXCEPTION_RATIO_THRESHOLD) << std::endl;
	c3_log_stream << "EQUALITY_EXCEPTION_RATIO_THRESHOLD: " << std::to_string(config.EQUALITY_EXCEPTION_RATIO_THRESHOLD) << std::endl;
	
	c3_log_stream << "DICTIONARY_COMPRESSION_SCHEME: " << std::to_string(config.DICTIONARY_COMPRESSION_SCHEME) << std::endl;
	c3_log_stream << "DICTIONARY_CODES_COMPRESSION_SCHEME: " << std::to_string(config.DICTIONARY_CODES_COMPRESSION_SCHEME) << std::endl;
	c3_log_stream << "EXCEPTION_COMPRESSION_SCHEME: " << std::to_string(config.EXCEPTION_COMPRESSION_SCHEME) << std::endl;
	c3_log_stream << "DFOR_CODES_COMPRESSION_SCHEME: " << std::to_string(config.DFOR_CODES_COMPRESSION_SCHEME) << std::endl;
	c3_log_stream << "NUMERICAL_CODES_COMPRESSION_SCHEME: " << std::to_string(config.NUMERICAL_CODES_COMPRESSION_SCHEME) << std::endl;
	
	c3_log_stream << "SHARE_ROW_GROUP_SCHEME: " << std::to_string(config.SHARE_ROW_GROUP_SCHEME) << std::endl;
	
	c3_log_stream << std::endl;
}

std::vector<DecompressedColumn> C3Compressor::decompress_row_group(std::vector<std::vector<uint8_t>> compressed_cols){
	// iterate over cols:
	// 	- if BB scheme, decompress
	// 	- if single-col dict, decompress BB only
	// 	- else is target column, do nothing
	// - second loop: decompress all target columns
	// - third loop: decode single-col dict columns 

	std::vector<DecompressedColumn> decompressed_rowgroup(compressed_cols.size());

	for(int i=0; i<4; i++){
		for(int col_i=0; col_i<compressed_cols.size(); col_i++){
			auto c3_meta = reinterpret_cast<c3::C3Chunk*>(compressed_cols[col_i].data());
			if(i==0){
				switch (static_cast<c3::SchemeType>(c3_meta->compression_type)) {
					case c3::SchemeType::BB: {
						assert(c3_meta->btrblocks_ColumnChunkMeta_offset == 0);
						int tuple_count = c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);
						std::vector<btrblocks::u8> output(c3::ChunkDecompression::bb_getDecompressedSize(c3_meta->data));
						std::vector<uint8_t> null_map;
						bool requires_copy = c3::ChunkDecompression::bb_decompressColumn(output, c3_meta->data, null_map);
						decompressed_rowgroup[col_i] = DecompressedColumn(requires_copy, null_map, output, tuple_count);
						break;
					}
					case c3::SchemeType::Single_Dictionary: {
						int tuple_count = c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);
						std::vector<btrblocks::u8> output(c3::ChunkDecompression::bb_getDecompressedSize(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset));
						std::vector<uint8_t> null_map;
						bool requires_copy = c3::ChunkDecompression::bb_decompressColumn(output, c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset, null_map);
						decompressed_rowgroup[col_i] = DecompressedColumn(requires_copy, null_map, output, tuple_count);
						break;
					}
					default: break;
				}
			}
			else if(i==1){
				switch (static_cast<c3::SchemeType>(c3_meta->compression_type)) {
					case c3::SchemeType::Dict_1to1: {
						bool target_requires_copy;
						int source_column_idx = c3_meta->source_column_id;
						int tuple_count = decompressed_rowgroup[source_column_idx].tuple_count;
						auto output = c3::multi_col::Dictionary_1to1::decompress(decompressed_rowgroup[source_column_idx].data, c3_meta, decompressed_rowgroup[source_column_idx].nullmap, tuple_count, target_requires_copy);
						decompressed_rowgroup[col_i] = DecompressedColumn(target_requires_copy, output.second, output.first, tuple_count);
						break;
					}
					case c3::SchemeType::Dict_1toN: {
						assert(c3_meta->btrblocks_ColumnChunkMeta_offset != 0);
						int source_column_idx = c3_meta->source_column_id;
						std::vector<uint8_t> target_output(c3::ChunkDecompression::bb_getDecompressedSize(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset));
						std::vector<uint8_t> target_null_map;
						c3::ChunkDecompression::bb_decompressColumn(target_output,c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset,target_null_map);
						auto target_tuple_count = c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);
						bool target_requires_copy;
						auto decompressed_target = c3::multi_col::Dictionary_1toN::decompress(decompressed_rowgroup[source_column_idx].data, target_output, c3_meta, decompressed_rowgroup[source_column_idx].nullmap, target_null_map, target_tuple_count, target_requires_copy);
						decompressed_rowgroup[col_i] = DecompressedColumn(target_requires_copy, target_null_map, decompressed_target, target_tuple_count);
						break;
					}
					case c3::SchemeType::DFOR: {
						int tuple_count = c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);
						int source_column_idx = c3_meta->source_column_id;
						bool target_requires_copy;
						auto output = c3::multi_col::DFOR::decompress(decompressed_rowgroup[source_column_idx].data, c3_meta, decompressed_rowgroup[source_column_idx].nullmap, tuple_count, target_requires_copy);
						decompressed_rowgroup[col_i] = DecompressedColumn(decompressed_rowgroup[source_column_idx].requires_copy, output.second, output.first, tuple_count);
						break;
					}
					case c3::SchemeType::Dict_Sharing: {
						auto source_col_meta = reinterpret_cast<c3::C3Chunk*>(compressed_cols[c3_meta->source_column_id].data());
						auto source_dict_meta = reinterpret_cast<c3::DictMeta*>(source_col_meta->data);
						std::vector<uint8_t> decompressed_dict_values;
						std::vector<uint8_t> decompressed_dict_nullmap;
						auto requires_copy = ChunkDecompression::bb_decompressColumn(decompressed_dict_values, source_dict_meta->data, decompressed_dict_nullmap);

						int tuple_count = c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);
						std::vector<uint8_t> target_output(c3::ChunkDecompression::bb_getDecompressedSize(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset));
						std::vector<uint8_t> target_null_map;
						c3::ChunkDecompression::bb_decompressColumn(target_output,c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset,target_null_map);
						auto target_tuple_count = c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);
						auto output = c3::single_col::Dictionary::decompress(target_output, source_dict_meta, tuple_count, c3_meta->original_col_size, target_null_map, requires_copy, decompressed_dict_values, decompressed_dict_nullmap);
						decompressed_rowgroup[col_i] = DecompressedColumn(requires_copy, output.second, output.first, tuple_count);
						break;
					}
					default: break;
				}
			}
			else if(i==2){
				switch (static_cast<c3::SchemeType>(c3_meta->compression_type)) {
					case c3::SchemeType::Single_Dictionary: {
						int tuple_count = c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);
						auto dict_meta = reinterpret_cast<c3::DictMeta*>(c3_meta->data);
						auto output = c3::single_col::Dictionary::decompress(decompressed_rowgroup[col_i].data, dict_meta, tuple_count, c3_meta->original_col_size, decompressed_rowgroup[col_i].nullmap, decompressed_rowgroup[col_i].requires_copy);
						decompressed_rowgroup[col_i] = DecompressedColumn(decompressed_rowgroup[col_i].requires_copy, output.second, output.first, tuple_count);
						break;
					}
					default: break;
				}
			}
			else if(i==3){
				switch (static_cast<c3::SchemeType>(c3_meta->compression_type)) {
					case c3::SchemeType::Equality: {
						int source_column_idx = c3_meta->source_column_id;
						int tuple_count = decompressed_rowgroup[source_column_idx].tuple_count;
						auto output = c3::multi_col::Equality::decompress(decompressed_rowgroup[source_column_idx].data, c3_meta, tuple_count, decompressed_rowgroup[source_column_idx].requires_copy);
						decompressed_rowgroup[col_i] = DecompressedColumn(decompressed_rowgroup[source_column_idx].requires_copy, output.second, output.first, tuple_count);
						break;
					}
					case c3::SchemeType::Numerical: {
						int tuple_count = c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);
						int source_column_idx = c3_meta->source_column_id;
						auto output = c3::multi_col::Numerical::decompress(decompressed_rowgroup[source_column_idx].data, c3_meta, decompressed_rowgroup[source_column_idx].nullmap, tuple_count, decompressed_rowgroup[source_column_idx].requires_copy);
						decompressed_rowgroup[col_i] = DecompressedColumn(decompressed_rowgroup[source_column_idx].requires_copy, output.second, output.first, tuple_count);
						break;
					}
					default: break;
				}
			}
		}
	}

	return decompressed_rowgroup;
}

int C3Compressor::compress_and_verify_c3(std::ofstream& log_stream, btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, c3_bench::Dataset dataset) {
	finalized_schemes.clear();
	original_c3_graph = nullptr;
	std::vector<std::shared_ptr<C3LoggingInfo>> c3_rg_log_info(ranges.size());
	std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>> bb_info(ranges.size());
	for(size_t rg_i=0; rg_i<ranges.size(); rg_i++){	
		log_stream << "Compressing: " << dataset.dataset_name << "; Row Group " << rg_i << std::endl;
		
		auto compressed_cols = compress_rowGroup_c3(relation, ranges, rg_i, dataset, c3_rg_log_info, bb_info);
		auto decompressed_cols = C3Compressor::decompress_row_group(compressed_cols);

		log_stream << "Verifying: " << dataset.dataset_name << "; Row Group " << rg_i << std::endl;
		std::vector<btrblocks::InputChunk> original_chunks;
		for (size_t col_i = 0; col_i < relation.columns.size(); col_i++) {
			original_chunks.push_back(relation.getInputChunk(ranges[rg_i], 0, col_i));
		}
		for(int col_i=0; col_i<decompressed_cols.size(); col_i++){
			size_t tuple_count = original_chunks[col_i].tuple_count;
			if (!original_chunks[col_i].compareContents(decompressed_cols[col_i].data.data(), decompressed_cols[col_i].nullmap, decompressed_cols[col_i].tuple_count, decompressed_cols[col_i].requires_copy, col_i, rg_i)){
				return 1;
			}
		}
	}	
	return 0;
}

int C3Compressor::compress_and_verify_c3_old(std::ofstream& log_stream, btrblocks::Relation& relation, std::vector<btrblocks::Range> ranges, c3_bench::Dataset dataset) {

	finalized_schemes.clear();
	original_c3_graph = nullptr;
	std::vector<std::shared_ptr<C3LoggingInfo>> c3_rg_log_info(ranges.size());
	for(size_t rg_i=0; rg_i<ranges.size(); rg_i++){
		
		log_stream << "Compressing: " << dataset.dataset_name << "; Row Group " << rg_i << std::endl;
		std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>> bb_info;
		auto compressed_cols = compress_rowGroup_c3(relation, ranges, rg_i, dataset, c3_rg_log_info, bb_info);
		
		log_stream << "Verifying: " << dataset.dataset_name << "; Row Group " << rg_i << std::endl;
	
		std::vector<btrblocks::InputChunk> original_chunks;
		for (size_t col_i = 0; col_i < relation.columns.size(); col_i++) {
			original_chunks.push_back(relation.getInputChunk(ranges[rg_i], 0, col_i));
		}

		for (size_t col_i = 0; col_i < compressed_cols.size(); col_i++) {
			
			auto   c3_meta = reinterpret_cast<c3::C3Chunk*>(compressed_cols[col_i].data());
			log_stream << "Verifying column " << col_i << ": " << relation.columns[col_i].name << "(" << Utils::scheme_to_string(static_cast<c3::SchemeType>(c3_meta->compression_type)) << ")" << std::endl;

			size_t tup_c   = original_chunks[col_i].tuple_count;
			switch (static_cast<c3::SchemeType>(c3_meta->compression_type)) {
			case c3::SchemeType::BB: {
				assert(c3_meta->btrblocks_ColumnChunkMeta_offset == 0);

				std::vector<btrblocks::u8> output(c3::ChunkDecompression::bb_getDecompressedSize(c3_meta->data));
				std::vector<uint8_t>       null_map;
				bool requires_copy = c3::ChunkDecompression::bb_decompressColumn(output, c3_meta->data, null_map);
				// auto bm = c3::ChunkDecompression::bb_getBitmap(c3_meta->data)->writeBITMAP();
				if (!original_chunks[col_i].compareContents(
						output.data(), null_map, c3::ChunkDecompression::bb_getTupleCount(c3_meta->data), requires_copy, col_i, rg_i)) {
					return 1;
				}

				break;
			}
			case c3::SchemeType::Single_Dictionary: {

				std::vector<btrblocks::u8> output(c3::ChunkDecompression::bb_getDecompressedSize(
					c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset));
				std::vector<uint8_t>       null_map;
				bool                       requires_copy = c3::ChunkDecompression::bb_decompressColumn(
					output, c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset, null_map);

				auto dict_meta = reinterpret_cast<c3::DictMeta*>(c3_meta->data);

				auto decompressed_col = c3::single_col::Dictionary::decompress(
					output, dict_meta, tup_c, c3_meta->original_col_size, null_map, requires_copy);

				if (!original_chunks[col_i].compareContents(
						decompressed_col.first.data(),
						null_map,
						c3::ChunkDecompression::bb_getTupleCount(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset),
						requires_copy, col_i, rg_i)) {
					return 1;
				}

				break;
			}
			case c3::SchemeType::Equality: {

				// decompress source column
				auto source_col_meta =
					reinterpret_cast<c3::C3Chunk*>(compressed_cols[c3_meta->source_column_id].data());
				// assert(source_col_meta->btrblocks_ColumnChunkMeta_offset == 0);

				std::vector<uint8_t> source_output(c3::ChunkDecompression::bb_getDecompressedSize(source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset));
				std::vector<uint8_t> source_null_map;
				int tuple_count = c3::ChunkDecompression::bb_getTupleCount(source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset);
				bool requires_copy = c3::ChunkDecompression::bb_decompressColumn(source_output,source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset,source_null_map);
				
				if(source_col_meta->btrblocks_ColumnChunkMeta_offset > 0){
					// source is C3::single_dict compressed -> single col dict decompress
					auto dict_meta = reinterpret_cast<c3::DictMeta*>(source_col_meta->data);
					auto decompressed_col = c3::single_col::Dictionary::decompress(source_output, dict_meta, tup_c, source_col_meta->original_col_size, source_null_map, requires_copy);
					source_output = decompressed_col.first;
				}

				// patch exceptions to get target column
				std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> output =
					c3::multi_col::Equality::decompress(source_output, c3_meta, tup_c, requires_copy);

				if (!original_chunks[col_i].compareContents(output.first.data(),
														output.second,
														tuple_count,
														requires_copy, col_i, rg_i)) {
					throw Generic_Exception("Decompression yields different contents");
				}

				break;
			}
			case c3::SchemeType::Dict_1to1: {

				// btrblocks decompress source column
				auto source_col_meta =
					reinterpret_cast<c3::C3Chunk*>(compressed_cols[c3_meta->source_column_id].data());
				assert(source_col_meta->btrblocks_ColumnChunkMeta_offset != 0);

				std::vector<uint8_t> source_output(c3::ChunkDecompression::bb_getDecompressedSize(
					source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset));
				std::vector<uint8_t> source_null_map;

				// TODO clean
				bool requires_copy = c3::ChunkDecompression::bb_decompressColumn(
					source_output,
					source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset,
					source_null_map);

				auto tuple_count = c3::ChunkDecompression::bb_getTupleCount(
					source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset);

				// decompress target: apply cross dict, excpetions and target dict
				bool                                                            target_requires_copy;
				std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> decomrpessed_target =
					c3::multi_col::Dictionary_1to1::decompress(
						source_output, c3_meta, source_null_map, tuple_count, target_requires_copy);

				if (!original_chunks[col_i].compareContents(
						decomrpessed_target.first.data(), decomrpessed_target.second, tuple_count, target_requires_copy, col_i, rg_i)) {
					return 1;
				}

				break;
			}
			case c3::SchemeType::Numerical: {
				// decompress source column
				auto source_col_meta =
					reinterpret_cast<c3::C3Chunk*>(compressed_cols[c3_meta->source_column_id].data());
				
				std::vector<uint8_t> source_output(c3::ChunkDecompression::bb_getDecompressedSize(source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset));
				std::vector<uint8_t> source_null_map;
				int tuple_count = c3::ChunkDecompression::bb_getTupleCount(source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset);
				bool requires_copy = c3::ChunkDecompression::bb_decompressColumn(source_output,source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset,source_null_map);

				if(source_col_meta->btrblocks_ColumnChunkMeta_offset > 0){
					auto dict_meta = reinterpret_cast<c3::DictMeta*>(source_col_meta->data);
					auto decompressed_col = c3::single_col::Dictionary::decompress(source_output, dict_meta, tup_c, source_col_meta->original_col_size, source_null_map, requires_copy);
					source_output = decompressed_col.first;
				}

				// patch exceptions to get target column
				std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> output =
					c3::multi_col::Numerical::decompress(source_output, c3_meta, source_null_map, tup_c, requires_copy);

				if (!original_chunks[col_i].compareContents(output.first.data(),
														output.second,
														tuple_count,
														requires_copy, col_i, rg_i)) {
					throw Generic_Exception("Decompression yields different contents");
				}
				break;
			}
			case c3::SchemeType::DFOR: {
				// btrblocks decompress source column
				auto source_col_meta =
					reinterpret_cast<c3::C3Chunk*>(compressed_cols[c3_meta->source_column_id].data());
				assert(source_col_meta->btrblocks_ColumnChunkMeta_offset != 0);

				std::vector<uint8_t> source_output(c3::ChunkDecompression::bb_getDecompressedSize(
					source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset));
				std::vector<uint8_t> source_null_map;

				bool requires_copy = c3::ChunkDecompression::bb_decompressColumn(
					source_output,
					source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset,
					source_null_map);

				auto tuple_count = c3::ChunkDecompression::bb_getTupleCount(
					source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset);

				bool                                                            target_requires_copy;
				std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> decomrpessed_target =
					c3::multi_col::DFOR::decompress(
						source_output, c3_meta, source_null_map, tuple_count, target_requires_copy);

				if (!original_chunks[col_i].compareContents(
						decomrpessed_target.first.data(), decomrpessed_target.second, tuple_count, target_requires_copy, col_i, rg_i)) {
					return 1;
				}
				break;
			}
			case c3::SchemeType::Dict_1toN: {

				// btrblocks decompress source column
				auto source_col_meta = reinterpret_cast<c3::C3Chunk*>(compressed_cols[c3_meta->source_column_id].data());
				assert(source_col_meta->btrblocks_ColumnChunkMeta_offset != 0);
				std::vector<uint8_t> source_output(c3::ChunkDecompression::bb_getDecompressedSize(source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset));
				std::vector<uint8_t> source_null_map;

				bool requires_copy = c3::ChunkDecompression::bb_decompressColumn(
					source_output,
					source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset,
					source_null_map);

				auto source_tuple_count = c3::ChunkDecompression::bb_getTupleCount(
					source_col_meta->data + source_col_meta->btrblocks_ColumnChunkMeta_offset);

				// btrblocks decompress target column
				assert(c3_meta->btrblocks_ColumnChunkMeta_offset != 0);
				std::vector<uint8_t> target_output(c3::ChunkDecompression::bb_getDecompressedSize(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset));
				std::vector<uint8_t> target_null_map;

				c3::ChunkDecompression::bb_decompressColumn(
					target_output,
					c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset,
					target_null_map);

				auto target_tuple_count = c3::ChunkDecompression::bb_getTupleCount(
					c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);

				assert(source_tuple_count == target_tuple_count);

				// decompress target
				bool target_requires_copy;
				std::vector<uint8_t> decompressed_target =
					c3::multi_col::Dictionary_1toN::decompress(
						source_output, target_output, c3_meta, source_null_map, target_null_map, target_tuple_count, target_requires_copy);

				if (!original_chunks[col_i].compareContents(
						decompressed_target.data(), target_null_map, target_tuple_count, target_requires_copy, col_i, rg_i)) {
					return 1;
				}

				break;
			}
			case c3::SchemeType::Dict_Sharing: {

				// get/decompress dict from source col
				auto source_col_meta = reinterpret_cast<c3::C3Chunk*>(compressed_cols[c3_meta->source_column_id].data());
				auto source_dict_meta = reinterpret_cast<c3::DictMeta*>(source_col_meta->data);
				std::vector<uint8_t> decompressed_dict_values;
				std::vector<uint8_t> decompressed_dict_nullmap;
				auto requires_copy = ChunkDecompression::bb_decompressColumn(decompressed_dict_values, source_dict_meta->data, decompressed_dict_nullmap);

				// BB decompress target col
				std::vector<uint8_t> target_output(c3::ChunkDecompression::bb_getDecompressedSize(c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset));
				std::vector<uint8_t> target_null_map;

				c3::ChunkDecompression::bb_decompressColumn(
					target_output,
					c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset,
					target_null_map);

				auto target_tuple_count = c3::ChunkDecompression::bb_getTupleCount(
					c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset);

				// pass dict and decompressed target to dict-decoder
				auto decompressed_target = c3::single_col::Dictionary::decompress(
					target_output, source_dict_meta, tup_c, c3_meta->original_col_size, target_null_map, requires_copy, decompressed_dict_values, decompressed_dict_nullmap);

				if (!original_chunks[col_i].compareContents(
						decompressed_target.first.data(), target_null_map, target_tuple_count, requires_copy, col_i, rg_i)) {
					return 1;
				}

				break;
			}
			default:
				throw Generic_Exception("Verify: C3 scheme not found");
			}
		}
	}
	return 0;
}

void C3Compressor::verify_or_die(bool verify, const std::string& filename, const std::vector<btrblocks::InputChunk>& input_chunks) {
	if (!verify) { return; }
	// Verify that decompression works
	thread_local std::vector<char> COMPRESSED_DATA;
	btrblocks::Utils::readFileToMemory(filename, COMPRESSED_DATA);
	btrblocks::BtrReader reader(COMPRESSED_DATA.data());
	for (btrblocks::SIZE chunk_i = 0; chunk_i < reader.getChunkCount(); chunk_i++) {
		std::vector<btrblocks::u8> output(reader.getDecompressedSize(chunk_i));
		bool                       requires_copy = reader.readColumn(output, chunk_i);
		auto                       bm            = reader.getBitmap(chunk_i)->writeBITMAP();
		if (!input_chunks[chunk_i].compareContents(output.data(), bm, reader.getTupleCount(chunk_i), requires_copy)) {
			throw Generic_Exception("Decompression yields different contents");
		}
	}
}

}