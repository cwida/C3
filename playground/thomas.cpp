#include "c3/C3Compressor.hpp"
#include "c3/Utils.hpp"

int main(int argc, char **argv) {
    
    c3::C3Compressor compressor;

    std::vector<c3_bench::Dataset> datasets;
    if(argc == 2){
        datasets = {compressor.get_dataset(std::stoi(argv[1]))};
    }
    else{
        datasets = c3_bench::datasets_public_bi_small;
    }

    compressor.log_config();

    compressor.relations_stats_stream << "Relation|uncompressed|compressed|c3_compressed|compression_ratio_improvement|column_count|compute_ecr_counter|add_to_graph_counter|final_scheme_counter|avg_schemes_per_source" << std::endl;
    compressor.row_group_stats_stream << "Relation|RowGroup|uncompressed|compressed|c3_compressed|compression_ratio_improvement|compute_ecr_counter|add_to_graph_counter|final_scheme_counter" << std::endl;
    compressor.columns_stats_stream << "Relation|RowGroup|Column|uncompressed|bb_compressed|c3_compressed|compression_ratio_improvement|bb_cr|c3_cr|datatype|C3_source_target|" << c3::CompressionScheme::print_log_header() << std::endl;
    compressor.schemes_stats_stream << "Relation|RowGroup|C3_Scheme|source_id|target_id|source_name|target_name|source_type|target_type|uncompressed_source_size|uncompressed_target_size|c3_compressed_source_size|c3_compressed_target_size|c3_source_cr|c3_target_cr|bb_source_size|bb_target_size|bb_source_cr|bb_target_cr|scheme_total_cr_improvement|" << c3::CompressionScheme::print_log_header() << std::endl;

    double avg_compression_ratio_improvement = 0;
    int total_ecr_computed = 0;
    double total_bb_time = 0;
    double total_c3_time = 0;
    double c3_time_overhead = 0;

    for(auto dataset: datasets){

        std::ofstream dataset_stats_stream(dataset.stats_file_out);
		std::vector<std::shared_ptr<c3::C3LoggingInfo>> c3_rg_log_info;
        std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>> bb_info;

        auto relation_range = compressor.get_btrblocks_relation(dataset);
        // compress C3
        auto c3_compressed_sizes = compressor.compress_table_c3(relation_range.first, relation_range.second, dataset, c3_rg_log_info, bb_info);


	    compressor.log_dataset_stats(relation_range.first, dataset_stats_stream, c3_compressed_sizes, bb_info);
        compressor.log_column_stats(relation_range.first, dataset, c3_compressed_sizes, c3_rg_log_info, bb_info);        
        int num_ecr_computed;
        auto cr_improv = compressor.log_relation_stats(relation_range.first, dataset, c3_compressed_sizes, c3_rg_log_info, num_ecr_computed, bb_info);
	    avg_compression_ratio_improvement += cr_improv;
        total_ecr_computed += num_ecr_computed;
        compressor.log_scheme_stats(relation_range.first, dataset, c3_rg_log_info, c3_compressed_sizes, bb_info);
        compressor.log_row_group_stats(relation_range.first, dataset, c3_compressed_sizes, c3_rg_log_info, bb_info);
    
    }

    compressor.relations_stats_stream << "Average compression_ratio_improvement: " << avg_compression_ratio_improvement / datasets.size() << std::endl;
    compressor.relations_stats_stream << "Total ECRs computed: " << total_ecr_computed << std::endl;

	return 0;
}