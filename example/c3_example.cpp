#include "c3/C3Compressor.hpp"

int main(int argc, char **argv) {
    
    c3::C3Compressor compressor;
    std::vector<c3_bench::Dataset> datasets = c3_bench::datasets_public_bi_small;

    for(auto dataset: datasets){
		std::vector<std::shared_ptr<c3::C3LoggingInfo>> rg_log_info;
        std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>> bb_info;

        // compress C3
        auto relation_range = compressor.get_btrblocks_relation(dataset);
        auto c3_compressed_sizes = compressor.compress_table_c3(relation_range.first, relation_range.second, dataset, rg_log_info, bb_info);

        // log
        size_t total_uncompressed = 0;
        size_t total_c3_compressed = 0;
        size_t total_bb_compressed = 0;
        for(size_t row_group=0; row_group<c3_compressed_sizes.size(); row_group++){
            for(size_t col=0; col<relation_range.first.columns.size(); col++){
                total_uncompressed += bb_info[row_group][col]->uncompressed_size;
                total_c3_compressed += c3_compressed_sizes[row_group][col];
                total_bb_compressed +=bb_info[row_group][col]->get_BB_compressed_size();
            }
        }

        double compression_ratio_improvement = 1.0 * total_bb_compressed / total_c3_compressed;
    
        std::cout << dataset.dataset_name << " C3 compression ratio improvement: " << compression_ratio_improvement << std::endl;

    }

	return 0;
}