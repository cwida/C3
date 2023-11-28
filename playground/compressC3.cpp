#include "c3/C3Compressor.hpp"

int main(int argc, char **argv) {
    
    c3::C3Compressor compressor;

    std::vector<c3_bench::Dataset> datasets;
    if(argc == 2){
        datasets = {compressor.get_dataset(std::stoi(argv[1]))};
    }
    else{
        datasets = c3_bench::datasets_public_bi_small;
    }

    for(auto dataset: datasets){
		std::vector<std::shared_ptr<c3::C3LoggingInfo>> c3_rg_log_info;
        auto relation_range = compressor.get_btrblocks_relation(dataset);
        std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>> bb_info;

        // compress C3
        auto c3_start_time = std::chrono::steady_clock::now();
        auto c3_compressed = compressor.compress_table_c3(relation_range.first, relation_range.second, dataset, c3_rg_log_info, bb_info);
        auto c3_end_time = std::chrono::steady_clock::now();
        auto c3_runtime = std::chrono::duration_cast<std::chrono::milliseconds>(c3_end_time - c3_start_time);
        
        std::cout << dataset.dataset_name << " C3 time: " << 1.0 * c3_runtime.count() / 1000 << std::endl;
    }

	return 0;
}