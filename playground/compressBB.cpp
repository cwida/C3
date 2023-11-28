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
        auto relation_range = compressor.get_btrblocks_relation(dataset);

        // compress BB
        auto bb_start_time = std::chrono::steady_clock::now();
        std::vector<std::vector<size_t>> original_sizes;
        auto btrblocks_compressed = compressor.compress_btrblocks(relation_range.first, relation_range.second, dataset, original_sizes);
        auto bb_end_time = std::chrono::steady_clock::now();
        auto bb_runtime = std::chrono::duration_cast<std::chrono::milliseconds>(bb_end_time - bb_start_time);

        std::cout << dataset.dataset_name << " BB time: " << 1.0 * bb_runtime.count() / 1000 << std::endl;
    }

	return 0;
}