#include "c3/C3Compressor.hpp"
#include "c3/Utils.hpp"


// loop through each config
// for each config, change dataset result path´of output streams

std::vector<int> sample_sizes = {327, 655, 3276, 6553, 16384, 32768, 49152, 65536};
std::vector<std::string> scheme_names = {"equality", "d1to1", "d1ton", "numerical", "dfor", "dshare"};
std::vector<std::vector<bool>> schemes = {{true, false, false, false, false, false},
                                            {false, true, false, false, false, false},
                                            {false, false, true, false, false, false},
                                            {false, false, false, true, false, false},
                                            {false, false, false, false, true, false},
                                            {false, false, false, false, false, true}};
std::vector<int> share_schemes = {0,1,2};
void compress(int argc, char **argv);

void init_config(){
    config.C3_WINDOW_SIZE                  = 100;
    config.BYTES_SAVED_MARGIN              = 0.01;
    config.C3_GRAPH_SHARE_SOURCE_NODES     = true;
    config.REVERT_BB_IF_C3_BAD             = false; // changed
    config.USE_PRUNING_RULES               = true; // changed
    config.FINALIZE_GRAPH_RESORT_EDGES     = false;
    config.USE_RANDOM_GENERATOR_SAMPLES    = false;
    config.IGNORE_BB_ECR                   = false; // changed

    // 327	655	3276	6553	16384	32768	49152	65536
    config.C3_SAMPLE_NUM_RUNS    = 327; // 1
    config.C3_SAMPLE_RUN_SIZE    = 1; // 65536

    //---------- SCHEME CONFIG ----------//
    config.ENABLE_EQUALITY      = true;
    config.ENABLE_DICT_1TO1     = true;
    config.ENABLE_DICT_1TON     = true;
    config.ENABLE_NUMERICAL     = true;
    config.ENABLE_DFOR          = true;
    config.ENABLE_DICT_SHARING  = true;

    config.DICT_EXCEPTION_RATIO_THRESHOLD     = 0.10;
    config.EQUALITY_EXCEPTION_RATIO_THRESHOLD = 0.10;

    // 255 = autoscheme(); 0 = Uncompressed; 5 = BP; 4 = PFOR
    config.DICTIONARY_COMPRESSION_SCHEME         = 0; 
    config.DICTIONARY_CODES_COMPRESSION_SCHEME   = 5;
    config.EXCEPTION_COMPRESSION_SCHEME          = 0;
    config.DFOR_CODES_COMPRESSION_SCHEME         = 5;
    config.NUMERICAL_CODES_COMPRESSION_SCHEME    = 5;

    //---------- MULTI ROW GROUP CONFIG ----------//
    // 0 = none; 1 = share finalized; 2 = share graph schemes
    config.SHARE_ROW_GROUP_SCHEME = 1;
}

void config_schemes_samples(int sample_size, std::vector<bool> schemes){
    config.C3_SAMPLE_NUM_RUNS    = sample_size;
    config.C3_SAMPLE_RUN_SIZE    = 1;

    config.ENABLE_EQUALITY      = schemes[0];
    config.ENABLE_DICT_1TO1     = schemes[1];
    config.ENABLE_DICT_1TON     = schemes[2];
    config.ENABLE_NUMERICAL     = schemes[3];
    config.ENABLE_DFOR          = schemes[4];
    config.ENABLE_DICT_SHARING  = schemes[5];
};

int main(int argc, char **argv) {

    init_config();

    // // 2. all schemes, best case/max potential
    std::string test_id = "2";
    for(int scheme_i=0; scheme_i<schemes.size(); scheme_i++){  
        for(auto sample_size: sample_sizes){
            std::cout << test_id + "/" + scheme_names[scheme_i]  + "/" + std::to_string(sample_size) << std::endl;
            c3_bench::result_path = c3_bench::public_bi_path + "result/" + test_id + "/" + scheme_names[scheme_i] + "/" + std::to_string(sample_size) + "/";
            config_schemes_samples(sample_size, schemes[scheme_i]);
            compress(argc, argv);
        }
    }

    // 3. all schemes 
    test_id = "3";
    std::vector<bool> all_schemes = {true, true, true, true, true, true};
    for(auto sample_size: sample_sizes){
        std::cout << test_id + "/" + std::to_string(sample_size) << std::endl;
        c3_bench::result_path = c3_bench::public_bi_path + "result/" + test_id + "/" + std::to_string(sample_size) + "/";
        config_schemes_samples(sample_size, all_schemes);
        compress(argc, argv);
    }

    // 4. all schemes, reversing
    test_id = "4";
    config.REVERT_BB_IF_C3_BAD = true; 
    for(auto sample_size: sample_sizes){
        if(sample_size == 327) continue;
        std::cout << test_id + "/" + std::to_string(sample_size) << std::endl;
        c3_bench::result_path = c3_bench::public_bi_path + "result/" + test_id + "/" + std::to_string(sample_size) + "/";
        config_schemes_samples(sample_size, all_schemes);
        compress(argc, argv);
    }

    // 5. all schemes, no source sharing
    test_id = "5";
    config.C3_GRAPH_SHARE_SOURCE_NODES = false;
    for(auto sample_size: sample_sizes){
        std::cout << test_id + "/" + std::to_string(sample_size) << std::endl;
        c3_bench::result_path = c3_bench::public_bi_path + "result/" + test_id + "/" + std::to_string(sample_size) + "/";
        config_schemes_samples(sample_size, all_schemes);
        compress(argc, argv);
    }
    config.C3_GRAPH_SHARE_SOURCE_NODES = true;
    
    // 6. all schemes, different sample run sizes
    test_id = "6";
    std::vector<std::pair<int,int>> sample_configs = {{660, 1}, {66, 10}, {20, 33}, {10, 66}, {1, 660}}; // run count, run_size
    for(auto sample_config: sample_configs){
        config.C3_SAMPLE_NUM_RUNS    = sample_config.first;
        config.C3_SAMPLE_RUN_SIZE    = sample_config.second;
        std::cout << test_id + "/" + std::to_string(sample_config.first) + "_" + std::to_string(sample_config.second) << std::endl;
        c3_bench::result_path = c3_bench::public_bi_path + "result/" + test_id + "/" + std::to_string(sample_config.first) + "_" + std::to_string(sample_config.second) + "/";
        compress(argc, argv);
    }
    config.C3_SAMPLE_RUN_SIZE = 1;

    // 7. multi row group - need dataset with multiple row groups
    test_id = "7";
    int sample_size_multiRG = sample_sizes[1]; // 655
    for(auto share_scheme: share_schemes){
        config.SHARE_ROW_GROUP_SCHEME = share_scheme;
        std::cout << test_id + "/" + std::to_string(share_scheme) << std::endl;
        c3_bench::result_path = c3_bench::public_bi_path + "result/" + test_id + "/" + std::to_string(share_scheme) + "/";
        config_schemes_samples(sample_size_multiRG, all_schemes);
        compress(argc, argv);
    }
    
    return 0;
}

void compress(int argc, char **argv){
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

}