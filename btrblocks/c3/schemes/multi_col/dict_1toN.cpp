#include "dict_1toN.hpp"
#include "c3/Utils.hpp"
#include "compression/Datablock.hpp"
#include "c3/schemes/single_col/dictionary.hpp"

#include <algorithm>

namespace c3{
namespace multi_col{

bool Dictionary_1toN::skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j){
    // trim using column statistics?
        // based on #unique values in row group
        // this dict is better if col[i] has more unique vales than col[j]
    
    if(config.USE_PRUNING_RULES){
        double unique_count_threshold = row_group->columns[i]->tuple_count * 0.15;
        if(btrBlocksSchemes[i]->get_unique_count() > unique_count_threshold){                    
            return true;
        }
        if(btrBlocksSchemes[j]->get_unique_count() > unique_count_threshold){                                
            return true;
        }
    }
    
    return false;
}


double Dictionary_1toN::expectedCompressionRatio(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::vector<int>& samples, std::shared_ptr<ColumnStats> sourceStats, std::shared_ptr<ColumnStats> targetStats, int& estimated_source_target_dict_size, int& estimated_target_compressed_codes_size, int& estimated_offsets_size, std::shared_ptr<RowGroup> row_group){
    // only handle target col here, source is compressed with single dict
    // target compressed size = target_ids_BP + sizeof(source-target-dict) + sizeof(offsets)
        // target_ids_BP = (floor(log2(avg_child_count)) + 1) * tuple_count
        // offsets = source_unique_count * sizeof(int)
        // source-target-dict = source_unique_count * avg_child_count * sizeof(datatype)
            // minimum source-target-dict size is sizeof(unique_target_vals)
            // avg str size of strings = stringStats.totalLength / stringStats.tuple_count
            // how to extrapolate sample_avg_child_count to entire row group?
            // ALT IDEA: 
                // source-target-dict = sizeof(unique_target_val) + #repeated_values * sizeof(datatype)
                // if every target column only appears once, then sizeof(source-target-dict) = sizeof(unique_target_val)
                // compute how many unique target vals are mapped extra -> extrapolate this count to row group
                    // need reverse_map: target val to source val 

    // we need from samples: 
        // avg number of children per parent
        // number of target values mapped to multiple source vals

    // handling null values
        // store original nullmap of target
        // if target null, ignore, don't map
        // if source null, use id 0 to encode null in dict?
    
    int mapped_count = 0;
    int uncompressed_size = target_column->size;
    int compressed_size = sizeof(C3Chunk) + sizeof(Dict1toNMeta) + 2 * sizeof(DictMeta);

    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
                    
            // size of offsets; +1 to store offset for null
            estimated_offsets_size = (sourceStats->intStats->unique_count + 1) * sizeof(btrblocks::units::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; 
            compressed_size += estimated_offsets_size;

            switch(target_column->type){
                case btrblocks::ColumnType::INTEGER:{
                    // map parent val to source vals, map to source val id
                    std::map<int,std::map<int,int>> map;
                    std::map<int,std::set<int>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->integers()[samples[i]]+1].count(target_column->integers()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->integers()[samples[i]]+1][target_column->integers()[samples[i]]] = map[source_column->integers()[samples[i]]+1].size();
                                    reverse_map[target_column->integers()[samples[i]]].insert(source_column->integers()[samples[i]]+1);
                                }
                                sample_encoded_target.push_back(map[source_column->integers()[samples[i]]+1][target_column->integers()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map[0].count(target_column->integers()[samples[i]])==0){
                                    map[0][target_column->integers()[samples[i]]] = map[0].size();
                                    reverse_map[target_column->integers()[samples[i]]].insert(0);
                                }
                                sample_encoded_target.push_back(map[0][target_column->integers()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }

                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size
                    
                    compressed_size += estimated_target_compressed_codes_size;

                    estimated_source_target_dict_size = (targetStats->intStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::units::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
                case btrblocks::ColumnType::DOUBLE:{
                    // map parent val to source vals, map to source val id
                    std::map<int,std::map<double,int>> map;
                    std::map<double,std::set<int>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->integers()[samples[i]]+1].count(target_column->doubles()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->integers()[samples[i]]+1][target_column->doubles()[samples[i]]] = map[source_column->integers()[samples[i]]+1].size();
                                    reverse_map[target_column->doubles()[samples[i]]].insert(source_column->integers()[samples[i]]+1);
                                }
                                sample_encoded_target.push_back(map[source_column->integers()[samples[i]]+1][target_column->doubles()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map[0].count(target_column->doubles()[samples[i]])==0){
                                    map[0][target_column->doubles()[samples[i]]] = map[0].size();
                                    reverse_map[target_column->doubles()[samples[i]]].insert(0);
                                }
                                sample_encoded_target.push_back(map[0][target_column->doubles()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }

                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size

                    compressed_size += estimated_target_compressed_codes_size;

                    estimated_source_target_dict_size = (targetStats->doubleStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::units::DOUBLE) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
                case btrblocks::ColumnType::STRING:{
                    // map parent val to source vals, map to source val id
                    std::map<int,std::map<std::string_view,int>> map;
                    std::map<std::string_view,std::set<int>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->integers()[samples[i]]+1].count(target_column->strings()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->integers()[samples[i]]+1][target_column->strings()[samples[i]]] = map[source_column->integers()[samples[i]]+1].size();
                                    reverse_map[target_column->strings()[samples[i]]].insert(source_column->integers()[samples[i]]+1);
                                }
                                sample_encoded_target.push_back(map[source_column->integers()[samples[i]]+1][target_column->strings()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map[0].count(target_column->strings()[samples[i]])==0){
                                    map[0][target_column->strings()[samples[i]]] = map[0].size();
                                    reverse_map[target_column->strings()[samples[i]]].insert(0);
                                }
                                sample_encoded_target.push_back(map[0][target_column->strings()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }

                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    double avg_string_length = 1.0 * targetStats->stringStats->total_length / targetStats->stringStats->tuple_count;

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size
                    
                    compressed_size += estimated_target_compressed_codes_size;
                    
                    estimated_source_target_dict_size = (targetStats->stringStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::StringArrayViewer::Slot) + (targetStats->stringStats->total_unique_length + extrapolated_target_vals_mapped_extra * avg_string_length) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
            }
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
                    
            // size of offsets; +1 to store offset for null
            estimated_offsets_size = (sourceStats->doubleStats->unique_count + 1) * sizeof(btrblocks::units::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; 
            compressed_size += estimated_offsets_size;

            switch(target_column->type){
                case btrblocks::ColumnType::INTEGER:{
                    // map parent val to source vals, map to source val id
                    std::map<double,std::map<int,int>> map;
                    std::map<int,std::set<double>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->doubles()[samples[i]]+1].count(target_column->integers()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->doubles()[samples[i]]+1][target_column->integers()[samples[i]]] = map[source_column->doubles()[samples[i]]+1].size();
                                    reverse_map[target_column->integers()[samples[i]]].insert(source_column->doubles()[samples[i]]+1);
                                }
                                sample_encoded_target.push_back(map[source_column->doubles()[samples[i]]+1][target_column->integers()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map[0].count(target_column->integers()[samples[i]])==0){
                                    map[0][target_column->integers()[samples[i]]] = map[0].size();
                                    reverse_map[target_column->integers()[samples[i]]].insert(0);
                                }
                                sample_encoded_target.push_back(map[0][target_column->integers()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }

                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size
                    
                    compressed_size += estimated_target_compressed_codes_size;
                    
                    estimated_source_target_dict_size = (targetStats->intStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::units::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
                case btrblocks::ColumnType::DOUBLE:{
                    // map parent val to source vals, map to source val id
                    std::map<double,std::map<double,int>> map;
                    std::map<double,std::set<double>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->doubles()[samples[i]]+1].count(target_column->doubles()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->doubles()[samples[i]]+1][target_column->doubles()[samples[i]]] = map[source_column->doubles()[samples[i]]+1].size();
                                    reverse_map[target_column->doubles()[samples[i]]].insert(source_column->doubles()[samples[i]]+1);
                                }
                                sample_encoded_target.push_back(map[source_column->doubles()[samples[i]]+1][target_column->doubles()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map[0].count(target_column->doubles()[samples[i]])==0){
                                    map[0][target_column->doubles()[samples[i]]] = map[0].size();
                                    reverse_map[target_column->doubles()[samples[i]]].insert(0);
                                }
                                sample_encoded_target.push_back(map[0][target_column->doubles()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }
                    
                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size
                    
                    compressed_size += estimated_target_compressed_codes_size;
                 
                    estimated_source_target_dict_size = (targetStats->doubleStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::units::DOUBLE) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
                case btrblocks::ColumnType::STRING:{
                    // map parent val to source vals, map to source val id
                    std::map<double,std::map<std::string_view,int>> map;
                    std::map<std::string_view,std::set<double>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->doubles()[samples[i]]+1].count(target_column->strings()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->doubles()[samples[i]]+1][target_column->strings()[samples[i]]] = map[source_column->doubles()[samples[i]]+1].size();
                                    reverse_map[target_column->strings()[samples[i]]].insert(source_column->doubles()[samples[i]]+1);
                                }
                                sample_encoded_target.push_back(map[source_column->doubles()[samples[i]]+1][target_column->strings()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map[0].count(target_column->strings()[samples[i]])==0){
                                    map[0][target_column->strings()[samples[i]]] = map[0].size();
                                    reverse_map[target_column->strings()[samples[i]]].insert(0);
                                }
                                sample_encoded_target.push_back(map[0][target_column->strings()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }
                    
                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    double avg_string_length = 1.0 * targetStats->stringStats->total_length / targetStats->stringStats->tuple_count;

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size
                    
                    compressed_size += estimated_target_compressed_codes_size;

                    estimated_source_target_dict_size = (targetStats->stringStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::StringArrayViewer::Slot) + (targetStats->stringStats->total_unique_length + extrapolated_target_vals_mapped_extra * avg_string_length) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
            }
            break;
        }
        case btrblocks::ColumnType::STRING:{
                    
            // size of offsets; +1 to store offset for null
            estimated_offsets_size = (sourceStats->stringStats->unique_count + 1) * sizeof(btrblocks::units::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; 
            compressed_size += estimated_offsets_size;

            switch(target_column->type){
                case btrblocks::ColumnType::INTEGER:{
                    // map parent val to source vals, map to source val id
                    std::map<std::string_view,std::map<int,int>> map;
                    std::map<int,std::set<std::string_view>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->strings()[samples[i]]].count(target_column->integers()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->strings()[samples[i]]][target_column->integers()[samples[i]]] = map[source_column->strings()[samples[i]]].size();
                                    reverse_map[target_column->integers()[samples[i]]].insert(source_column->strings()[samples[i]]);
                                }
                                sample_encoded_target.push_back(map[source_column->strings()[samples[i]]][target_column->integers()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map["null"].count(target_column->integers()[samples[i]])==0){
                                    map["null"][target_column->integers()[samples[i]]] = map["null"].size();
                                    reverse_map[target_column->integers()[samples[i]]].insert("null");
                                }
                                sample_encoded_target.push_back(map["null"][target_column->integers()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }
                    // avg_num_children = std::ceil(1.0 * avg_num_children / map.size()); 
                    // avg_num_children /= map.size(); 

                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size
                    
                    compressed_size += estimated_target_compressed_codes_size;
                    
                    estimated_source_target_dict_size = (targetStats->intStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::units::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
                case btrblocks::ColumnType::DOUBLE:{
                    // map parent val to source vals, map to source val id
                    std::map<std::string_view,std::map<double,int>> map;
                    std::map<double,std::set<std::string_view>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->strings()[samples[i]]].count(target_column->doubles()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->strings()[samples[i]]][target_column->doubles()[samples[i]]] = map[source_column->strings()[samples[i]]].size();
                                    reverse_map[target_column->doubles()[samples[i]]].insert(source_column->strings()[samples[i]]);
                                }
                                sample_encoded_target.push_back(map[source_column->strings()[samples[i]]][target_column->doubles()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map["null"].count(target_column->doubles()[samples[i]])==0){
                                    map["null"][target_column->doubles()[samples[i]]] = map["null"].size();
                                    reverse_map[target_column->doubles()[samples[i]]].insert("null");
                                }
                                sample_encoded_target.push_back(map["null"][target_column->doubles()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }
                    
                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size
                    
                    compressed_size += estimated_target_compressed_codes_size;

                    estimated_source_target_dict_size = (targetStats->doubleStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::units::DOUBLE) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
                case btrblocks::ColumnType::STRING:{
                    // map parent val to source vals, map to source val id
                    std::map<std::string_view,std::map<std::string_view,int>> map;
                    std::map<std::string_view,std::set<std::string_view>> reverse_map;
                    std::vector<int> sample_encoded_target;
                    for(size_t i=0; i<samples.size(); i++){
                        if(target_column->nullmap[samples[i]] == 1){
                            if(source_column->nullmap[samples[i]] == 1){
                                if(map[source_column->strings()[samples[i]]].count(target_column->strings()[samples[i]])==0){
                                    // +1, since 0 is used for encoding null source value
                                    map[source_column->strings()[samples[i]]][target_column->strings()[samples[i]]] = map[source_column->strings()[samples[i]]].size();
                                    reverse_map[target_column->strings()[samples[i]]].insert(source_column->strings()[samples[i]]);
                                }
                                sample_encoded_target.push_back(map[source_column->strings()[samples[i]]][target_column->strings()[samples[i]]]);
                            }
                            else if(source_column->nullmap[samples[i]] == 0){
                                if(map["null"].count(target_column->strings()[samples[i]])==0){
                                    map["null"][target_column->strings()[samples[i]]] = map["null"].size();
                                    reverse_map[target_column->strings()[samples[i]]].insert("null");
                                }
                                sample_encoded_target.push_back(map["null"][target_column->strings()[samples[i]]]);
                            }
                            mapped_count++;
                        }
                        else{
                            sample_encoded_target.push_back(0);
                        }
                    }
                    
                    // if target samples only null
                    if(mapped_count==0){
                        return 1;
                    }

                    size_t max_num_children = 0;
                    for(auto& parent: map){
                        max_num_children = std::max(max_num_children, parent.second.size());
                    }
                    
                    size_t target_vals_mapped_extra = 0;
                    for(auto& target_val: reverse_map){
                        // assert(target_val.second.size()>0);
                        target_vals_mapped_extra += target_val.second.size()-1;
                    }
                    size_t extrapolated_target_vals_mapped_extra = target_vals_mapped_extra * (target_column->tuple_count / samples.size());

                    double avg_string_length = 1.0 * targetStats->stringStats->total_length / targetStats->stringStats->tuple_count;

                    estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
                    // estimated_target_compressed_codes_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(max_num_children)) + 1) / 8; // encoded target size
                    
                    compressed_size += estimated_target_compressed_codes_size;
                    
                    estimated_source_target_dict_size = (targetStats->stringStats->unique_count + extrapolated_target_vals_mapped_extra) * sizeof(btrblocks::StringArrayViewer::Slot) + (targetStats->stringStats->total_unique_length + extrapolated_target_vals_mapped_extra * avg_string_length) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE; // source-target-dict
                    compressed_size += estimated_source_target_dict_size;
                    break;
                }
            }
            break;
        }
    }

    return 1.0 * uncompressed_size / compressed_size;
}


std::vector<std::vector<uint8_t>> Dictionary_1toN::apply_scheme(std::shared_ptr<btrblocks::InputChunk>& source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::shared_ptr<Dict_1toN_CompressionScheme> scheme, bool skip_source_encoding, bool last_source_column_scheme, const int& source_idx, const int& target_idx, std::vector<uint8_t>* sourceChunkVec, std::shared_ptr<ColumnStats> sourceBBSchemes, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes, std::shared_ptr<RowGroup> row_group){
    // compress
    // dict encode source, save dictionary d1 (and return d1?)
    // dict map source to target vals, assign each val an id
    // create d2: for each source val in order of d1, add target val in order of id
        // todo: remove empty slot filling
    // create offset array: based on number of target vals per source val
    // encode target column as target val id
    // bitpack source col and target col

    // misc
    // if target col is int or double, d2 is array of values
    // if target col is string, d2 is strings in order


    std::shared_ptr<btrblocks::InputChunk> sourceColDictEncoded = source_column;

    if(!skip_source_encoding){
        // 1. 
        // dict encode source column: compute dict (compressed) and encoded source column
        // store dict in scheme
        // update source column with new encoded one

        assert(sourceChunkVec->size()==0);
        *sourceChunkVec = std::vector<uint8_t>(sizeof(C3Chunk) + 10 * source_column->size, 0);
        C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(sourceChunkVec->data());
        sourceChunk->compression_type = static_cast<uint8_t>(SchemeType::Single_Dictionary);
        sourceChunk->type = source_column->type;
        sourceChunk->source_column_id = source_idx; // not used, since compression type set to single dict

        sourceChunk->original_col_size = source_column->size;
        auto source_dict = reinterpret_cast<DictMeta*>(sourceChunk->data);
        source_dict->type = source_column->type;
        size_t compressed_source_dict_size;
        size_t uncompressed_source_dict_size;
        sourceColDictEncoded = single_col::Dictionary::apply_scheme(source_column, source_dict, bbSchemes[source_idx], compressed_source_dict_size, uncompressed_source_dict_size);
        sourceChunk->btrblocks_ColumnChunkMeta_offset = sizeof(DictMeta) + compressed_source_dict_size;
    }

    std::vector<uint8_t> target_output(sizeof(C3Chunk) + 10 * target_column->size, 0);    
    C3Chunk* targetChunk = reinterpret_cast<C3Chunk*>(target_output.data());    
    targetChunk->compression_type = static_cast<uint8_t>(SchemeType::Dict_1toN);    
    targetChunk->type = target_column->type;    
    targetChunk->source_column_id = source_idx;
    targetChunk->original_col_size = target_column->size;

    auto dict_1toN_meta = reinterpret_cast<Dict1toNMeta*>(targetChunk->data);

    // encoded target column buffer
    std::unique_ptr<uint8_t[]> target_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[target_column->tuple_count * sizeof(btrblocks::units::INTEGER)]());
    std::unique_ptr<uint8_t[]> target_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[target_column->tuple_count]());
    auto target_buffer_writer = reinterpret_cast<btrblocks::units::INTEGER*>(target_buffer.get());
    
    // dictionary buffer
    std::unique_ptr<uint8_t[]> dict_buffer;
    std::unique_ptr<uint8_t[]> dict_nullmap_buffer;
    size_t dict_entries;
    size_t dict_size;
    
    // offset buffer
    std::unique_ptr<uint8_t[]> offset_buffer;
    std::unique_ptr<uint8_t[]> offset_nullmap_buffer;
    size_t offset_count;
    size_t offset_size;

    std::shared_ptr<ColumnStats> col_stats_target_dict;

    // compute source-target mapping
    switch(target_column->type){
        case btrblocks::ColumnType::INTEGER:{
            
            // 1. map parent val to target vals, map to target val id
            // 2. write target vals equal to val id within source val
            std::map<int,std::map<int,int>> map;
            
            for(size_t i=0; i<sourceColDictEncoded->tuple_count; i++){
                target_nullmap_buffer[i] = target_column->nullmap[i];
                if(target_column->nullmap[i] == 1){
                    if(sourceColDictEncoded->nullmap[i] == 1){
                        if(map[sourceColDictEncoded->integers()[i]+1].count(target_column->integers()[i])==0){
                            // +1, since 0 is used for encoding null source value
                            map[sourceColDictEncoded->integers()[i]+1][target_column->integers()[i]] = map[sourceColDictEncoded->integers()[i]+1].size();
                        }
                        target_buffer_writer[i] = map[sourceColDictEncoded->integers()[i]+1][target_column->integers()[i]];
                    }
                    else if(sourceColDictEncoded->nullmap[i] == 0){
                        if(map[0].count(target_column->integers()[i])==0){
                            map[0][target_column->integers()[i]] = map[0].size();
                        }
                        target_buffer_writer[i] = map[0][target_column->integers()[i]];
                    }
                }
            }

            size_t total_num_children = 0;
            for(auto& parent: map){
                total_num_children += parent.second.size();
            }
            
            // offset buffer
            offset_count = sourceBBSchemes->get_unique_count() + 1; // +1 for null
            offset_size = offset_count * sizeof(btrblocks::units::INTEGER); 
            offset_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[offset_size]()); 
            auto offset_buffer_writer = reinterpret_cast<btrblocks::units::INTEGER*>(offset_buffer.get());
            offset_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[offset_count]());
            
            // fill offsets
            offset_buffer_writer[0] = 0;
            offset_nullmap_buffer[0] = 1;
            for(size_t i=1; i<offset_count; i++){
                offset_buffer_writer[i] = offset_buffer_writer[i-1] + map[i-1].size();
                offset_nullmap_buffer[i] = 1;
            }
            
            // dictionary buffer
            dict_entries = total_num_children;
            dict_size = dict_entries * sizeof(btrblocks::units::INTEGER);
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_size]()); 
            auto dict_buffer_writer = reinterpret_cast<btrblocks::units::INTEGER*>(dict_buffer.get());
            dict_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_entries]());

            // loop through map, fill in dictionary buffer, all values should be filled        
            for(auto const& [sourceVal, inner_map] : map){
                for(auto const& [targetVal, targetValID] : inner_map){
                    size_t idx = offset_buffer_writer[sourceVal] + targetValID;
                    // assert(idx < dict_entries);
                    dict_buffer_writer[idx] = targetVal;
                    dict_nullmap_buffer[idx] = 1;
                }
            }
        
            auto basic_stats_target_dict = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(dict_entries)); // int uncompressed
            col_stats_target_dict = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_target_dict, {}));

            break;
        }
        case btrblocks::ColumnType::DOUBLE:{

            // 1. map parent val to target vals, map to target val id
            // 2. write target vals equal to val id within source val
            std::map<int,std::map<double,int>> map;
            
            for(size_t i=0; i<sourceColDictEncoded->tuple_count; i++){
                target_nullmap_buffer[i] = target_column->nullmap[i];
                if(target_column->nullmap[i] == 1){
                    if(sourceColDictEncoded->nullmap[i] == 1){
                        if(map[sourceColDictEncoded->integers()[i]+1].count(target_column->doubles()[i])==0){
                            // +1, since 0 is used for encoding null source value
                            map[sourceColDictEncoded->integers()[i]+1][target_column->doubles()[i]] = map[sourceColDictEncoded->integers()[i]+1].size();
                        }
                        target_buffer_writer[i] = map[sourceColDictEncoded->integers()[i]+1][target_column->doubles()[i]];
                    }
                    else if(sourceColDictEncoded->nullmap[i] == 0){
                        if(map[0].count(target_column->doubles()[i])==0){
                            map[0][target_column->doubles()[i]] = map[0].size();
                        }
                        target_buffer_writer[i] = map[0][target_column->doubles()[i]];
                    }
                }
            }

            size_t total_num_children = 0;
            for(auto& parent: map){
                total_num_children += parent.second.size();
            }
            
            // offset buffer
            offset_count = sourceBBSchemes->get_unique_count() + 1; // +1 for null
            offset_size = offset_count * sizeof(btrblocks::units::INTEGER); 
            offset_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[offset_size]()); 
            auto offset_buffer_writer = reinterpret_cast<btrblocks::units::INTEGER*>(offset_buffer.get());
            offset_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[offset_count]());
            
            // fill offsets
            offset_buffer_writer[0] = 0;
            offset_nullmap_buffer[0] = 1;
            for(size_t i=1; i<offset_count; i++){
                offset_buffer_writer[i] = offset_buffer_writer[i-1] + map[i-1].size();
                offset_nullmap_buffer[i] = 1;
            }
            
            // dictionary buffer
            dict_entries = total_num_children;
            dict_size = dict_entries * sizeof(btrblocks::units::DOUBLE);
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_size]()); 
            auto dict_buffer_writer = reinterpret_cast<btrblocks::units::DOUBLE*>(dict_buffer.get());
            dict_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_entries]());

            // loop through map, fill in dictionary buffer, all values should be filled        
            for(auto const& [sourceVal, inner_map] : map){
                for(auto const& [targetVal, targetValID] : inner_map){
                    size_t idx = offset_buffer_writer[sourceVal] + targetValID;
                    assert(idx < dict_entries);
                    dict_buffer_writer[idx] = targetVal;
                    dict_nullmap_buffer[idx] = 1;
                }
            }
        
            auto basic_stats_target_dict = std::make_shared<btrblocks::DoubleStats>(btrblocks::DoubleStats::generateStatsBasic(dict_entries)); // int uncompressed
            col_stats_target_dict = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::DOUBLE, basic_stats_target_dict, {}));

            break;
        }
        case btrblocks::ColumnType::STRING:{

            // 1. map parent val to target vals, map to target val id
            // 2. write target vals equal to val id within source val
            std::map<int,std::map<std::string_view,int>> map;
            std::map<int,std::vector<std::string_view>> dict_build_helper;
            size_t mapped_target_string_size = 0;
            
            for(size_t i=0; i<sourceColDictEncoded->tuple_count; i++){
                target_nullmap_buffer[i] = target_column->nullmap[i];
                if(target_column->nullmap[i] == 1){
                    if(sourceColDictEncoded->nullmap[i] == 1){
                        if(map[sourceColDictEncoded->integers()[i]+1].count(target_column->strings()[i])==0){
                            // +1, since 0 is used for encoding null source value
                            map[sourceColDictEncoded->integers()[i]+1][target_column->strings()[i]] = map[sourceColDictEncoded->integers()[i]+1].size();
                            dict_build_helper[sourceColDictEncoded->integers()[i]+1].push_back(target_column->strings()[i]);
                            mapped_target_string_size += target_column->strings()[i].length();
                        }
                        target_buffer_writer[i] = map[sourceColDictEncoded->integers()[i]+1][target_column->strings()[i]];
                    }
                    else if(sourceColDictEncoded->nullmap[i] == 0){
                        if(map[0].count(target_column->strings()[i])==0){
                            map[0][target_column->strings()[i]] = map[0].size();
                            dict_build_helper[0].push_back(target_column->strings()[i]);
                            mapped_target_string_size += target_column->strings()[i].length();
                        }
                        target_buffer_writer[i] = map[0][target_column->strings()[i]];
                    }
                }
            }

            size_t total_num_children = 0;
            for(auto& parent: map){
                total_num_children += parent.second.size();
            }
            
            // offset buffer
            offset_count = sourceBBSchemes->get_unique_count() + 1; // +1 for null
            offset_size = offset_count * sizeof(btrblocks::units::INTEGER); 
            offset_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[offset_size]()); 
            auto offset_buffer_writer = reinterpret_cast<btrblocks::units::INTEGER*>(offset_buffer.get());
            offset_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[offset_count]());
            
            // fill offsets
            offset_buffer_writer[0] = 0;
            offset_nullmap_buffer[0] = 1;
            for(size_t i=1; i<offset_count; i++){
                offset_buffer_writer[i] = offset_buffer_writer[i-1] + map[i-1].size();
                offset_nullmap_buffer[i] = 1;
            }
            
            // dictionary buffer
            dict_entries = total_num_children;
            dict_size = (dict_entries + 1) * sizeof(btrblocks::StringArrayViewer::Slot) + mapped_target_string_size;
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_size]()); 
            auto dict_slot_writer = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(dict_buffer.get());
            dict_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_entries]());

            // loop through map, fill in dictionary buffer (all values should be filled)
            // for strings, need to fill dict in sequentially from start to end
            dict_slot_writer[0].offset = (dict_entries + 1) * sizeof(btrblocks::StringArrayViewer::Slot);
                    
            for(size_t i=0; i<dict_entries; i++){
                dict_nullmap_buffer[i] = 1;
            }
            for(size_t source_chunk=0; source_chunk<offset_count; source_chunk++){
                int target_count = dict_build_helper[source_chunk].size(); // if i not in dict helper, then target_count should be 0
                // if(source_chunk < offset_count-1){
                //     assert(target_count == offset_buffer_writer[source_chunk+1]-offset_buffer_writer[source_chunk]);
                // } 
                for(size_t target_idx=0; target_idx<target_count; target_idx++){
                    std::string_view target_val = dict_build_helper[source_chunk][target_idx];
                    int dict_idx = offset_buffer_writer[source_chunk] + target_idx;
                    dict_slot_writer[dict_idx+1].offset = dict_slot_writer[dict_idx].offset + target_val.length();
                    memcpy(dict_buffer.get() + dict_slot_writer[dict_idx].offset, target_val.begin(), target_val.length());
                }
            }

            auto basic_stats_target_dict = std::make_shared<btrblocks::StringStats>(btrblocks::StringStats::generateStatsBasic(dict_entries, dict_size)); // int uncompressed
            col_stats_target_dict = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::STRING, basic_stats_target_dict, {}));

            break;
        }
    }
    
    auto offsets_meta = reinterpret_cast<DictMeta*>(dict_1toN_meta->data);

    // compress offsets
    auto basic_stats_offsets = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(offset_count)); // int uncompressed
    auto col_stats_offsets = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_offsets, {}));
    
    // 255 = autoscheme()
    if(config.DICTIONARY_COMPRESSION_SCHEME == 255){
        col_stats_offsets = nullptr;
    }
    
    auto uncompressed_offsets = btrblocks::InputChunk(std::move(offset_buffer), std::move(offset_nullmap_buffer), btrblocks::ColumnType::INTEGER, offset_count, offset_size);
    auto compressed_offsets_size = btrblocks::Datablock::compress(uncompressed_offsets, offsets_meta->data, static_cast<uint8_t>(config.DICTIONARY_COMPRESSION_SCHEME), nullptr, nullptr, col_stats_offsets);
    
    offsets_meta->type = btrblocks::ColumnType::INTEGER;
    dict_1toN_meta->targetDictOffset = sizeof(DictMeta) + compressed_offsets_size;
    auto target_dict_meta = reinterpret_cast<DictMeta*>(dict_1toN_meta->data + dict_1toN_meta->targetDictOffset);

    // compress target dict
    // 255 = autoscheme()
    if(config.DICTIONARY_COMPRESSION_SCHEME == 255){
        col_stats_target_dict = nullptr;
    }

    auto uncompressed_dict = btrblocks::InputChunk(std::move(dict_buffer), std::move(dict_nullmap_buffer), target_column->type, dict_entries, dict_size);
    auto compressed_dict_size = btrblocks::Datablock::compress(uncompressed_dict, target_dict_meta->data, static_cast<uint8_t>(config.DICTIONARY_COMPRESSION_SCHEME), nullptr, nullptr, col_stats_target_dict);
    
    target_dict_meta->type = target_column->type;
    targetChunk->btrblocks_ColumnChunkMeta_offset = sizeof(Dict1toNMeta) + 2 * sizeof(DictMeta) + compressed_offsets_size + compressed_dict_size;

    // compress target column (BP)
    auto basic_stats_target = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(target_column->tuple_count)); // int BP
    auto col_stats_target = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_target, {}));
    
    // 255 = autoscheme()
    if(config.DICTIONARY_CODES_COMPRESSION_SCHEME == 255){
        col_stats_target = nullptr;
    }
    
    auto uncompressed_target_codes = btrblocks::InputChunk(std::move(target_buffer), std::move(target_nullmap_buffer), btrblocks::ColumnType::INTEGER, target_column->tuple_count, target_column->tuple_count * sizeof(btrblocks::units::INTEGER));
    auto compressed_target_codes_size = btrblocks::Datablock::compress(uncompressed_target_codes, targetChunk->data + targetChunk->btrblocks_ColumnChunkMeta_offset, static_cast<uint8_t>(config.DICTIONARY_CODES_COMPRESSION_SCHEME), nullptr, &scheme->target_nullmap_size, col_stats_target);

    target_output.resize(sizeof(C3Chunk) + targetChunk->btrblocks_ColumnChunkMeta_offset + compressed_target_codes_size);

    // If worse than BB ECR, then cancel compression and don't use C3
    // subtract target_nullmap_size, because BB scheme ECR also doesn't include nullmap
    if(config.REVERT_BB_IF_C3_BAD && (target_output.size() - scheme->target_nullmap_size) - (target_column->size / bbSchemes[target_idx]->get_best_scheme().second) > config.BYTES_SAVED_MARGIN * bbSchemes[target_idx]->get_original_chunk_size()){
        // cancel scheme compression
        if(skip_source_encoding && last_source_column_scheme){
            // source column is needed by a previous scheme, compress and return
            auto basic_stats_source = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(source_column->tuple_count));// int BP
            auto col_stats_source = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_source, {}));
            
            // 255 = autoscheme()
            if(config.DICTIONARY_CODES_COMPRESSION_SCHEME == 255){
                col_stats_source = nullptr;
            }

            C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(sourceChunkVec->data());
            auto compressed_values_size = btrblocks::Datablock::compress(*sourceColDictEncoded, sourceChunk->data + sourceChunk->btrblocks_ColumnChunkMeta_offset, static_cast<uint8_t>(config.DICTIONARY_CODES_COMPRESSION_SCHEME), &scheme->source_bb_scheme, &scheme->source_nullmap_size, col_stats_source);
            sourceChunkVec->resize(sizeof(C3Chunk) + sourceChunk->btrblocks_ColumnChunkMeta_offset + compressed_values_size);
            return {std::move(*sourceChunkVec), {}};
        }
        return {{},{}};
    }
    else{

        source_column = sourceColDictEncoded;

        scheme->real_source_target_dict_size = compressed_dict_size;
        scheme->source_target_dict_compression_ratio = 1.0 * dict_size / compressed_dict_size;
        scheme->real_target_compressed_codes_size = compressed_target_codes_size;
        scheme->real_offsets_size = compressed_offsets_size;

        if(last_source_column_scheme){
            // for last apply_scheme compress source col
            // store encoded source and dict together
            auto basic_stats_source = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(source_column->tuple_count));// int BP
            auto col_stats_source = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_source, {}));
            
            // 255 = autoscheme()
            if(config.DICTIONARY_CODES_COMPRESSION_SCHEME == 255){
                col_stats_source = nullptr;
            }

            C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(sourceChunkVec->data());
            auto compressed_values_size = btrblocks::Datablock::compress(*sourceColDictEncoded, sourceChunk->data + sourceChunk->btrblocks_ColumnChunkMeta_offset, static_cast<uint8_t>(config.DICTIONARY_CODES_COMPRESSION_SCHEME), &scheme->source_bb_scheme, &scheme->source_nullmap_size, col_stats_source);
            sourceChunkVec->resize(sizeof(C3Chunk) + sourceChunk->btrblocks_ColumnChunkMeta_offset + compressed_values_size);

            return {std::move(*sourceChunkVec), std::move(target_output)};
        }
        
        return {{}, std::move(target_output)};
    }

}

// decompress target
// bitunpack source and target
// bitshift source, & with target, create indexes
// use indexes to decode with d2, recreating original target
std::vector<uint8_t> Dictionary_1toN::decompress(const std::vector<uint8_t>& source_column, const std::vector<uint8_t>& target_column, C3Chunk* c3_meta, const std::vector<uint8_t>& source_nullmap, const std::vector<uint8_t>& target_nullmap, uint32_t tuple_count, bool& target_requires_copy){
    // here, source and target are already bit unpacked
    
    auto dict_1toN_meta = reinterpret_cast<Dict1toNMeta*>(c3_meta->data);
    auto source_codes_reader = reinterpret_cast<const btrblocks::units::INTEGER*>(source_column.data());
    auto target_codes_reader = reinterpret_cast<const btrblocks::units::INTEGER*>(target_column.data());

    // decompress offsets
    auto offsets_meta = reinterpret_cast<DictMeta*>(dict_1toN_meta->data);
    std::vector<uint8_t> offsets_decompressed_values;
    std::vector<uint8_t> offsets_decompressed_bitmap;
    ChunkDecompression::bb_decompressColumn(offsets_decompressed_values, offsets_meta->data, offsets_decompressed_bitmap);

    // decompress target dict
    auto target_dict_meta = reinterpret_cast<DictMeta*>(dict_1toN_meta->data + dict_1toN_meta->targetDictOffset);
    std::vector<uint8_t> dict_decompressed_values;
    std::vector<uint8_t> dict_decompressed_bitmap;
    target_requires_copy = ChunkDecompression::bb_decompressColumn(dict_decompressed_values, target_dict_meta->data, dict_decompressed_bitmap);

    std::vector<uint8_t> output;
    switch(c3_meta->type){
        case btrblocks::ColumnType::INTEGER:{
            // target col
            output.resize(tuple_count * sizeof(btrblocks::INTEGER));
            auto target_writer = reinterpret_cast<btrblocks::units::INTEGER*>(output.data());
            auto dict_reader = reinterpret_cast<const btrblocks::units::INTEGER*>(dict_decompressed_values.data());
            auto offset_reader = reinterpret_cast<const btrblocks::units::INTEGER*>(offsets_decompressed_values.data());

            for(size_t i=0; i<tuple_count; i++){
                if(source_nullmap[i] == 0){
                    // assert(offset_reader[0] == 0); // offset is 0
                    uint32_t idx = target_codes_reader[i];
                    target_writer[i] = dict_reader[idx];
                }
                else if(source_nullmap[i] == 1){
                    uint32_t dict_offset = offset_reader[source_codes_reader[i]+1];
                    uint32_t idx = dict_offset + target_codes_reader[i];
                    target_writer[i] = dict_reader[idx];
                }
            }
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
            // target col
            output.resize(tuple_count * sizeof(btrblocks::DOUBLE));
            auto target_writer = reinterpret_cast<btrblocks::units::DOUBLE*>(output.data());
            auto dict_reader = reinterpret_cast<const btrblocks::units::DOUBLE*>(dict_decompressed_values.data());
            auto offset_reader = reinterpret_cast<const btrblocks::units::INTEGER*>(offsets_decompressed_values.data());
        
            for(size_t i=0; i<tuple_count; i++){
                if(source_nullmap[i] == 0){
                    // assert(offset_reader[0] == 0); // offset is 0
                    uint32_t idx = target_codes_reader[i];
                    target_writer[i] = dict_reader[idx];
                }
                else if(source_nullmap[i] == 1){
                    uint32_t dict_offset = offset_reader[source_codes_reader[i]+1];
                    uint32_t idx = dict_offset + target_codes_reader[i];
                    target_writer[i] = dict_reader[idx];
                }
            }
            break;
        }
        case btrblocks::ColumnType::STRING:{
            // we have target col with ints and dict with strings
            // recreate target string col
            // two cases: are dict strings compressed with StringArrayViewer or StringArrayPointerViewer?
    
            if(target_requires_copy){

                // copy strings to end of col
                // update offsets and lengths of all views
                auto offset_reader = reinterpret_cast<const btrblocks::units::INTEGER*>(offsets_decompressed_values.data());

                size_t dict_num_tuples = dict_decompressed_bitmap.size();
                size_t dict_views_size = dict_num_tuples * sizeof(btrblocks::StringPointerArrayViewer::View);
                size_t target_views_size = tuple_count * sizeof(btrblocks::StringPointerArrayViewer::View);
                output.resize(target_views_size + dict_decompressed_values.size() - dict_views_size);

                memcpy(output.data() + target_views_size, dict_decompressed_values.data() + dict_views_size, dict_decompressed_values.size() - dict_views_size);

                auto dict_string_views = reinterpret_cast<btrblocks::StringPointerArrayViewer::View*>(dict_decompressed_values.data());
                auto target_views = reinterpret_cast<btrblocks::StringPointerArrayViewer::View*>(output.data());

                for(size_t i=0; i<tuple_count; i++){
                    if(source_nullmap[i] == 0){
                        // assert(offset_reader[0] == 0); // offset is 0
                        uint32_t idx = target_codes_reader[i];
                        target_views[i].length = target_views_size + dict_string_views[idx].offset - dict_views_size;
                        target_views[i].offset = dict_string_views[idx].length;
                    }
                    else{
                        uint32_t dict_offset = offset_reader[source_codes_reader[i]+1];
                        uint32_t idx = dict_offset + target_codes_reader[i];
                        target_views[i].length = target_views_size + dict_string_views[idx].offset - dict_views_size;
                        target_views[i].offset = dict_string_views[idx].length;
                    }
                }
            }
            else{
                output.resize(c3_meta->original_col_size * 2);

                auto dict_string_viewer = btrblocks::StringArrayViewer(dict_decompressed_values.data());
                auto offset_reader = reinterpret_cast<const btrblocks::units::INTEGER*>(offsets_decompressed_values.data());
                auto slots = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(output.data());
                slots[0].offset = sizeof(btrblocks::StringArrayViewer::Slot) * (tuple_count + 1);

                for(size_t i=0; i<tuple_count; i++){
                    if(target_nullmap[i] == 1){
                        if(source_nullmap[i] == 0){
                            // assert(offset_reader[0] == 0); // offset is 0
                            uint32_t idx = target_codes_reader[i];
                            std::string_view val = dict_string_viewer(idx);
                            slots[i+1].offset = slots[i].offset + val.size();
                            std::memcpy(output.data() + slots[i].offset, val.begin(), val.size());
                        }
                        else{
                            uint32_t dict_offset = offset_reader[source_codes_reader[i]+1];
                            uint32_t idx = dict_offset + target_codes_reader[i];
                            std::string_view val = dict_string_viewer(idx);
                            slots[i+1].offset = slots[i].offset + val.size();
                            std::memcpy(output.data() + slots[i].offset, val.begin(), val.size());
                        }
                    }
                    else{
                        slots[i+1].offset = slots[i].offset;
                    }
                }

                output.resize(slots[tuple_count].offset);// + sizeof(btrblocks::StringArrayViewer::Slot));
            }
            break;
        }
    }


    return output;
}

}
}