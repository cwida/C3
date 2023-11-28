#include "dict_sharing.hpp"
#include "c3/Utils.hpp"
#include "c3/schemes/single_col/dictionary.hpp"
#include "compression/Datablock.hpp"

#include <algorithm>
#include <limits.h>

namespace c3{
namespace multi_col{

bool DictSharing::skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j){
    
    // need same data type
    if(row_group->columns[i]->type != row_group->columns[j]->type){
        return true;
    }
    
    if(config.USE_PRUNING_RULES){
        double unique_count_threshold = row_group->columns[i]->tuple_count * 0.25;
        if(btrBlocksSchemes[i]->get_unique_count() > unique_count_threshold){                    
            return true;
        }
        if(btrBlocksSchemes[j]->get_unique_count() > unique_count_threshold){                                
            return true;
        }
    }
    
    return false;
}

// ECR = dict ECR source + dict ECR target - bytes_saved_from_dict_sharing
// return source ECR = dict ECR source + extra  
std::pair<double,double> DictSharing::expectedCompressionRatio(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::vector<int>& samples, std::shared_ptr<ColumnStats> sourceStats, std::shared_ptr<ColumnStats> targetStats, int& estimated_source_target_dict_size, int& estimated_target_compressed_codes_size){

    assert(target_column->type==source_column->type);

    int compressed_source_size = source_column->size / sourceStats->get_dict_compression_ratio();
    int compressed_target_size;
    
    int sample_size = samples.size(); // only check as many target values as sample size

    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
            
            int additional_bytes = 0;
            int sample_counter = 0;
            auto shared_map = sourceStats->intStats->distinct_values;
            for(auto const& pair: targetStats->intStats->distinct_values){
                if(shared_map.count(pair.first)==0){
                    shared_map[pair.first] = {shared_map.size(), pair.second.second};
                    additional_bytes += sizeof(btrblocks::units::INTEGER);
                }
                if(sample_counter++ > sample_size){
                    break;
                }
            }
            additional_bytes *= 1.0 * (targetStats->get_unique_count() / std::max(sample_counter,1)); // extrapolate to all target vals

            std::vector<int> sample_encoded_target;
            for(auto sample: samples){
                if(shared_map.count(target_column->integers()[sample])==0){
                    shared_map[target_column->integers()[sample]] = {shared_map.size(), 0};
                }
                sample_encoded_target.push_back(shared_map[target_column->integers()[sample]].first);
            }
            compressed_target_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, samples);
            estimated_target_compressed_codes_size = compressed_target_size;
            // compressed_target_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(sourceStats->get_unique_count()+additional_values_counter)) + 1) / 8; // encoded target size;

            estimated_source_target_dict_size = sourceStats->get_unique_count() * sizeof(btrblocks::units::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
            estimated_source_target_dict_size += additional_bytes / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
            compressed_source_size += additional_bytes;
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
            
            int additional_bytes = 0;
            int sample_counter = 0;
            auto shared_map = sourceStats->doubleStats->distinct_values;
            for(auto const& pair: targetStats->doubleStats->distinct_values){
                if(shared_map.count(pair.first)==0){
                    shared_map[pair.first] = {shared_map.size(), 0};
                    additional_bytes += sizeof(btrblocks::units::DOUBLE);
                }
                if(sample_counter++ > sample_size){
                    break;
                }
            }
            additional_bytes *= 1.0 * targetStats->get_unique_count() / std::max(sample_counter,1); // extrapolate to all target vals

            std::vector<int> sample_encoded_target;
            for(auto sample: samples){
                if(shared_map.count(target_column->doubles()[sample])==0){
                    shared_map[target_column->doubles()[sample]] = {shared_map.size(), 0};
                }
                sample_encoded_target.push_back(shared_map[target_column->doubles()[sample]].first);
            }
            compressed_target_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, samples);
            estimated_target_compressed_codes_size = compressed_target_size;
            // compressed_target_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(sourceStats->get_unique_count()+additional_values_counter)) + 1) / 8; // encoded target size;

            estimated_source_target_dict_size =  sourceStats->get_unique_count() * sizeof(btrblocks::units::DOUBLE) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
            estimated_source_target_dict_size += additional_bytes / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
            compressed_source_size += additional_bytes;
            break;
        }
        case btrblocks::ColumnType::STRING:{
            
            int additional_bytes = 0;
            int sample_counter = 0;
            auto shared_map = sourceStats->stringStats->distinct_values_counter;
            for(auto const& pair: targetStats->stringStats->distinct_values_counter){
                if(shared_map.count(pair.first)==0){
                    shared_map[pair.first] = shared_map.size();
                    additional_bytes += pair.first.size() + sizeof(btrblocks::StringArrayViewer::Slot) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                }
                if(sample_counter++ > sample_size){
                    break;
                }
            }
            additional_bytes *= 1.0 * (targetStats->get_unique_count() / std::max(sample_counter,1)); // extrapolate to all target vals
            
            std::vector<int> sample_encoded_target;
            for(auto sample: samples){
                if(shared_map.count(target_column->strings()[sample])==0){
                    shared_map[target_column->strings()[sample]] = shared_map.size();
                }
                sample_encoded_target.push_back(shared_map[target_column->strings()[sample]]);
            }
            compressed_target_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, samples);
            estimated_target_compressed_codes_size = compressed_target_size;
            // compressed_target_size = 1.0 * target_column->tuple_count * (std::floor(std::log2(sourceStats->get_unique_count()+additional_values_counter)) + 1) / 8; // encoded target size;

            estimated_source_target_dict_size = (sourceStats->get_unique_count() + 1) * sizeof(btrblocks::StringArrayViewer::Slot) + sourceStats->stringStats->total_unique_length / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
            estimated_source_target_dict_size += additional_bytes;
            compressed_source_size += additional_bytes;
            break;
        }
    }

    double source_compression_ratio = 1.0 * source_column->size / compressed_source_size;
    double target_compression_ratio = 1.0 * target_column->size / compressed_target_size;

    return {source_compression_ratio, target_compression_ratio};
}

std::vector<std::vector<uint8_t>> DictSharing::apply_scheme(std::shared_ptr<btrblocks::InputChunk>& source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::shared_ptr<DictSharingCompressionScheme> scheme, int source_idx, int target_idx, bool skip_source_encoding, bool last_source_column_scheme, std::vector<uint8_t>* sourceChunkVec, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes){
    // shared dict should first assign codes to source vals, then to target vals
        // since source codes will be used to access dict vals in other schemes, need to make sure max source code < unique_source_val_count

    // 0. ! must always be first scheme using source
    // 1. create dict buffer
        // switch col type, create shared dict by computing union of sets
        // init union with source dict, then add target dict to it
    // 2. encode target col
        // pass union dict to single col dict compress with union dict and BP compress
    // 3. encode source col
        // pass union dict to single col dict compress
        // store union dict in source chunk
    // 4. if last scheme using source: BP compress source codes

    assert(source_column->type == target_column->type);
    assert(!skip_source_encoding);
    int dict_size;

    std::shared_ptr<SharedDict> shared_dict;
    std::unique_ptr<uint8_t[]> dict_buffer;
    std::unique_ptr<uint8_t[]> dict_nullmap_buffer;
    int source_dict_extra_bytes = 0;

    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
            // 1.
            shared_dict = std::make_shared<SharedDict>(bbSchemes[source_idx]->intStats->distinct_values);
            auto code_counter = bbSchemes[source_idx]->intStats->unique_count;
            assert(code_counter == shared_dict->intMap.size());
            for(const auto& pair : bbSchemes[target_idx]->intStats->distinct_values){
                if(shared_dict->intMap.count(pair.first)==0){
                    shared_dict->intMap[pair.first] = {code_counter++, pair.second.second};
                }
            }

            source_dict_extra_bytes = (shared_dict->intMap.size() - bbSchemes[source_idx]->intStats->unique_count) * sizeof(btrblocks::INTEGER);
            dict_size = shared_dict->intMap.size() * sizeof(btrblocks::INTEGER);
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_size]());
            dict_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[shared_dict->intMap.size()]());
            auto dict_buffer_writer = reinterpret_cast<btrblocks::INTEGER*>(dict_buffer.get());            
            for(const auto& [val, code] : shared_dict->intMap){
                dict_buffer_writer[code.first] = val;
                dict_nullmap_buffer[code.first] = 1;
            }
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
            // 1.
            shared_dict = std::make_shared<SharedDict>(bbSchemes[source_idx]->doubleStats->distinct_values);
            auto code_counter = bbSchemes[source_idx]->doubleStats->unique_count;
            assert(code_counter == shared_dict->doubleMap.size());
            for(const auto& pair : bbSchemes[target_idx]->doubleStats->distinct_values){
                if(shared_dict->doubleMap.count(pair.first)==0){
                    shared_dict->doubleMap[pair.first] = {code_counter++, pair.second.second};
                }
            }

            source_dict_extra_bytes = (shared_dict->doubleMap.size() - bbSchemes[source_idx]->doubleStats->unique_count) * sizeof(btrblocks::DOUBLE);
            dict_size = shared_dict->doubleMap.size() * sizeof(btrblocks::DOUBLE);
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_size]());
            dict_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[shared_dict->doubleMap.size()]());
            auto dict_buffer_writer = reinterpret_cast<btrblocks::DOUBLE*>(dict_buffer.get());            
            for(const auto& [val, code] : shared_dict->doubleMap){
                dict_buffer_writer[code.first] = val;
                dict_nullmap_buffer[code.first] = 1;
            }
            break;
        }
        case btrblocks::ColumnType::STRING:{
            // 1.
            int distinct_strings_size = bbSchemes[source_idx]->stringStats->total_unique_length;
            shared_dict = std::make_shared<SharedDict>(bbSchemes[source_idx]->stringStats->distinct_values_counter);
            auto code_counter = bbSchemes[source_idx]->stringStats->unique_count;
            assert(code_counter == shared_dict->stringMap.size());
            for(const auto& pair : bbSchemes[target_idx]->stringStats->distinct_values_counter){
                if(shared_dict->stringMap.count(pair.first)==0){
                    shared_dict->stringMap[pair.first] = code_counter++;
                    distinct_strings_size += pair.first.size();
                    source_dict_extra_bytes += pair.first.size() + sizeof(btrblocks::StringArrayViewer::Slot);
                }
            }

            std::map<int,std::string_view> source_dict_reverse;
            for(const auto& [val, code] : shared_dict->stringMap){
                source_dict_reverse[code] = val;
            }

            assert(code_counter == source_dict_reverse.size());
            assert(code_counter == shared_dict->stringMap.size());

            dict_size = sizeof(btrblocks::StringArrayViewer::Slot) * (shared_dict->stringMap.size() + 1) + distinct_strings_size;
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_size]());
            dict_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[shared_dict->stringMap.size()]());
            auto string_slots = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(dict_buffer.get());            
            string_slots[0].offset = sizeof(btrblocks::StringArrayViewer::Slot) * (code_counter + 1);
            for(size_t code_i=0; code_i<source_dict_reverse.size(); code_i++){
                dict_nullmap_buffer[code_i] = 1;

                auto dest = dict_buffer.get() + string_slots[code_i].offset;
                memcpy(dest, source_dict_reverse[code_i].begin(), source_dict_reverse[code_i].length());
                string_slots[code_i+1].offset = string_slots[code_i].offset + source_dict_reverse[code_i].length();
            }
            break;
        }
    }

    // 2.
    std::vector<uint8_t> target_output(sizeof(C3Chunk) + 10 * target_column->size, 0);    
    C3Chunk* targetChunk = reinterpret_cast<C3Chunk*>(target_output.data());    
    targetChunk->compression_type = static_cast<uint8_t>(SchemeType::Dict_Sharing);    
    targetChunk->type = target_column->type;    
    targetChunk->source_column_id = source_idx;
    targetChunk->original_col_size = target_column->size;
    targetChunk->btrblocks_ColumnChunkMeta_offset = 0; // just store compressed col, no other metadata needed

    // compress target column
    size_t dummy_compressed_source_dict_size;
    std::shared_ptr<btrblocks::InputChunk> targetColDictEncoded = single_col::Dictionary::apply_scheme_dict_sharing(target_column, nullptr, shared_dict, nullptr, 0, nullptr, dummy_compressed_source_dict_size);
    auto basic_stats_target = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(target_column->tuple_count)); // int BP
    auto col_stats_target = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_target, {}));
    
    // 255 = autoscheme()
    if(config.DICTIONARY_CODES_COMPRESSION_SCHEME == 255){
        col_stats_target = nullptr;
    }
    
    auto compressed_target_codes_size = btrblocks::Datablock::compress(*targetColDictEncoded, targetChunk->data, static_cast<uint8_t>(config.DICTIONARY_CODES_COMPRESSION_SCHEME), nullptr, &scheme->target_nullmap_size, col_stats_target);
    target_output.resize(sizeof(C3Chunk) + targetChunk->btrblocks_ColumnChunkMeta_offset + compressed_target_codes_size);
    
    // 3. 
    assert(sourceChunkVec->size()==0);
    *sourceChunkVec = std::vector<uint8_t>(sizeof(C3Chunk) + 100 * source_column->size, 0);
    C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(sourceChunkVec->data());
    sourceChunk->compression_type = static_cast<uint8_t>(SchemeType::Single_Dictionary);
    sourceChunk->type = source_column->type;
    sourceChunk->source_column_id = source_idx; // not used, since compression type set to single dict

    sourceChunk->original_col_size = source_column->size;
    auto source_dict = reinterpret_cast<DictMeta*>(sourceChunk->data);
    source_dict->type = source_column->type;
    size_t compressed_source_dict_size;
    auto sourceColDictEncoded = single_col::Dictionary::apply_scheme_dict_sharing(source_column, source_dict, shared_dict, std::move(dict_buffer), dict_size, std::move(dict_nullmap_buffer), compressed_source_dict_size);
    sourceChunk->btrblocks_ColumnChunkMeta_offset = sizeof(DictMeta) + compressed_source_dict_size;
    
    int best_bb_size = (source_column->size / bbSchemes[source_idx]->get_best_scheme().second) + (target_column->size / bbSchemes[target_idx]->get_best_scheme().second);
    int new_c3_size_estimate = (source_column->size / bbSchemes[source_idx]->get_dict_compression_ratio() + source_dict_extra_bytes) + (target_output.size() - scheme->target_nullmap_size);

    // If worse than BB ECR, then cancel compression and don't use C3
    // subtract target_nullmap_size, because BB scheme ECR also doesn't include nullmap
    if(config.REVERT_BB_IF_C3_BAD && (new_c3_size_estimate - best_bb_size) > config.BYTES_SAVED_MARGIN * bbSchemes[target_idx]->get_original_chunk_size()){
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
            auto compressed_values_size = btrblocks::Datablock::compress(*sourceColDictEncoded, sourceChunk->data + sourceChunk->btrblocks_ColumnChunkMeta_offset, static_cast<uint8_t>(config.DICTIONARY_CODES_COMPRESSION_SCHEME), nullptr, &scheme->source_nullmap_size, col_stats_source);
            sourceChunkVec->resize(sizeof(C3Chunk) + sourceChunk->btrblocks_ColumnChunkMeta_offset + compressed_values_size);
            return {std::move(*sourceChunkVec), {}};
        }
        return {{},{}};
    }
    else{
        source_column = sourceColDictEncoded;

        scheme->real_source_target_dict_size = compressed_source_dict_size;
        scheme->real_target_compressed_codes_size = compressed_target_codes_size;

        if(last_source_column_scheme){
            auto basic_stats_source = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(source_column->tuple_count)); // int BP
            auto col_stats_source = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_source, {}));
            
            // 255 = autoscheme()
            if(config.DICTIONARY_CODES_COMPRESSION_SCHEME == 255){
                col_stats_source = nullptr;
            }

            auto compressed_source_codes_size = btrblocks::Datablock::compress(*sourceColDictEncoded, sourceChunk->data + sourceChunk->btrblocks_ColumnChunkMeta_offset, static_cast<uint8_t>(config.DICTIONARY_CODES_COMPRESSION_SCHEME), nullptr, &scheme->source_nullmap_size, col_stats_source);
            sourceChunkVec->resize(sizeof(C3Chunk) + sourceChunk->btrblocks_ColumnChunkMeta_offset + compressed_source_codes_size);
            return {std::move(*sourceChunkVec), std::move(target_output)};
        }

        return {{}, std::move(target_output)};
    }
}

// std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> DictSharing::decompress(const std::vector<uint8_t>& source_column, C3Chunk* c3_meta, const std::vector<uint8_t>& source_nullmap, uint32_t tuple_count, bool& requires_copy){
    
// }

}
}
