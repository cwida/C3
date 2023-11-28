#include "dfor.hpp"
#include "c3/Utils.hpp"
#include "c3/schemes/single_col/dictionary.hpp"
#include "compression/Datablock.hpp"

#include <algorithm>
#include <limits.h>

namespace c3{
namespace multi_col{

bool DFOR::skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j){
    
    // target must be integer
    if(row_group->columns[j]->type != btrblocks::ColumnType::INTEGER){
        return true;
    }
    
    if(config.USE_PRUNING_RULES){
        double unique_count_threshold = row_group->columns[i]->tuple_count * 0.1;
        if(btrBlocksSchemes[i]->get_unique_count() > unique_count_threshold){                    
            return true;
        }
    }

    // // only want target columns with large range
    // auto range = btrBlocksSchemes[j].intStats->max - btrBlocksSchemes[j].intStats->min;
    // if(range < 100){
    //     return true;
    // }
    return false;
}

double DFOR::expectedCompressionRatio(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::vector<int>& samples, std::shared_ptr<ColumnStats> sourceStats, std::shared_ptr<ColumnStats> targetStats, int& estimated_target_compressed_codes_size, int& estimated_source_target_dict_size){
    // 1. build dictionary on source
    // 2. for each dict value, find min max, compute bits needed for range
        // have a separate for range for null source values 

    assert(target_column->type==btrblocks::ColumnType::INTEGER);

    double estimated_compression_ratio;

    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
            std::map<int,int> map; // map to min max target val
            int null_min = INT_MAX; // min max of null source values
            for(auto sample: samples){
                if(source_column->nullmap[sample] == 1 && target_column->nullmap[sample] == 1){
                    if(map.find(source_column->integers()[sample]) == map.end()){
                        map[source_column->integers()[sample]] = target_column->integers()[sample];
                    }
                    else{
                        map[source_column->integers()[sample]] = std::min(target_column->integers()[sample], map[source_column->integers()[sample]]);
                    }
                }
                else if(source_column->nullmap[sample] == 0 && target_column->nullmap[sample] == 1){
                    null_min = std::min(target_column->integers()[sample], null_min);
                }
            }

            if(map.empty()){
                return 1;
            }

            std::vector<int> sample_encoded_target;
            for(auto sample: samples){
                if(target_column->nullmap[sample] == 1){
                    if(source_column->nullmap[sample] == 1){
                        sample_encoded_target.push_back(target_column->integers()[sample] - map[source_column->integers()[sample]]);
                    }
                    else{
                        sample_encoded_target.push_back(target_column->integers()[sample] - null_min);
                    }
                }
                else{
                    sample_encoded_target.push_back(0);
                }
            }
            estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, samples);
            estimated_source_target_dict_size = sizeof(DictMeta) + sourceStats->intStats->unique_count * sizeof(int) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;

            auto compressed_size = estimated_target_compressed_codes_size + estimated_source_target_dict_size;
            estimated_compression_ratio = 1.0 * target_column->size / compressed_size;
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
            std::map<double,int> map; // map to min max target val
            int null_min = INT_MAX; // min max of null source values
            for(auto sample: samples){
                if(source_column->nullmap[sample] == 1 && target_column->nullmap[sample] == 1){
                    if(map.find(source_column->doubles()[sample]) == map.end()){
                        map[source_column->doubles()[sample]] = target_column->integers()[sample];
                    }
                    else{
                        map[source_column->doubles()[sample]] = std::min(target_column->integers()[sample], map[source_column->doubles()[sample]]);
                    }
                }
                else if(source_column->nullmap[sample] == 0 && target_column->nullmap[sample] == 1){
                    null_min = std::min(target_column->integers()[sample], null_min);
                }
            }

            if(map.empty()){
                return 1;
            }

            std::vector<int> sample_encoded_target;
            for(auto sample: samples){
                if(target_column->nullmap[sample] == 1){
                    if(source_column->nullmap[sample] == 1){
                        sample_encoded_target.push_back(target_column->integers()[sample] - map[source_column->doubles()[sample]]);
                    }
                    else{
                        sample_encoded_target.push_back(target_column->integers()[sample] - null_min);
                    }
                }
                else{
                    sample_encoded_target.push_back(0);
                }
            }
            estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, samples);
            estimated_source_target_dict_size = sizeof(DictMeta) + sourceStats->doubleStats->unique_count * sizeof(int)/ config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;

            auto compressed_size = estimated_target_compressed_codes_size + estimated_source_target_dict_size;
            estimated_compression_ratio = 1.0 * target_column->size / compressed_size;
            break;
        }
        case btrblocks::ColumnType::STRING:{
            std::map<std::string_view,int> map; // map to min max target val
            int null_min = INT_MAX; // min max of null source values
            for(auto sample: samples){
                if(source_column->nullmap[sample] == 1 && target_column->nullmap[sample] == 1){
                    if(map.find(source_column->strings()[sample]) == map.end()){
                        map[source_column->strings()[sample]] = target_column->integers()[sample];
                    }
                    else{
                        map[source_column->strings()[sample]] = std::min(target_column->integers()[sample], map[source_column->strings()[sample]]);
                    }
                }
                else if(source_column->nullmap[sample] == 0 && target_column->nullmap[sample] == 1){
                    null_min = std::min(target_column->integers()[sample], null_min);
                }
            }

            if(map.empty()){
                return 1;
            }

            std::vector<int> sample_encoded_target;
            for(auto sample: samples){
                if(target_column->nullmap[sample] == 1){
                    if(source_column->nullmap[sample] == 1){
                        sample_encoded_target.push_back(target_column->integers()[sample] - map[source_column->strings()[sample]]);
                    }
                    else{
                        sample_encoded_target.push_back(target_column->integers()[sample] - null_min);
                    }
                }
                else{
                    sample_encoded_target.push_back(0);
                }
            }
            estimated_target_compressed_codes_size = Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, samples);
            estimated_source_target_dict_size = sizeof(DictMeta) + sourceStats->stringStats->unique_count * sizeof(int) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;

            auto compressed_size = estimated_target_compressed_codes_size + estimated_source_target_dict_size;
            estimated_compression_ratio = 1.0 * target_column->size / compressed_size;
            break;
        }
    }
    return estimated_compression_ratio;
}

std::vector<std::vector<uint8_t>> DFOR::apply_scheme(std::shared_ptr<btrblocks::InputChunk>& source_column, std::shared_ptr<btrblocks::InputChunk> target_column, int source_idx, int target_idx, bool skip_source_encoding, bool last_source_column_scheme, std::shared_ptr<DForCompressionScheme> scheme, int source_unique_count, std::vector<uint8_t>* sourceChunkVec, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes){

    assert(target_column->type==btrblocks::ColumnType::INTEGER);

    // get copy of target column, in case we need to undo C3
    // data
    auto copied_data = std::unique_ptr<uint8_t[]>(new uint8_t[target_column->size]); 
    memcpy(copied_data.get(), target_column->data.get(), target_column->size);

    // nullmap
    auto copied_nullmap = std::unique_ptr<uint8_t[]>(new uint8_t[target_column->tuple_count]); 
    memcpy(copied_nullmap.get(), target_column->nullmap.get(), target_column->tuple_count);

    auto copied_target_col = std::make_shared<btrblocks::InputChunk>(std::move(copied_data), std::move(copied_nullmap), target_column->type, target_column->tuple_count, target_column->size);

    // int compressed_values_size;

        // 1. encode source col to integer codes
        // 2. create reference dictionary for target col
        // 3. encode target col
        // 4. compress reference dictionary, write to DictMeta
        // 5. compress target col, write to targetChunk->data + targetChunk->btrblocks_ColumnChunkMeta_offset

    std::shared_ptr<btrblocks::InputChunk> sourceColDictEncoded = source_column;
            
    if(!skip_source_encoding){
        assert(sourceChunkVec->size()==0);
        *sourceChunkVec = std::vector<uint8_t>(sizeof(C3Chunk) + 10 * source_column->size);
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
    
    std::vector<uint8_t> target_output(sizeof(C3Chunk) + 10 * copied_target_col->size);    
    C3Chunk* targetChunk = reinterpret_cast<C3Chunk*>(target_output.data());    
    targetChunk->compression_type = static_cast<uint8_t>(SchemeType::DFOR);    
    targetChunk->type = copied_target_col->type;
    targetChunk->source_column_id = source_idx;
    targetChunk->original_col_size = copied_target_col->size;

    auto dfor_meta = reinterpret_cast<DForMeta*>(targetChunk->data);

    auto dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[source_unique_count * sizeof(btrblocks::units::INTEGER)]()); 
    auto dict_buffer_writer = reinterpret_cast<btrblocks::units::INTEGER*>(dict_buffer.get());
    auto nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[source_unique_count]());
    
    // std::map<int,int> map; // map to min target val
    int null_min = INT_MAX; // min of null source values
    for(size_t i=0; i<source_unique_count; i++){
        dict_buffer_writer[i] = INT_MAX;
        nullmap_buffer[i] = 1;
    }

    // go through whole column, build map to smallest value
    for(size_t i=0; i<sourceColDictEncoded->tuple_count; i++){
        if(sourceColDictEncoded->nullmap[i] == 1 && copied_target_col->nullmap[i] == 1){
            // assert(sourceColDictEncoded->integers()[i] < source_unique_count);
            dict_buffer_writer[sourceColDictEncoded->integers()[i]] = std::min(copied_target_col->integers()[i], dict_buffer_writer[sourceColDictEncoded->integers()[i]]);
        }
        else if(sourceColDictEncoded->nullmap[i] == 0 && copied_target_col->nullmap[i] == 1){
            null_min = std::min(copied_target_col->integers()[i], null_min);
        }
    }

    // compute new int target column
    for(size_t i=0; i<sourceColDictEncoded->tuple_count; i++){
        if(copied_target_col->nullmap[i] == 1){
            if(sourceColDictEncoded->nullmap[i]==0){
                copied_target_col->integers()[i] -= null_min;
            }
            else{
                copied_target_col->integers()[i] -= dict_buffer_writer[sourceColDictEncoded->integers()[i]];
            }
            // assert(copied_target_col->integers()[i] >= 0);
        }
    }
    
    // compress dict
    auto basic_stats_dict = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(source_unique_count));// int BP
    auto dict_col_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_dict, {}));
    
    // 255 = autoscheme()
    if(config.DICTIONARY_COMPRESSION_SCHEME == 255){
        dict_col_stats = nullptr;
    }
    
    auto uncompressed_dict = btrblocks::InputChunk(std::move(dict_buffer), std::move(nullmap_buffer), btrblocks::ColumnType::INTEGER, source_unique_count, source_unique_count * sizeof(btrblocks::units::INTEGER));
    auto compressed_dict_size = btrblocks::Datablock::compress(uncompressed_dict, dfor_meta->data, static_cast<uint8_t>(config.DICTIONARY_COMPRESSION_SCHEME), nullptr, nullptr, dict_col_stats);
    dfor_meta->null_reference = null_min;

    // compress target column
    auto basic_stats_target = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(target_column->tuple_count));// int BP
    auto target_col_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_target, {}));
    
    // 255 = autoscheme()
    if(config.DFOR_CODES_COMPRESSION_SCHEME == 255){
        target_col_stats = nullptr;
    }

    targetChunk->btrblocks_ColumnChunkMeta_offset = sizeof(DForMeta) + compressed_dict_size;
    size_t compressed_target_size = btrblocks::Datablock::compress(*copied_target_col, targetChunk->data + targetChunk->btrblocks_ColumnChunkMeta_offset, static_cast<uint8_t>(config.DFOR_CODES_COMPRESSION_SCHEME), &scheme->target_bb_scheme, &scheme->target_nullmap_size, target_col_stats);
    target_output.resize(sizeof(C3Chunk) + targetChunk->btrblocks_ColumnChunkMeta_offset + compressed_target_size);

    // If worse than BB ECR, then cancel compression and don't use C3
    // subtract target_nullmap_size, because BB scheme ECR also doesn't include nullmap
    if(config.REVERT_BB_IF_C3_BAD && (target_output.size() - scheme->target_nullmap_size) - (copied_target_col->size / bbSchemes[target_idx]->get_best_scheme().second) > config.BYTES_SAVED_MARGIN * bbSchemes[target_idx]->get_original_chunk_size()){
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
        scheme->real_target_compressed_codes_size = compressed_target_size;
        scheme->real_source_target_dict_size = compressed_dict_size;

        // compress source column
        if(last_source_column_scheme){

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

std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> DFOR::decompress(const std::vector<uint8_t>& source_column, C3Chunk* c3_meta, const std::vector<uint8_t>& source_nullmap, uint32_t tuple_count, bool& requires_copy){
    
    // decompress target col
    std::vector<uint8_t> target_values;
    std::vector<uint8_t> target_nullmap;
    requires_copy = ChunkDecompression::bb_decompressColumn(target_values, c3_meta->data + c3_meta->btrblocks_ColumnChunkMeta_offset, target_nullmap);

    // decompress reference dictionary
    auto dfor_meta = reinterpret_cast<DForMeta*>(c3_meta->data);
    std::vector<uint8_t> dict_values;
    std::vector<uint8_t> dict_nullmap;
    ChunkDecompression::bb_decompressColumn(dict_values, dfor_meta->data, dict_nullmap);

    // replace target values
    auto target_writer = reinterpret_cast<btrblocks::INTEGER*>(target_values.data());
    auto dict_reader = reinterpret_cast<btrblocks::INTEGER*>(dict_values.data());
    auto source_reader = reinterpret_cast<const btrblocks::INTEGER*>(source_column.data());
    for(size_t i=0; i<tuple_count; i++){
        if(source_nullmap[i] == 1){
            target_writer[i] += dict_reader[source_reader[i]];
        }
        else{
            target_writer[i] += dfor_meta->null_reference;
        }
    }

    return {std::move(target_values), std::move(target_nullmap)};
}

}
}
