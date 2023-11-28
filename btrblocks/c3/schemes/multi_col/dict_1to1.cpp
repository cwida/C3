#include "dict_1to1.hpp"
#include "c3/Utils.hpp"
#include "compression/Datablock.hpp"
#include "c3/schemes/single_col/dictionary.hpp"

#include <algorithm>

namespace c3{
namespace multi_col{

bool Dictionary_1to1::skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j){
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

        // each source can only map to one target, so target_unique_count - source_unique_count is guaranteed minimum number of exceptions
        int guaranteed_exception_count = btrBlocksSchemes[j]->get_unique_count() - btrBlocksSchemes[i]->get_unique_count();
        if(guaranteed_exception_count > row_group->tuple_count * config.DICT_EXCEPTION_RATIO_THRESHOLD){
            return true;
        }
    }

    return false;
}

/* 
    IDEA: STORING EXCEPTIONS in null source value 
    // is only an exception, if source is not null && target is not null && source != target
    // if source is null && target is null, nothing to do
    // if source is null && target is not null, store source id which maps to target in source
    // if source is not null && target is null, do nothing (we keep target nullmap)

    if(source_column->nullmap[i]==1 && target_column->nullmap[i]==1 && source_val != target_val){
        // exception: store target value as exception
    }
    else if(source_column->nullmap[i]==0 && target_column->nullmap[i]==1){
        // not exception: find source id which maps to target val, store this source id in source col
    }

*/

std::pair<double, double> Dictionary_1to1::expectedCompressionRatio(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::vector<int>& samples, std::shared_ptr<ColumnStats> sourceStats, std::shared_ptr<ColumnStats> targetStats, int& estimated_source_target_dict_size, int& estimated_target_dict_size, int& estimated_exception_size){
    // return expected exception and compression ratio of target column (for source col, will use BB stats)

    int exceptions = 0;
    int mapped_count = 0;
    size_t target_size_compressed = sizeof(C3Chunk);

    // calculate number of exceptions
    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
            switch(target_column->type){
                case btrblocks::ColumnType::INTEGER:{

                    std::map<int,std::map<int,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[source_column->integers()[samples[i]]][target_column->integers()[samples[i]]] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each source_column key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }

                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->intStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = sizeof(DictMeta) + targetStats->intStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->intStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
                case btrblocks::ColumnType::DOUBLE:{

                    std::map<int,std::map<double,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[source_column->integers()[samples[i]]][target_column->doubles()[samples[i]]] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each col1 key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }
                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->intStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = sizeof(DictMeta) + targetStats->doubleStats->unique_count * sizeof(btrblocks::DOUBLE) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->intStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
                case btrblocks::ColumnType::STRING:{
                    
                    std::map<int,std::map<std::string,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[source_column->integers()[samples[i]]][std::string(target_column->strings()(samples[i]))] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each col1 key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }
                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->intStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = (sizeof(DictMeta) + targetStats->stringStats->total_unique_length + targetStats->stringStats->unique_count * sizeof(btrblocks::StringArrayViewer::Slot)) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->intStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
                default: std::cout << "data type not supported" << std::endl;
            }
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
             switch(target_column->type){
                case btrblocks::ColumnType::INTEGER:{
                    std::map<double,std::map<int,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[source_column->doubles()[samples[i]]][target_column->integers()[samples[i]]] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each col1 key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }
                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->doubleStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = sizeof(DictMeta) + targetStats->intStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->doubleStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
                case btrblocks::ColumnType::DOUBLE:{
                    
                    std::map<double,std::map<double,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[source_column->doubles()[samples[i]]][target_column->doubles()[samples[i]]] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each col1 key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }
                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->doubleStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = sizeof(DictMeta) + targetStats->doubleStats->unique_count * sizeof(btrblocks::DOUBLE) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->doubleStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
                case btrblocks::ColumnType::STRING:{

                    std::map<double,std::map<std::string,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[source_column->doubles()[samples[i]]][std::string(target_column->strings()(samples[i]))] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each col1 key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }
                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->doubleStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = (sizeof(DictMeta) + targetStats->stringStats->total_unique_length + targetStats->stringStats->unique_count * sizeof(btrblocks::StringArrayViewer::Slot)) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->doubleStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
            }
            break;
        }
        case btrblocks::ColumnType::STRING:{
             switch(target_column->type){
                case btrblocks::ColumnType::INTEGER:{
                    
                    std::map<std::string,std::map<int,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[std::string(source_column->strings()(samples[i]))][target_column->integers()[samples[i]]] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each col1 key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }
                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->stringStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = sizeof(DictMeta) + targetStats->intStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->stringStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
                case btrblocks::ColumnType::DOUBLE:{
                    
                    std::map<std::string,std::map<double,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[std::string(source_column->strings()(samples[i]))][target_column->doubles()[samples[i]]] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each col1 key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }
                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->stringStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = sizeof(DictMeta) + targetStats->doubleStats->unique_count * sizeof(btrblocks::DOUBLE) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->stringStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
                case btrblocks::ColumnType::STRING:{

                    std::map<std::string,std::map<std::string,int>> map;
                    for(size_t i=0; i<samples.size(); i++){
                        if(source_column->nullmap[samples[i]] == 1 && target_column->nullmap[samples[i]] == 1){
                            map[std::string(std::string(source_column->strings()(samples[i])))][std::string(target_column->strings()(samples[i]))] += 1;
                            mapped_count++;
                        }
                        else if(source_column->nullmap[samples[i]] == 0 && target_column->nullmap[samples[i]] == 1){
                            exceptions++;
                        }
                    }

                    // for each col1 key, find most common col2 value, count #non-exceptions/#exceptions
                    for(auto const& [col1Key, map2] : map){
                        int maxCounter = 0;
                        for(auto const& [col2Key, counter] : map2){
                            if(counter>maxCounter){
                                exceptions += maxCounter;
                                maxCounter = counter;
                            }
                            else{
                                exceptions += counter;
                            }
                        }
                    }
                    int extrapolated_num_exceptions = exceptions * target_column->tuple_count / samples.size(); // extrapolate num exceptions to entire column
                    extrapolated_num_exceptions = std::max(int(extrapolated_num_exceptions), int(sourceStats->stringStats->null_count));
                    estimated_exception_size = extrapolated_num_exceptions * (2*sizeof(btrblocks::INTEGER)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_target_dict_size = (sizeof(DictMeta) + targetStats->stringStats->total_unique_length + targetStats->stringStats->unique_count * sizeof(btrblocks::StringArrayViewer::Slot)) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    estimated_source_target_dict_size = sizeof(MultiDictMeta) + sourceStats->stringStats->unique_count * sizeof(btrblocks::INTEGER) / config.DICTIONARY_CASCADE_COMPRESSION_ESTIMATE;
                    target_size_compressed += estimated_target_dict_size; // target dict
                    target_size_compressed += estimated_source_target_dict_size; // source target dict
                    target_size_compressed += estimated_exception_size;
                    break;
                }
            }
            break;
        }
    }


    double compression_ratio = 1.0 * target_column->size / target_size_compressed;
    double exception_ratio = 1.0 * exceptions / samples.size();
    
    // no mapping exists between source and target in samples (e.g. due to null values)
    if(mapped_count==0){
        return {exception_ratio,1};
    }

    return {exception_ratio, compression_ratio};
}

std::vector<std::vector<uint8_t>> Dictionary_1to1::apply_scheme(std::shared_ptr<btrblocks::InputChunk>& source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::shared_ptr<Dictionary_1to1_CompressionScheme> scheme, bool skip_source_encoding, bool last_source_column_scheme, double& exception_ratio, const int& source_idx, const int& target_idx, std::vector<uint8_t>* sourceChunkVec, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes, std::shared_ptr<RowGroup> row_group){
// bool flag_revert_bb_if_bad
    // ... build dictionary, check exceptions...
    // if worse than BB && flag_revert_bb_if_bad: (comput)
        // exit with revert signal
        // remove scheme from graph final out/in edges
        // set columnStatus to None (check source first)
    // else: assign source chunk to sourceChunkVec

    // (done) if cancel compression, but a previous scheme needs the source column, still need to compress and return it...
        // if skip_source_encoding -> previous scheme needs source column
        // && last_source_column_scheme -> no other scheme uses source column
        // && flag_revert_bb_if_bad -> compress source column

    // (done) if transformed source
    // && cancel -> undo transform source

    // (done) don't update target col in place
    // if transformed target but don't need, then throw away transformed target

    std::shared_ptr<btrblocks::InputChunk> sourceColDictEncoded = source_column;
    std::shared_ptr<btrblocks::InputChunk> targetColDictEncoded;

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
    
    // 2.
    // dict encode target column: compute dict (compressed) and encoded target column
    // compute source-target dict and exceptions
    // compress source-target dict and exceptions
    // create compressed target column (C3Chunk)
    
    std::vector<uint8_t> target_output(sizeof(C3Chunk) + 10 * target_column->size, 0);    
    C3Chunk* targetChunk = reinterpret_cast<C3Chunk*>(target_output.data());    
    targetChunk->compression_type = static_cast<uint8_t>(SchemeType::Dict_1to1);    
    targetChunk->type = target_column->type;    
    targetChunk->source_column_id = source_idx;
    targetChunk->original_col_size = target_column->size;

    auto cross_dict_meta = reinterpret_cast<MultiDictMeta*>(targetChunk->data);

    // compress target nullmap
    auto nullmap_meta = reinterpret_cast<NullmapMeta*>(cross_dict_meta->data);
    auto [nullmap_size, bitmap_type] = btrblocks::bitmap::RoaringBitmap::compress(target_column->nullmap.get(), nullmap_meta->data, target_column->tuple_count);
    nullmap_meta->nullmap_type = bitmap_type;
    cross_dict_meta->targetDictOffset = sizeof(NullmapMeta) + nullmap_size;
    scheme->target_nullmap_size = nullmap_size;

    size_t compressed_target_dict_size;
    size_t uncompressed_target_dict_size;
    auto target_dict_meta = reinterpret_cast<DictMeta*>(cross_dict_meta->data + cross_dict_meta->targetDictOffset);
    target_dict_meta->type = target_column->type;
    targetColDictEncoded = single_col::Dictionary::apply_scheme(target_column, target_dict_meta, bbSchemes[target_idx], compressed_target_dict_size, uncompressed_target_dict_size);
    cross_dict_meta->crossDictOffset = cross_dict_meta->targetDictOffset + sizeof(DictMeta) + compressed_target_dict_size;

    // compute source-target dict
    C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(sourceChunkVec->data());
    auto source_dict_meta = reinterpret_cast<DictMeta*>(reinterpret_cast<C3Chunk*>(sourceChunkVec->data())->data);
    auto source_dict_tuple_count = c3::ChunkDecompression::bb_getTupleCount(source_dict_meta->data);

    std::map<int, std::map<int,int>> source_target_dict;
    for(size_t i=0; i<sourceColDictEncoded->tuple_count; i++){
        if(sourceColDictEncoded->nullmap[i] == 1 && targetColDictEncoded->nullmap[i] == 1){
            source_target_dict[sourceColDictEncoded->integers()[i]][targetColDictEncoded->integers()[i]] += 1;
        }
    }
    
    // assert(!source_target_dict.empty());

    auto dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[source_dict_tuple_count * sizeof(btrblocks::units::INTEGER)]()); 
    auto dict_buffer_writer = reinterpret_cast<btrblocks::units::INTEGER*>(dict_buffer.get());
    auto nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[source_dict_tuple_count]());

    // find most common mapping
    for(auto const& [sourceKey, map2] : source_target_dict){
        int maxCounter = 0;
        int maxTargetKey = 0;
        for(auto const& [targetKey, counter] : map2){
            if(counter>maxCounter){
                maxCounter = counter;
                maxTargetKey = targetKey;
            }
        }
        // assert(sourceKey < source_dict_tuple_count);
        dict_buffer_writer[sourceKey] = maxTargetKey;
        nullmap_buffer[sourceKey] = 1;
    }

    // find exceptions
    auto exception_values_buffer = std::make_unique<uint8_t[]>(targetColDictEncoded->tuple_count * sizeof(btrblocks::units::INTEGER)); // create buffer of same size as original col
    auto exception_values_nullmap_buffer = std::make_unique<uint8_t[]>(targetColDictEncoded->tuple_count); // create buffer for max num of exceptions
    auto exception_values = reinterpret_cast<btrblocks::units::INTEGER*>(exception_values_buffer.get());

    auto exception_index_buffer = std::make_unique<uint8_t[]>(targetColDictEncoded->tuple_count * sizeof(btrblocks::units::INTEGER)); // create buffer for max num of exceptions
    auto exception_indexes_nullmap_buffer = std::make_unique<uint8_t[]>(targetColDictEncoded->tuple_count);
    auto exception_indexes = reinterpret_cast<btrblocks::units::INTEGER*>(exception_index_buffer.get());
    
    size_t exception_counter = 0;
    for(size_t i=0; i<sourceColDictEncoded->tuple_count; i++){

        int target_val = targetColDictEncoded->integers()[i];
        if(targetColDictEncoded->nullmap[i] == 1){
            if((sourceColDictEncoded->nullmap[i] == 0) || 
            (sourceColDictEncoded->nullmap[i] == 1 && dict_buffer_writer[sourceColDictEncoded->integers()[i]] != target_val)){
                exception_indexes[exception_counter] = i;
                exception_values[exception_counter] = target_val;
                exception_indexes_nullmap_buffer[exception_counter] = 1;
                exception_values_nullmap_buffer[exception_counter] = 1;
                exception_counter++;
            }
        }
    }

    // compress source_target dictionary
    auto basic_stats_source_target_dict = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(source_dict_tuple_count)); // int uncompressed
    auto col_stats_source_target_dict = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_source_target_dict, {}));
    
    // 255 = autoscheme()
    if(config.DICTIONARY_COMPRESSION_SCHEME == 255){
        col_stats_source_target_dict = nullptr;
    }
    
    auto uncompressed_dict = btrblocks::InputChunk(std::move(dict_buffer), std::move(nullmap_buffer), btrblocks::ColumnType::INTEGER, source_dict_tuple_count, source_dict_tuple_count * sizeof(btrblocks::units::INTEGER));
    auto compressed_source_target_dict_size = btrblocks::Datablock::compress(uncompressed_dict, cross_dict_meta->data + cross_dict_meta->crossDictOffset, static_cast<uint8_t>(config.DICTIONARY_COMPRESSION_SCHEME), nullptr, nullptr, col_stats_source_target_dict);
    
    cross_dict_meta->exceptionsOffset = cross_dict_meta->crossDictOffset + compressed_source_target_dict_size;
    size_t cross_dict_meta_data_size = cross_dict_meta->exceptionsOffset + sizeof(ExceptionsMeta);
    
    exception_ratio = 1.0 * exception_counter / targetColDictEncoded->tuple_count;

    // compress exceptions
    if(exception_counter>0){
        auto exceptions_meta = reinterpret_cast<ExceptionsMeta*>(cross_dict_meta->data + cross_dict_meta->exceptionsOffset);
    
        auto basic_stats_exception_idx = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(exception_counter)); // int uncompressed
        auto col_stats_exception_idx = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_exception_idx, {}));
        auto basic_stats_exception_val = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(exception_counter)); // int uncompressed
        auto col_stats_exception_val = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_exception_val, {}));
        
        // 255 = autoscheme()
        if(config.EXCEPTION_COMPRESSION_SCHEME == 255){
            col_stats_exception_idx = nullptr;
            col_stats_exception_val = nullptr;
        }

        // exception indexes
        size_t exceptions_size = exception_counter * sizeof(btrblocks::units::INTEGER);
        auto uncompressed_indexes = btrblocks::InputChunk(std::move(exception_index_buffer), std::move(exception_indexes_nullmap_buffer), btrblocks::ColumnType::INTEGER, exception_counter, exceptions_size);
        auto compressed_indexes_size = btrblocks::Datablock::compress(uncompressed_indexes, exceptions_meta->data, static_cast<uint8_t>(config.EXCEPTION_COMPRESSION_SCHEME), nullptr, nullptr, col_stats_exception_idx);
        exceptions_meta->values_offset = compressed_indexes_size;

        // exception values
        auto uncompressed_values = btrblocks::InputChunk(std::move(exception_values_buffer), std::move(exception_values_nullmap_buffer), btrblocks::ColumnType::INTEGER, exception_counter, exceptions_size);
        auto compressed_values_size = btrblocks::Datablock::compress(uncompressed_values, exceptions_meta->data + exceptions_meta->values_offset, static_cast<uint8_t>(config.EXCEPTION_COMPRESSION_SCHEME), nullptr, nullptr, col_stats_exception_val);
        cross_dict_meta_data_size += exceptions_meta->values_offset + compressed_values_size;

        scheme->exception_compression_ratio = 2.0 * exceptions_size / (compressed_indexes_size + compressed_values_size);
        scheme->real_exception_size = (compressed_indexes_size + compressed_values_size);
    }
    
    targetChunk->btrblocks_ColumnChunkMeta_offset = sizeof(MultiDictMeta) + cross_dict_meta_data_size;
    target_output.resize(sizeof(C3Chunk) + targetChunk->btrblocks_ColumnChunkMeta_offset);
    
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

        scheme->real_exception_count = exception_counter;
        scheme->real_source_target_dict_size = compressed_source_target_dict_size;
        scheme->source_target_dict_compression_ratio = 1.0 * (source_dict_tuple_count * sizeof(btrblocks::units::INTEGER)) / compressed_source_target_dict_size;
        scheme->real_target_dict_size = compressed_target_dict_size;
        scheme->target_dict_compression_ratio = 1.0 * uncompressed_target_dict_size / compressed_target_dict_size;
        scheme->real_target_nullmap_size = nullmap_size;

        if(last_source_column_scheme){
            // 3.
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

std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> Dictionary_1to1::decompress(const std::vector<uint8_t>& source_column, C3Chunk* c3_meta, const std::vector<uint8_t>& source_nullmap, uint32_t tuple_count, bool& requires_copy){

    auto cross_dict_meta = reinterpret_cast<MultiDictMeta*>(c3_meta->data);

    // decompress original target null map
    auto target_nullmap_meta = reinterpret_cast<NullmapMeta*>(cross_dict_meta->data);
    auto m_bitset = new boost::dynamic_bitset<>(tuple_count);
    auto target_nullmap = std::make_unique<BitmapWrapper>(target_nullmap_meta->data, target_nullmap_meta->nullmap_type, tuple_count, m_bitset)->writeBITMAP();

    // decompress cross dict (int->int)
    std::vector<uint8_t> target_codes(tuple_count * sizeof(btrblocks::INTEGER));
    std::vector<uint8_t> cross_dict_decompressed_values;
    std::vector<uint8_t> cross_dict_decompressed_bitmap;
    requires_copy = ChunkDecompression::bb_decompressColumn(cross_dict_decompressed_values, cross_dict_meta->data + cross_dict_meta->crossDictOffset, cross_dict_decompressed_bitmap);

    // rebuild target column 
    auto cross_dict_reader = reinterpret_cast<const btrblocks::INTEGER*>(cross_dict_decompressed_values.data());
    auto source_reader = reinterpret_cast<const btrblocks::INTEGER*>(source_column.data());
    auto target_col_writer = reinterpret_cast<btrblocks::INTEGER*>(target_codes.data());
    
    for(size_t i=0; i<tuple_count; i++){
        // only write non-null target values
        if(source_nullmap[i] == 1 && target_nullmap[i] == 1){
            // assert(source_reader[i] < cross_dict_decompressed_bitmap.size());
            target_col_writer[i] = cross_dict_reader[source_reader[i]];
        }
    }

    // replace exceptions
    auto cross_exceptions_meta = reinterpret_cast<ExceptionsMeta*>(cross_dict_meta->data + cross_dict_meta->exceptionsOffset);
    bool has_exceptions = cross_exceptions_meta->values_offset > 0;

    if(has_exceptions){
        // decompress cross dict exceptions (int)
        std::vector<uint8_t> indexes_decompressed_values;
        std::vector<uint8_t> indexes_decompressed_bitmap;
        ChunkDecompression::bb_decompressColumn(indexes_decompressed_values, cross_exceptions_meta->data, indexes_decompressed_bitmap);
        
        std::vector<uint8_t> exception_values_decompressed;
        std::vector<uint8_t> exception_nullmap_decompressed;
        ChunkDecompression::bb_decompressColumn(exception_values_decompressed, cross_exceptions_meta->data + cross_exceptions_meta->values_offset, exception_nullmap_decompressed);
    
        for(size_t i=0; i<indexes_decompressed_bitmap.size(); i++){
            auto index = reinterpret_cast<btrblocks::units::INTEGER*>(indexes_decompressed_values.data())[i];
            auto value = reinterpret_cast<btrblocks::units::INTEGER*>(exception_values_decompressed.data())[i];
            // assert(index < tuple_count);
            target_col_writer[index] = value;
        }
    }

    // single col decompress target column
    auto target_dict = reinterpret_cast<DictMeta*>(cross_dict_meta->data + cross_dict_meta->targetDictOffset);
    std::pair<std::vector<uint8_t>,std::vector<btrblocks::BITMAP>> decompressed_source_column = single_col::Dictionary::decompress(target_codes, target_dict, tuple_count, c3_meta->original_col_size, target_nullmap, requires_copy);

    return {std::move(decompressed_source_column.first), std::move(target_nullmap)};
}


}
}