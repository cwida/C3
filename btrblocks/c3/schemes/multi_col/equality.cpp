#include "equality.hpp"
#include "c3/Utils.hpp"
#include "compression/Datablock.hpp"

namespace c3{
namespace multi_col{


bool Equality::skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j){
    
    if(row_group->columns[i]->type != row_group->columns[j]->type){
        return true;
    }
    
    if(config.USE_PRUNING_RULES){
        int guaranteed_exception_count = btrBlocksSchemes[j]->get_unique_count() - btrBlocksSchemes[i]->get_unique_count();
        if(guaranteed_exception_count > row_group->tuple_count * config.EQUALITY_EXCEPTION_RATIO_THRESHOLD){
            return true;
        }
    }

    return false;
}

// EXCEPTIONS
// is only an exception if:
    // source and target are not null && source != target
    // source is null && target is not null
// store target nullmap -> no need to handle target null values
std::pair<double,double> Equality::expectedCompressionRatio(std::shared_ptr<RowGroup> row_group, const std::shared_ptr<btrblocks::InputChunk> source_column, const std::shared_ptr<btrblocks::InputChunk> target_column, int& estimated_exception_size){
    // 1. switch data type
    // 2. compute size of original target column
    // 3. find exceptions: compute how many bits needed to store them
    // 4. compression ratio: original target column size / bits for exceptions

    int uncompressed_target_size;
    int compressed_target_size = sizeof(C3Chunk) + sizeof(EqualityMeta) + sizeof(ExceptionsMeta) + sizeof(NullmapMeta);
    int exception_counter = 0;

    // ---------- 1 ---------- //
    assert(source_column->type == target_column->type);
    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
            uncompressed_target_size = row_group->samples.size() * sizeof(int);

            for(const auto& sample: row_group->samples){
                if(target_column->nullmap[sample] == 1 && 
                (source_column->integers()[sample] != target_column->integers()[sample] || source_column->nullmap[sample] == 0)){
                    exception_counter++;
                }
            }
            compressed_target_size += exception_counter * (sizeof(int) + sizeof(uint32_t)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
            uncompressed_target_size = row_group->samples.size() * sizeof(double);

            for(const auto& sample: row_group->samples){
                if(target_column->nullmap[sample] == 1 && 
                (source_column->doubles()[sample] != target_column->doubles()[sample] || source_column->nullmap[sample] == 0)){
                    exception_counter++;
                }
            }
            compressed_target_size += exception_counter * (sizeof(double) + sizeof(uint32_t)) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
            break;
        }
        case btrblocks::ColumnType::STRING:{
            uncompressed_target_size = 0;
            
            for(const auto& sample: row_group->samples){
                std::string_view target_val = target_column->strings()(sample);

                if(target_column->nullmap[sample] == 1 && 
                (source_column->strings()[sample] != target_val || source_column->nullmap[sample] == 0)){
                    exception_counter++;
                    compressed_target_size += target_val.size() + sizeof(uint32_t) + sizeof(btrblocks::StringArrayViewer::Slot) / config.EXCEPTION_CASCADE_COMPRESSION_ESTIMATE;
                }
                uncompressed_target_size += target_val.size() + sizeof(btrblocks::StringArrayViewer::Slot); 
            }
            break;
        }
    }

    estimated_exception_size = compressed_target_size;
    double compression_ratio = 1.0 * uncompressed_target_size / compressed_target_size;
    double exception_ratio = 1.0 * exception_counter / row_group->samples.size();

    return {exception_ratio, compression_ratio};
}

std::vector<std::vector<uint8_t>> Equality::apply_scheme(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, double& exception_ratio, const int& source_idx, const int& target_idx, const uint8_t& force_source_scheme, std::shared_ptr<c3::EqualityCompressionScheme> scheme, bool skip_source_compression, bool previous_scheme_uses_source, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes){
    
    assert(source_column->type == target_column->type);

    // allocate large amount of data (in vector<u8>)
    // reinterpret cast to C3Chunk, assign values of members, pass data pointer to write to
    // reinterpret cast data to exceptions, assign values, pass data pointer to write to
    // return vector<u8>

    // prepare target chunk
    std::vector<uint8_t> target_output(sizeof(C3Chunk) + 10 * target_column->size);
    C3Chunk* targetChunk = reinterpret_cast<C3Chunk*>(target_output.data());
    targetChunk->compression_type = static_cast<uint8_t>(SchemeType::Equality);
    targetChunk->original_col_size = target_column->size;
    targetChunk->type = target_column->type;
    targetChunk->source_column_id = source_idx;

    auto equality_meta = reinterpret_cast<EqualityMeta*>(targetChunk->data);

    // compress target nullmap
    auto target_nullmap_meta = reinterpret_cast<NullmapMeta*>(equality_meta->data);
    auto [nullmap_size, bitmap_type] = btrblocks::bitmap::RoaringBitmap::compress(target_column->nullmap.get(), target_nullmap_meta->data, target_column->tuple_count);
    target_nullmap_meta->nullmap_type = bitmap_type;
    equality_meta->exceptionsOffset = sizeof(NullmapMeta) + nullmap_size;
    scheme->target_nullmap_size = nullmap_size;

   // nullmap: 0 = null, 1 = value
    auto values_nullmap_buffer = std::make_unique<uint8_t[]>(target_column->tuple_count); // create buffer for max num of exceptions
    auto indexes_nullmap_buffer = std::make_unique<uint8_t[]>(target_column->tuple_count);

    auto index_buffer = std::make_unique<uint8_t[]>(target_column->tuple_count * sizeof(btrblocks::units::INTEGER)); // create buffer for max num of exceptions
    auto exception_indexes = reinterpret_cast<btrblocks::units::INTEGER*>(index_buffer.get());
    
    auto values_buffer = std::make_unique<uint8_t[]>(target_column->size); // create buffer of same size as original col

    uint16_t exception_counter = 0;
    size_t values_size = 0;

    std::shared_ptr<ColumnStats> col_stats_exception_val;
        
    // get exceptions
    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
            auto exception_values = reinterpret_cast<btrblocks::units::INTEGER*>(values_buffer.get());
            for(size_t i=0; i<source_column->tuple_count; i++){
                int target_val = target_column->integers()[i];

                if(target_column->nullmap[i] == 1 
                && (source_column->integers()[i] != target_val || source_column->nullmap[i] == 0)){
                    // exception
                    values_nullmap_buffer[exception_counter] = 1;
                    indexes_nullmap_buffer[exception_counter] = 1;
                    exception_indexes[exception_counter] = i;
                    exception_values[exception_counter] = target_val;
                    exception_counter++;
                }
            }
            values_size = exception_counter * sizeof(btrblocks::units::INTEGER);
            
            auto basic_stats_exception_val = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(exception_counter)); // int uncompressed
            col_stats_exception_val = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_exception_val, {}));
            
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
            auto exception_values = reinterpret_cast<btrblocks::units::DOUBLE*>(values_buffer.get());
            for(size_t i=0; i<source_column->tuple_count; i++){
                double target_val = target_column->doubles()[i];

                if(target_column->nullmap[i] == 1 
                && (source_column->doubles()[i] != target_val || source_column->nullmap[i] == 0)){
                    // exception
                    values_nullmap_buffer[exception_counter] = 1;
                    indexes_nullmap_buffer[exception_counter] = 1;
                    exception_indexes[exception_counter] = i;
                    exception_values[exception_counter] = target_val;
                    exception_counter++;
                }
            }
            values_size = exception_counter * sizeof(btrblocks::units::DOUBLE);
            
            auto basic_stats_exception_val = std::make_shared<btrblocks::DoubleStats>(btrblocks::DoubleStats::generateStatsBasic(exception_counter)); // int uncompressed
            col_stats_exception_val = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::DOUBLE, basic_stats_exception_val, {}));
            
            break;
        }
        case btrblocks::ColumnType::STRING:{
            auto exception_value_slots = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(values_buffer.get());
            std::vector<std::string_view> exception_strings;
        
            for(size_t i=0; i<source_column->tuple_count; i++){
                std::string_view target_val = target_column->strings()(i);

                if(target_column->nullmap[i] == 1 
                && (source_column->strings()(i) != target_val || source_column->nullmap[i] == 0)){
                    // exception
                    values_nullmap_buffer[exception_counter] = 1;
                    indexes_nullmap_buffer[exception_counter] = 1;
                    exception_indexes[exception_counter] = i;
                    exception_strings.push_back(target_val);
                    exception_counter++;
                }
            }

            // init string slots
            exception_value_slots[0].offset = (exception_counter + 1) * sizeof(btrblocks::StringArrayViewer::Slot);
            for(size_t i=0; i<exception_counter; i++){
                exception_value_slots[i+1].offset = exception_value_slots[i].offset + exception_strings[i].size();
                memcpy(values_buffer.get() + exception_value_slots[i].offset, exception_strings[i].begin(), exception_strings[i].size());
                // std::cout << std::string(exception_strings[i].begin(), exception_strings[i].begin()+exception_strings[i].size()) << std::endl;
            }
            values_size = exception_value_slots[exception_counter].offset;
            
            auto basic_stats_exception_val = std::make_shared<btrblocks::StringStats>(btrblocks::StringStats::generateStatsBasic(exception_counter, values_size)); // int uncompressed
            col_stats_exception_val = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::STRING, basic_stats_exception_val, {}));
                        
            break;
        }
    }

    size_t exceptions_size = sizeof(ExceptionsMeta);
    
    if(exception_counter>0){
        auto target_exceptions_meta = reinterpret_cast<ExceptionsMeta*>(equality_meta->data + equality_meta->exceptionsOffset);
        
        auto basic_stats_exception_idx = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(exception_counter)); // int uncompressed
        auto col_stats_exception_idx = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_exception_idx, {}));
        
        // 255 = autoscheme()
        if(config.EXCEPTION_COMPRESSION_SCHEME == 255){
            col_stats_exception_idx = nullptr;
            col_stats_exception_val = nullptr;
        }

        // compress exception indexes
        size_t indexes_size = exception_counter * sizeof(btrblocks::units::INTEGER);
        auto uncompressed_indexes = btrblocks::InputChunk(std::move(index_buffer), std::move(indexes_nullmap_buffer), btrblocks::ColumnType::INTEGER, exception_counter, indexes_size);
        auto compressed_indexes_size = btrblocks::Datablock::compress(uncompressed_indexes, target_exceptions_meta->data, static_cast<uint8_t>(config.EXCEPTION_COMPRESSION_SCHEME), nullptr, nullptr, col_stats_exception_idx);
        target_exceptions_meta->values_offset = compressed_indexes_size;
        
        // compress exception values
        auto uncompressed_values = btrblocks::InputChunk(std::move(values_buffer), std::move(values_nullmap_buffer), target_column->type, exception_counter, values_size);
        auto compressed_values_size = btrblocks::Datablock::compress(uncompressed_values, target_exceptions_meta->data + target_exceptions_meta->values_offset, static_cast<uint8_t>(config.EXCEPTION_COMPRESSION_SCHEME), nullptr, nullptr, col_stats_exception_val);

        exceptions_size += target_exceptions_meta->values_offset + compressed_values_size;

        scheme->exception_compression_ratio = 1.0 * (indexes_size + values_size) / (compressed_indexes_size + compressed_values_size);
        scheme->real_exception_size = (compressed_indexes_size + compressed_values_size);
    }
    
    targetChunk->btrblocks_ColumnChunkMeta_offset = sizeof(EqualityMeta) + equality_meta->exceptionsOffset + exceptions_size;
    target_output.resize(sizeof(C3Chunk) + targetChunk->btrblocks_ColumnChunkMeta_offset);

    // if worse than BB ECR, cancel C3 compression
    // subtract target_nullmap_size, because BB scheme ECR also doesn't include nullmap
    if(config.REVERT_BB_IF_C3_BAD && (target_output.size() - scheme->target_nullmap_size) - (target_column->size / bbSchemes[target_idx]->get_best_scheme().second) > config.BYTES_SAVED_MARGIN * bbSchemes[target_idx]->get_original_chunk_size()){
        if(!skip_source_compression && previous_scheme_uses_source){
            // prepare source chunk & compress
            std::vector<uint8_t> source_output(sizeof(C3Chunk) + 10 * source_column->size);
            C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(source_output.data());
            sourceChunk->compression_type = static_cast<uint8_t>(SchemeType::BB);
            sourceChunk->type = source_column->type;
            sourceChunk->source_column_id = source_idx; // not used, since compression type set to BB
            sourceChunk->btrblocks_ColumnChunkMeta_offset = 0;
            auto compressed_source_size = btrblocks::Datablock::compress(*source_column, sourceChunk->data, force_source_scheme, &scheme->source_bb_scheme, &scheme->source_nullmap_size, bbSchemes[source_idx]);
            source_output.resize(sizeof(*sourceChunk) + compressed_source_size);
            return {source_output, {}};
        }
        else{
            return {{},{}};
        }
    }
    else{
        scheme->real_target_nullmap_size = nullmap_size;
        scheme->real_exception_count = exception_counter;
        exception_ratio = 1.0 * exception_counter / target_column->tuple_count;

        if(!skip_source_compression){
            // prepare source chunk & compress
            std::vector<uint8_t> source_output(sizeof(C3Chunk) + 10 * source_column->size);
            C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(source_output.data());
            sourceChunk->compression_type = static_cast<uint8_t>(SchemeType::BB);
            sourceChunk->type = source_column->type;
            sourceChunk->source_column_id = source_idx; // not used, since compression type set to BB
            sourceChunk->btrblocks_ColumnChunkMeta_offset = 0;
            auto compressed_source_size = btrblocks::Datablock::compress(*source_column, sourceChunk->data, force_source_scheme, &scheme->source_bb_scheme, &scheme->source_nullmap_size, bbSchemes[source_idx]);
            source_output.resize(sizeof(*sourceChunk) + compressed_source_size);
            return {source_output, target_output};
        }
        
        // source column stays unchanged, target column is removed
        return {{}, target_output};
    }
}

std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> Equality::decompress(const std::vector<uint8_t>& source_column, c3::C3Chunk* targetChunk, size_t tuple_count, bool source_requires_copy){

    std::vector<uint8_t> output;

    // recreate target column: memcpy and replace exceptions
    auto target_col_meta = reinterpret_cast<c3::C3Chunk*>(targetChunk);
    auto equality_meta = reinterpret_cast<EqualityMeta*>(target_col_meta->data);

    // decompress original target null map
    auto target_nullmap_meta = reinterpret_cast<NullmapMeta*>(equality_meta->data);
    auto m_bitset = new boost::dynamic_bitset<>(tuple_count);
    auto target_nullmap = std::make_unique<BitmapWrapper>(target_nullmap_meta->data, target_nullmap_meta->nullmap_type, tuple_count, m_bitset)->writeBITMAP();

    auto exceptions_meta = reinterpret_cast<ExceptionsMeta*>(equality_meta->data + equality_meta->exceptionsOffset);
    bool has_exceptions = exceptions_meta->values_offset == 0 ? false : true;

    std::vector<uint8_t> indexes_decompressed_values; // bitmap should always be ALLONES
    std::vector<uint8_t> indexes_decompressed_bitmap; // bitmap should always be ALLONES
    if(has_exceptions){
        c3::ChunkDecompression::bb_decompressColumn(indexes_decompressed_values, exceptions_meta->data, indexes_decompressed_bitmap);
    }

    auto foo_exception_count = indexes_decompressed_bitmap.size();

    switch(target_col_meta->type){
        case btrblocks::ColumnType::INTEGER:{
            // target_col_meta->original_col_size != source_column.size(), since source col size gets extra SIMD bits on decompression
            int size = source_column.size();
            output.resize(size);
            std::memcpy(output.data(), source_column.data(), source_column.size());

            if(has_exceptions){
                std::vector<uint8_t> exception_values_decompressed;
                std::vector<uint8_t> exception_nullmap_decompressed;
                c3::ChunkDecompression::bb_decompressColumn(exception_values_decompressed, exceptions_meta->data + exceptions_meta->values_offset, exception_nullmap_decompressed);

                auto target_vals = reinterpret_cast<btrblocks::INTEGER*>(output.data());
                for(size_t i=0; i<indexes_decompressed_bitmap.size(); i++){
                    auto index = reinterpret_cast<btrblocks::units::INTEGER*>(indexes_decompressed_values.data())[i];
                    auto value = reinterpret_cast<btrblocks::units::INTEGER*>(exception_values_decompressed.data())[i];
                    target_vals[index] = value;
                }
            }

            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
            // target_col_meta->original_col_size != source_column.size(), since source col size gets extra SIMD bits on decompression
            int size = source_column.size();
            output.resize(size);
            std::memcpy(output.data(), source_column.data(), source_column.size());

            if(has_exceptions){
                std::vector<uint8_t> exception_values_decompressed;
                std::vector<uint8_t> exception_nullmap_decompressed;
                c3::ChunkDecompression::bb_decompressColumn(exception_values_decompressed, exceptions_meta->data + exceptions_meta->values_offset, exception_nullmap_decompressed);

                auto target_vals = reinterpret_cast<btrblocks::DOUBLE*>(output.data());
                for(size_t i=0; i<indexes_decompressed_bitmap.size(); i++){
                    auto index = reinterpret_cast<btrblocks::units::INTEGER*>(indexes_decompressed_values.data())[i];
                    auto value = reinterpret_cast<btrblocks::units::DOUBLE*>(exception_values_decompressed.data())[i];
                    target_vals[index] = value;
                }
            }
            break;
        }
        case btrblocks::ColumnType::STRING:{

            if(!has_exceptions){
                int size = source_column.size(); // source size may differ from original target size, if source is decompressed as no copy
                output.resize(size);
                std::memcpy(output.data(), source_column.data(), source_column.size());
            }
            else{
                std::vector<uint8_t> exception_values_decompressed;
                std::vector<uint8_t> exception_nullmap_decompressed;
                bool exception_values_requires_copy = c3::ChunkDecompression::bb_decompressColumn(exception_values_decompressed, exceptions_meta->data + exceptions_meta->values_offset, exception_nullmap_decompressed);

                if(source_requires_copy){

                    // copy whole source to target
                    // copy exception strings to end of column strings
                    // update exception offsets & lengths

                    size_t num_exceptions = exception_nullmap_decompressed.size();
                    if(exception_values_requires_copy){

                        output.resize(source_column.size() * 2);
                        memcpy(output.data(), source_column.data(), source_column.size());
                
                        auto target_string_views = reinterpret_cast<btrblocks::StringPointerArrayViewer::View*>(output.data());

                        auto total_exceptions_size = exception_values_decompressed.size();
                        auto strings_viewer = reinterpret_cast<btrblocks::StringPointerArrayViewer::View*>(exception_values_decompressed.data());
                        size_t strings_start_offset = num_exceptions * sizeof(btrblocks::StringPointerArrayViewer::View);
                        memcpy(output.data() + source_column.size(), exception_values_decompressed.data() + strings_start_offset, total_exceptions_size - strings_start_offset);

                        for(size_t ex_i=0; ex_i<exception_nullmap_decompressed.size(); ex_i++){
                            // update exception view offset & length
                            size_t exception_idx = reinterpret_cast<btrblocks::units::INTEGER*>(indexes_decompressed_values.data())[ex_i];
                            std::string_view exception_value = exception_values_requires_copy ? btrblocks::StringPointerArrayViewer(exception_values_decompressed.data())(ex_i) : btrblocks::StringArrayViewer(exception_values_decompressed.data())(ex_i);
                            auto strings_viewer = reinterpret_cast<btrblocks::StringPointerArrayViewer::View*>(exception_values_decompressed.data());

                            target_string_views[exception_idx].offset = source_column.size() + strings_viewer[ex_i].offset - strings_start_offset;
                            target_string_views[exception_idx].length = strings_viewer[ex_i].length;
                        }

                        output.resize(source_column.size() + total_exceptions_size - strings_start_offset);

                    }
                    else{

                        output.resize(source_column.size() * 2);
                        memcpy(output.data(), source_column.data(), source_column.size());
                        auto target_string_views = reinterpret_cast<btrblocks::StringPointerArrayViewer::View*>(output.data());

                        auto total_exceptions_size = exception_values_decompressed.size();
                        auto strings_viewer = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(exception_values_decompressed.data());
                        size_t strings_start_offset = strings_viewer[0].offset;
                        size_t strings_end_offset = strings_viewer[num_exceptions].offset;
                        memcpy(output.data() + source_column.size(), exception_values_decompressed.data() + strings_start_offset, strings_end_offset - strings_start_offset);
                        
                        for(size_t ex_i=0; ex_i<exception_nullmap_decompressed.size(); ex_i++){
                            // update exception view offset & length
                            size_t exception_idx = reinterpret_cast<btrblocks::units::INTEGER*>(indexes_decompressed_values.data())[ex_i];
                            // std::string_view exception_value = exception_values_requires_copy ? btrblocks::StringPointerArrayViewer(exception_values_decompressed.data())(ex_i) : btrblocks::StringArrayViewer(exception_values_decompressed.data())(ex_i);
                            auto strings_viewer = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(exception_values_decompressed.data());

                            target_string_views[exception_idx].offset = source_column.size() + strings_viewer[ex_i].offset - strings_start_offset;
                            target_string_views[exception_idx].length = strings_viewer[ex_i+1].offset - strings_viewer[ex_i].offset;
                        }

                        output.resize(source_column.size() + total_exceptions_size - strings_start_offset);
                    }
                }
                else{
                    
                    output.resize(source_column.size() + target_col_meta->original_col_size);
                    auto source_strings = btrblocks::StringArrayViewer(source_column.data());
                    std::vector<std::string_view> target_strings(tuple_count, "");
                    for(int i=0; i<tuple_count; i++){
                        target_strings[i] = source_strings[i];
                    }

                    // patch exceptions
                    for(size_t ex_i=0; ex_i<exception_nullmap_decompressed.size(); ex_i++){
                        size_t exception_idx = reinterpret_cast<btrblocks::units::INTEGER*>(indexes_decompressed_values.data())[ex_i];
                        std::string_view exception_value = exception_values_requires_copy ? btrblocks::StringPointerArrayViewer(exception_values_decompressed.data())(ex_i) : btrblocks::StringArrayViewer(exception_values_decompressed.data())(ex_i);
                        target_strings[exception_idx] = exception_value;
                    }

                    auto target_slots = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(output.data());
                    target_slots[0].offset = (tuple_count + 1) * sizeof(btrblocks::StringArrayViewer::Slot);
                    for(int i=0; i<tuple_count; i++){
                        target_slots[i+1].offset = target_slots[i].offset + target_strings[i].size();
                        memcpy(output.data() + target_slots[i].offset, target_strings[i].begin(), target_strings[i].size());
                    }
                    output.resize(target_slots[tuple_count].offset);
                }
            }
            
            break;
        }
    }
    return {std::move(output), std::move(target_nullmap)};
}

}
}
