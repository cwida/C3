#include "dictionary.hpp"
#include "c3/Utils.hpp"
#include "compression/Datablock.hpp"
#include "c3/storage/Datablock.hpp"

namespace c3{
namespace single_col{

// return compression_ratio
// double Dictionary::expectedDictionaryCompressionRatio(const std::shared_ptr<btrblocks::InputChunk> column, std::shared_ptr<ColumnStats> columnStats){
    
//     // assume best case for null-encoded values, should compensate a bit for worst-case used to compute bits needed

//     // compressed size = tuple_count * (std::floor(std::log2(#unique_values)) + 1) + sizeof(unique_values)

//     int sizeUncompressed = column->size;
//     int sizeCompressed = sizeof(C3Chunk) + sizeof(DictMeta);

//     int unique_count = 0;
//     int null_count = 0;
//     int first_val_count = 0;

//     switch(column->type){
//         case btrblocks::ColumnType::INTEGER:{
//             unique_count = columnStats->intStats->unique_count;
//             null_count = columnStats->intStats->null_count;

//             // find first non-null val
//             if(columnStats->intStats->first_non_null_idx > 0){
//                 first_val_count = columnStats->intStats->distinct_values[column->integers()[columnStats->intStats->first_non_null_idx]];
//             }
            
//             sizeCompressed += columnStats->intStats->unique_count * sizeof(btrblocks::units::INTEGER); // dict size
//             break;
//         }
//         case btrblocks::ColumnType::DOUBLE:{
//             unique_count = columnStats->doubleStats->unique_count;
//             null_count = columnStats->doubleStats->null_count;

//             // find first non-null val
//             if(columnStats->doubleStats->first_non_null_idx > 0){
//                 first_val_count = columnStats->doubleStats->distinct_values[column->doubles()[columnStats->doubleStats->first_non_null_idx]];
//             }

//             sizeCompressed += columnStats->doubleStats->unique_count * sizeof(btrblocks::units::DOUBLE); // dict size
//             break;
//         }
//         case btrblocks::ColumnType::STRING:{
//             unique_count = columnStats->stringStats->unique_count;
//             null_count = columnStats->stringStats->null_count;

//             // find first non-null val
//             if(columnStats->stringStats->first_non_null_idx > 0){
//                 first_val_count = columnStats->stringStats->distinct_values_counter[column->strings()[columnStats->stringStats->first_non_null_idx]];
//             }

//             sizeCompressed += (columnStats->stringStats->unique_count + 1) * sizeof(btrblocks::StringArrayViewer::Slot) + columnStats->stringStats->total_unique_length; // dict size
//             break;
//         }
//         default: std::cout << "data type not supported" << std::endl;
//     }

//     // both null values and first non-null value are encoded as 0
//     // for 0-encoded values, potentially need only 4 bytes per 128 values
//     int null_encoded_count = 0; // null_count + first_val_count;
//     sizeCompressed += std::ceil(1.0 * null_encoded_count / 128) * 4; // 0 encoded values
//     sizeCompressed += (column->tuple_count - null_encoded_count) * (std::floor(std::log2(unique_count)) + 1) / 8; // remaining codes

//     return (double)sizeUncompressed / sizeCompressed;
// }


std::shared_ptr<btrblocks::InputChunk> Dictionary::apply_scheme(std::shared_ptr<btrblocks::InputChunk> source_column, DictMeta* dict_meta, std::shared_ptr<ColumnStats> column_stats, size_t& compressed_dict_size, size_t& uncompressed_dict_size){

    int size = source_column->tuple_count * sizeof(btrblocks::units::INTEGER);
    auto data = std::unique_ptr<uint8_t[]>(new uint8_t[size]());
    auto encoded_source = reinterpret_cast<btrblocks::units::INTEGER*>(data.get());
    
    std::unique_ptr<uint8_t[]> dict_buffer; 
    auto dict_nullmap_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[column_stats->get_unique_count()]());
    int code_counter = 0;
    int dict_size = 0;
    
    std::shared_ptr<c3::ColumnStats> dict_stats;
    
    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{ 
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[column_stats->get_unique_count() * sizeof(btrblocks::units::INTEGER)]());
            auto dict_buffer_writer = reinterpret_cast<btrblocks::units::INTEGER*>(dict_buffer.get());

            for(auto const& [source_val, counter] : column_stats->intStats->distinct_values){
                dict_buffer_writer[counter.first] = source_val;
                dict_nullmap_buffer[counter.first] = 1;
            }

            for(size_t i=0; i<source_column->tuple_count; i++){
                encoded_source[i] = column_stats->intStats->distinct_values[source_column->integers()[i]].first;
            }
            code_counter = column_stats->intStats->unique_count;
            dict_size = code_counter * sizeof(btrblocks::units::INTEGER);
            
            auto basic_dict_stats = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(code_counter)); // uncompressed
            dict_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_dict_stats, {}));

            break;
        }
        case btrblocks::ColumnType::DOUBLE:{      
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[column_stats->get_unique_count() * sizeof(btrblocks::units::DOUBLE)]());
            auto dict_buffer_writer = reinterpret_cast<btrblocks::units::DOUBLE*>(dict_buffer.get());

            for(auto const& [source_val, counter] : column_stats->doubleStats->distinct_values){
                dict_buffer_writer[counter.first] = source_val;
                dict_nullmap_buffer[counter.first] = 1;
            }

            for(size_t i=0; i<source_column->tuple_count; i++){
                encoded_source[i] = column_stats->doubleStats->distinct_values[source_column->doubles()[i]].first;                       
            }
            code_counter = column_stats->doubleStats->unique_count;
            dict_size = code_counter * sizeof(btrblocks::units::DOUBLE);
            
            auto basic_dict_stats = std::make_shared<btrblocks::DoubleStats>(btrblocks::DoubleStats::generateStatsBasic(code_counter)); // uncompressed
            dict_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::DOUBLE, basic_dict_stats, {}));

            break;
        }
        case btrblocks::ColumnType::STRING:{            
            
            std::vector<std::string_view> source_dict_reverse(column_stats->get_unique_count());
            std::map<std::string_view, int> source_dict;
            size_t distinct_strings_size = column_stats->stringStats->total_unique_length;
            code_counter = column_stats->stringStats->unique_count;
            
            for(auto const& [source_val, counter] : column_stats->stringStats->distinct_values_counter){
                source_dict_reverse[counter] = source_val;
                dict_nullmap_buffer[counter] = 1;
            }

            for(size_t i=0; i<source_column->tuple_count; i++){
                encoded_source[i] = column_stats->stringStats->distinct_values_counter[source_column->strings()[i]];                     
            }

            dict_size = sizeof(btrblocks::StringArrayViewer::Slot) * (code_counter + 1) + distinct_strings_size;
            dict_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[dict_size]());
            auto string_slots = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(dict_buffer.get());
            string_slots[0].offset = sizeof(btrblocks::StringArrayViewer::Slot) * (code_counter + 1);
            for(size_t i=0; i<code_counter; i++){
                auto dest = dict_buffer.get() + string_slots[i].offset;
                memcpy(dest, source_dict_reverse[i].begin(), source_dict_reverse[i].length());
                string_slots[i+1].offset = string_slots[i].offset + source_dict_reverse[i].length();
            }

            auto basic_dict_stats = std::make_shared<btrblocks::StringStats>(btrblocks::StringStats::generateStatsBasic(code_counter, dict_size)); // uncompressed
            dict_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::STRING, basic_dict_stats, {}));

            break;
        }
    }

    uncompressed_dict_size = dict_size;

    // 255 = autoscheme()
    if(config.DICTIONARY_COMPRESSION_SCHEME == 255){
        dict_stats = nullptr;
    }
    
    auto uncompressed_dict = btrblocks::InputChunk(std::move(dict_buffer), std::move(dict_nullmap_buffer), source_column->type, code_counter, dict_size);
    compressed_dict_size = btrblocks::Datablock::compress(uncompressed_dict, dict_meta->data, static_cast<uint8_t>(config.DICTIONARY_COMPRESSION_SCHEME), nullptr, nullptr, dict_stats);

    return std::make_shared<btrblocks::InputChunk>(std::move(data), std::move(source_column->get_nullmap_copy()), btrblocks::ColumnType::INTEGER, source_column->tuple_count, size);
}

std::shared_ptr<btrblocks::InputChunk> Dictionary::apply_scheme_dict_sharing(std::shared_ptr<btrblocks::InputChunk> source_column, DictMeta* dict_meta, std::shared_ptr<SharedDict> shared_dict, std::unique_ptr<uint8_t[]> dict_buffer, int dict_size, std::unique_ptr<uint8_t[]> dict_nullmap_buffer, size_t& compressed_dict_size){

    int size = source_column->tuple_count * sizeof(btrblocks::units::INTEGER);
    auto data = std::unique_ptr<uint8_t[]>(new uint8_t[size]());
    auto encoded_source = reinterpret_cast<btrblocks::units::INTEGER*>(data.get());
    int code_counter;
    
    std::shared_ptr<c3::ColumnStats> dict_stats;
    
    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
            code_counter = shared_dict->intMap.size();

            for(size_t i=0; i<source_column->tuple_count; i++){
                encoded_source[i] = shared_dict->intMap[source_column->integers()[i]].first;
            }
            
            auto basic_dict_stats = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(code_counter)); // uncompressed
            dict_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_dict_stats, {}));

            break;
        }
        case btrblocks::ColumnType::DOUBLE:{         
            code_counter = shared_dict->doubleMap.size();

            for(size_t i=0; i<source_column->tuple_count; i++){
                encoded_source[i] = shared_dict->doubleMap[source_column->doubles()[i]].first;
            }

            auto basic_dict_stats = std::make_shared<btrblocks::DoubleStats>(btrblocks::DoubleStats::generateStatsBasic(code_counter)); // uncompressed
            dict_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::DOUBLE, basic_dict_stats, {}));

            break;
        }
        case btrblocks::ColumnType::STRING:{            
            code_counter = shared_dict->stringMap.size();

            for(size_t i=0; i<source_column->tuple_count; i++){
                encoded_source[i] = shared_dict->stringMap[source_column->strings()[i]];                     
            }

            auto basic_dict_stats = std::make_shared<btrblocks::StringStats>(btrblocks::StringStats::generateStatsBasic(code_counter, dict_size)); // uncompressed
            dict_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::STRING, basic_dict_stats, {}));

            break;
        }
    }

    int dict_compression_scheme = config.DICTIONARY_COMPRESSION_SCHEME;

    // 255 = autoscheme()
    if(dict_compression_scheme == 255){
        dict_stats = nullptr;
        // // workaround for problem with FSST on large dictionary
        if(source_column->type==btrblocks::ColumnType::STRING){
            dict_compression_scheme = 0; // uncompressed
            // std::cout << "size: "  << dict_size << ",counter: " << code_counter << ",type: " << Utils::bb_ColumnType_to_string(source_column->type) << std::endl;
        }
    }

    if(dict_meta != nullptr){
        auto uncompressed_dict = btrblocks::InputChunk(std::move(dict_buffer), std::move(dict_nullmap_buffer), source_column->type, code_counter, dict_size);
        compressed_dict_size = btrblocks::Datablock::compress(uncompressed_dict, dict_meta->data, static_cast<uint8_t>(dict_compression_scheme), nullptr, nullptr, dict_stats);
    }

    return std::make_shared<btrblocks::InputChunk>(std::move(data), std::move(source_column->get_nullmap_copy()), btrblocks::ColumnType::INTEGER, source_column->tuple_count, size);
}

std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> Dictionary::decompress(const std::vector<uint8_t>& column, DictMeta* dict_meta, const uint32_t tuple_count, int32_t original_col_size, std::vector<btrblocks::BITMAP> col_nullmap, bool& requires_copy, std::vector<uint8_t> decompressed_dict_values, std::vector<uint8_t> decompressed_dict_nullmap){

    std::vector<uint8_t> output;
    
    if(decompressed_dict_values.empty()){
        // decompress dict
        requires_copy = ChunkDecompression::bb_decompressColumn(decompressed_dict_values, dict_meta->data, decompressed_dict_nullmap);
    }

    auto column_values = reinterpret_cast<const btrblocks::units::INTEGER*>(column.data());

    auto dict_num_tuples = decompressed_dict_nullmap.size();

    switch(dict_meta->type){
        case btrblocks::ColumnType::INTEGER:{
            output.resize(tuple_count * sizeof(btrblocks::INTEGER));
            auto output_values = reinterpret_cast<btrblocks::units::INTEGER*>(output.data());
            auto dict = reinterpret_cast<btrblocks::units::INTEGER*>(decompressed_dict_values.data());

            for(size_t i=0; i<tuple_count; i++){
                if(col_nullmap[i] == 1){
                    output_values[i] = dict[column_values[i]];
                }
            }
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{
            output.resize(tuple_count * sizeof(btrblocks::DOUBLE));
            auto output_values = reinterpret_cast<btrblocks::units::DOUBLE*>(output.data());
            auto dict = reinterpret_cast<btrblocks::units::DOUBLE*>(decompressed_dict_values.data());

            for(size_t i=0; i<tuple_count; i++){
                if(col_nullmap[i] == 1){
                    output_values[i] = dict[column_values[i]];
                }
            }
            break;
        }
        case btrblocks::ColumnType::STRING:{

            if(requires_copy){

                // copy strings to end of col
                // update offsets and lengths of all views

                size_t target_views_size = tuple_count * sizeof(btrblocks::StringPointerArrayViewer::View);
                size_t dict_views_size = dict_num_tuples * sizeof(btrblocks::StringPointerArrayViewer::View);
                output.resize(target_views_size + decompressed_dict_values.size() - dict_views_size);

                memcpy(output.data() + target_views_size, decompressed_dict_values.data() + dict_views_size, decompressed_dict_values.size() - dict_views_size);

                auto dict_string_views = reinterpret_cast<btrblocks::StringPointerArrayViewer::View*>(decompressed_dict_values.data());
                auto target_views = reinterpret_cast<btrblocks::StringPointerArrayViewer::View*>(output.data());

                for(size_t i=0; i<tuple_count; i++){
                    if(col_nullmap[i] == 1){
                        target_views[i].offset = target_views_size + dict_string_views[column_values[i]].offset - dict_views_size;
                        target_views[i].length = dict_string_views[column_values[i]].length;
                    }
                }

            }
            else{

                output.resize(original_col_size * 2);

                auto dict_string_viewer = btrblocks::StringArrayViewer(decompressed_dict_values.data());
                auto slots = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(output.data());
                slots[0].offset = sizeof(btrblocks::StringArrayViewer::Slot) * (tuple_count + 1);

                for(size_t i=0; i<tuple_count; i++){
                    if(col_nullmap[i] == 1){
                        std::string_view val = dict_string_viewer(column_values[i]);
                        slots[i+1].offset = slots[i].offset + val.size();
                        std::memcpy(output.data() + slots[i].offset, val.begin(), val.size());
                    }
                    else{
                        slots[i+1].offset = slots[i].offset;
                    }
                }

                output.resize(slots[tuple_count].offset + sizeof(btrblocks::StringArrayViewer::Slot));
            }

            break;
        }
    }

    return {std::move(output), std::move(col_nullmap)};
}

}
}