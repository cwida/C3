#include "c3/Utils.hpp"
#include "compression/Datablock.hpp"
#include "compression/SchemePicker.hpp"

struct C3Config config;

namespace c3{

ColumnStats::ColumnStats(btrblocks::ColumnType type, std::shared_ptr<btrblocks::SInteger32Stats> stats, std::vector<std::pair<uint8_t, double>> scr, int uncompressed_size, int compressed_nullmap_size)
:type(type), intStats(std::move(stats)), scheme_compression_ratios(scr), uncompressed_size(uncompressed_size), compressed_nullmap_size(compressed_nullmap_size)
{}

ColumnStats::ColumnStats(btrblocks::ColumnType type, std::shared_ptr<btrblocks::DoubleStats> stats, std::vector<std::pair<uint8_t, double>> scr, int uncompressed_size, int compressed_nullmap_size)
:type(type), doubleStats(std::move(stats)), scheme_compression_ratios(scr), uncompressed_size(uncompressed_size), compressed_nullmap_size(compressed_nullmap_size)
{}

ColumnStats::ColumnStats(btrblocks::ColumnType type, std::shared_ptr<btrblocks::StringStats> stats, std::vector<std::pair<uint8_t, double>> scr, int uncompressed_size, int compressed_nullmap_size)
:type(type), stringStats(std::move(stats)), scheme_compression_ratios(scr), uncompressed_size(uncompressed_size), compressed_nullmap_size(compressed_nullmap_size)
{}

size_t ColumnStats::get_BB_compressed_size() const{
    return 1.0 * uncompressed_size / get_best_scheme().second + compressed_nullmap_size;
}

size_t ColumnStats::get_original_chunk_size() const{
    switch(type){
        case btrblocks::ColumnType::INTEGER:{
            return intStats->total_size;
        }
        case btrblocks::ColumnType::DOUBLE:{
            return doubleStats->total_size;
        }
        case btrblocks::ColumnType::STRING:{
            return stringStats->total_size;
        }
    }
}

std::pair<uint8_t, double> ColumnStats::get_best_scheme() const{
    std::pair<uint8_t, double> best_scheme = {0, 0};
    for(const auto& scheme: scheme_compression_ratios){
        if(scheme.second > best_scheme.second){
            best_scheme = scheme;
        }
    }
    return best_scheme;
}

double ColumnStats::get_dict_compression_ratio() const{
    for(const auto& scheme: scheme_compression_ratios){
        if(scheme.first == 2){
            return scheme.second;
        }
    }
    return 1;
}

size_t ColumnStats::get_unique_count() const{
    switch(type){
        case btrblocks::ColumnType::INTEGER:{
            return intStats->unique_count;
        }
        case btrblocks::ColumnType::DOUBLE:{
            return doubleStats->unique_count;
        }
        case btrblocks::ColumnType::STRING:{
            return stringStats->unique_count;
        }
    }
}

void* ColumnStats::get_stats(){
    switch(type){
        case btrblocks::ColumnType::INTEGER:{
            return reinterpret_cast<void*>(&intStats);
        }
        case btrblocks::ColumnType::DOUBLE:{
            return reinterpret_cast<void*>(&doubleStats);
        }
        case btrblocks::ColumnType::STRING:{
            return reinterpret_cast<void*>(&stringStats);
        }
    }
}

size_t ColumnStats::get_null_count() const{
    switch(type){
        case btrblocks::ColumnType::INTEGER:{
            return intStats->null_count;
        }
        case btrblocks::ColumnType::DOUBLE:{
            return doubleStats->null_count;
        }
        case btrblocks::ColumnType::STRING:{
            return stringStats->null_count;
        }
    }
}

uint32_t ChunkDecompression::bb_getTupleCount(uint8_t* data){

    auto meta = reinterpret_cast<btrblocks::ColumnChunkMeta*>(data);
    return meta->tuple_count;

}

bool ChunkDecompression::bb_decompressColumn(std::vector<uint8_t>& output_chunk_v, uint8_t* data, std::vector<btrblocks::units::BITMAP>& null_map){

    auto meta = reinterpret_cast<btrblocks::ColumnChunkMeta*>(data);

    // Get a pointer to the beginning of the memory area with the data
    auto input_data = static_cast<const uint8_t*>(meta->data);

    // Decompress bitmap
    uint32_t tuple_count = meta->tuple_count;
    std::unique_ptr<BitmapWrapper> bitmap = bb_getBitmap(data);
    null_map = bitmap->writeBITMAP();

    auto output_chunk = btrblocks::get_data(output_chunk_v, bb_getDecompressedSize(data) + SIMD_EXTRA_BYTES);
    bool requires_copy = false;
    // Decompress data
    switch (meta->type) {
    case btrblocks::ColumnType::INTEGER: {
        // Prepare destination array
        auto destination_array = reinterpret_cast<btrblocks::INTEGER*>(output_chunk);

        // Fetch the scheme from metadata
        auto& scheme = btrblocks::IntegerSchemePicker::MyTypeWrapper::getScheme(meta->compression_type);
        scheme.decompress(destination_array, bitmap.get(), input_data, tuple_count, 0);
        break;
    }
    case btrblocks::ColumnType::DOUBLE: {
        // Prepare destination array
        auto destination_array = reinterpret_cast<btrblocks::DOUBLE*>(output_chunk);

        auto& scheme = btrblocks::DoubleSchemePicker::MyTypeWrapper::getScheme(meta->compression_type);
        scheme.decompress(destination_array, bitmap.get(), input_data, tuple_count, 0);
        break;
    }
    case btrblocks::ColumnType::STRING: {
        auto& scheme = btrblocks::StringSchemePicker::MyTypeWrapper::getScheme(meta->compression_type);
        auto foo = scheme.schemeType();
        requires_copy = scheme.decompressNoCopy(output_chunk, bitmap.get(), input_data, tuple_count, 0);
        break;
    }
    default: {
        throw Generic_Exception("Type " + ConvertTypeToString(meta->type) + " not supported");
    }
    }

    return requires_copy;
}

std::unique_ptr<BitmapWrapper> ChunkDecompression::bb_getBitmap(uint8_t* data){

    auto meta = reinterpret_cast<btrblocks::ColumnChunkMeta*>(data);

    auto type = meta->nullmap_type;
    // Allocate bitset
    auto m_bitset = new boost::dynamic_bitset<>(meta->tuple_count);

    return std::move(std::make_unique<BitmapWrapper>(meta->data + meta->nullmap_offset, type, meta->tuple_count, m_bitset));
}

uint32_t ChunkDecompression::bb_getDecompressedSize(uint8_t* data){

    auto meta = reinterpret_cast<btrblocks::ColumnChunkMeta*>(data);
    
    switch (meta->type) {
        case btrblocks::ColumnType::INTEGER: {
            return sizeof(btrblocks::INTEGER) * meta->tuple_count;
        }
        case btrblocks::ColumnType::DOUBLE: {
            return sizeof(btrblocks::DOUBLE) * meta->tuple_count;
        }
        case btrblocks::ColumnType::STRING: {
            auto& scheme = btrblocks::StringSchemePicker::MyTypeWrapper::getScheme(meta->compression_type);

            auto input_data = static_cast<const uint8_t*>(meta->data);
            std::unique_ptr<BitmapWrapper> bitmapWrapper = bb_getBitmap(data);
            uint32_t size = scheme.getDecompressedSizeNoCopy(input_data, meta->tuple_count, bitmapWrapper.get());
            // TODO The 4096 is temporary until I figure out why FSST is returning
            // bigger numbers
            return size + 8 + 4096;  // +8 because of fsst decompression
        }
        default: {
            throw Generic_Exception("Type " + btrblocks::ConvertTypeToString(meta->type) +
                                    " not supported");
        }
    }
}

bool Utils::is_dict_scheme(SchemeType type){
    if(type == SchemeType::DFOR
        || type == SchemeType::Dict_1toN
        || type == SchemeType::Dict_1to1
        || type == SchemeType::Dict_Sharing){
            return true;
    }
    assert(is_non_dict_scheme(type));
    return false;
}

bool Utils::is_non_dict_scheme(SchemeType type){
    if(type == SchemeType::Numerical
        || type == SchemeType::Equality){
            return true;
    }
    assert(is_dict_scheme(type));
    return false;
}

std::shared_ptr<btrblocks::InputChunk> Utils::get_samples(std::shared_ptr<btrblocks::InputChunk> column, int runs, int run_length){
    
    std::unique_ptr<btrblocks::u8[]> data = nullptr;
    std::unique_ptr<btrblocks::u8[]> nullmap = nullptr;
    size_t size = 0;
    size_t samples_tuple_count =  runs * run_length;
    
    switch(column->type){
        case btrblocks::ColumnType::INTEGER:{

            // less tuples than requested sample
            if(runs * run_length > column->tuple_count){
                size = column->size;
                data = std::make_unique<btrblocks::u8[]>(size);
                auto samples = reinterpret_cast<btrblocks::INTEGER*>(data.get());
                nullmap = std::make_unique<btrblocks::u8[]>(column->tuple_count);

                for(size_t i=0; i<samples_tuple_count; i++){
                    samples[i] = column->integers()[i];
                    nullmap[i] = column->nullmap[i];
                }
            }
            else{
                size = samples_tuple_count * sizeof(btrblocks::INTEGER);
                data = std::make_unique<btrblocks::u8[]>(size);
                auto samples = reinterpret_cast<btrblocks::INTEGER*>(data.get());
                nullmap = std::make_unique<btrblocks::u8[]>(samples_tuple_count);

                int windowSize = samples_tuple_count / runs;
                int i = 0;
                for(int run_i=0; run_i<runs; run_i++){
                    for(int sample_i=0; sample_i<run_length; sample_i++){
                        samples[i] = column->integers()[run_i * windowSize + sample_i];
                        nullmap[i] = column->nullmap[run_i * windowSize + sample_i];
                        i++;
                    }
                }
            }
            break;
        }
        case btrblocks::ColumnType::DOUBLE:{

            // less tuples than requested sample
            if(runs * run_length > column->tuple_count){
                size = column->size;
                data = std::make_unique<btrblocks::u8[]>(size);
                auto samples = reinterpret_cast<btrblocks::DOUBLE*>(data.get());
                nullmap = std::make_unique<btrblocks::u8[]>(column->tuple_count);

                for(size_t i=0; i<samples_tuple_count; i++){
                    samples[i] = column->doubles()[i];
                    nullmap[i] = column->nullmap[i];
                }
            }
            else{
                size = samples_tuple_count * sizeof(btrblocks::DOUBLE);
                data = std::make_unique<btrblocks::u8[]>(size);
                auto samples = reinterpret_cast<btrblocks::DOUBLE*>(data.get());
                nullmap = std::make_unique<btrblocks::u8[]>(samples_tuple_count);

                int windowSize = samples_tuple_count / runs;
                int i = 0;
                for(int run_i=0; run_i<runs; run_i++){
                    for(int sample_i=0; sample_i<run_length; sample_i++){
                        samples[i] = column->doubles()[run_i * windowSize + sample_i];
                        nullmap[i] = column->nullmap[run_i * windowSize + sample_i];
                        i++;
                    }
                }
            }
            break;
        }
        case btrblocks::ColumnType::STRING:{

            // 1. loop through slots, copy and get run lengths
            // 2. allocate memory based on slots and lengths
            // 3. loop through string runs, memcpy


            // less tuples than requested sample
            if(runs * run_length > column->tuple_count){
                nullmap = std::make_unique<btrblocks::u8[]>(column->tuple_count);
                memcpy(nullmap.get(), column->nullmap.get(), column->tuple_count);

                size = column->size;
                data = std::make_unique<btrblocks::u8[]>(size);
                memcpy(data.get(), column->data.get(), column->size);
            }
            else{
                nullmap = std::make_unique<btrblocks::u8[]>(samples_tuple_count);

                size_t slots_size = sizeof(btrblocks::StringArrayViewer::Slot) * (runs * run_length + 1);
                auto slots_data = std::make_unique<btrblocks::u8[]>(slots_size);
                auto slot_writer = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(slots_data.get());
                slot_writer[0].offset = slots_size; 

                auto source_slot_reader = reinterpret_cast<btrblocks::StringArrayViewer::Slot*>(column->data.get());

                int windowSize = samples_tuple_count / runs;
                int i = 0;

                // set slot offsets
                for(int run_i=0; run_i<runs; run_i++){
                    for(int sample_i=0; sample_i<run_length; sample_i++){
                        slot_writer[i+1].offset = slot_writer[i].offset + source_slot_reader[run_i * windowSize + sample_i + 1].offset - source_slot_reader[run_i * windowSize + sample_i].offset;
                        nullmap[i] = column->nullmap[run_i * windowSize + sample_i];
                        i++;
                    }
                }

                // copy strings
                size = slot_writer[runs * run_length].offset; // includes slots and strings size
                auto strings_data = std::make_unique<btrblocks::u8[]>(size);

                memcpy(strings_data.get(), slots_data.get(), slots_size);

                for(int run_i=0; run_i<runs; run_i++){
                    auto dest = strings_data.get() + slot_writer[run_i * run_length].offset;
                    auto src = column->data.get() + source_slot_reader[run_i * windowSize].offset;
                    auto cpy_size = source_slot_reader[run_i * windowSize + run_length].offset - source_slot_reader[run_i * windowSize].offset;
                    memcpy(dest, src, cpy_size);
                }
                data = std::move(strings_data);

                // std::cout << btrblocks::StringArrayViewer(column->data.get())(0) << ":" << btrblocks::StringArrayViewer(data.get())(0) << std::endl;
                
            }

            break;
        }
    }
    return std::make_shared<btrblocks::InputChunk>(std::move(data), std::move(nullmap), column->type, samples_tuple_count, size);
}


std::string Utils::bb_ColumnType_to_string(btrblocks::units::ColumnType column_type){
    switch(column_type){
         case btrblocks::units::ColumnType::INTEGER:{
            return "integer";
        } 
        case btrblocks::units::ColumnType::DOUBLE:{
            return "double";
        } 
        case btrblocks::units::ColumnType::STRING:{
            return "string";
        } 
    }
}

std::string Utils::scheme_to_string(SchemeType scheme){
    switch(scheme){
        case SchemeType::BB:{return "BB";}
        case SchemeType::Dict_1to1:{return "Dict_1to1";}
        case SchemeType::Single_Dictionary:{return "Single_Dictionary";}
        case SchemeType::Equality:{return "Equality";}
        case SchemeType::Numerical:{return "Numerical";}
        case SchemeType::DFOR:{return "DFOR";}
        case SchemeType::Dict_1toN:{return "Dict_1toN";}
        case SchemeType::Dict_Sharing:{return "Dict_Sharing";}
        default: return "SchemeType unknown";
    }
}

std::string Utils::bb_scheme_to_string(SchemeType C3_scheme, btrblocks::units::ColumnType column_type, int scheme_id){
    // taken from SchemeType.hpp
    std::vector<std::string> int_schemes = {
        "UNCOMPRESSED",
        "ONE_VALUE",
        "DICT",
        "RLE",
        "PFOR",
        "BP",
    };
    std::vector<std::string> double_schemes = {
        "UNCOMPRESSED",
        "ONE_VALUE",
        "DICT",
        "RLE",
        "FREQUENCY",
        "PSEUDODECIMAL",
    };
    std::vector<std::string> string_schemes = {
        "UNCOMPRESSED",
        "ONE_VALUE",
        "DICT",
        "FSST",
    };
    
    if(scheme_id == -1){
        return "-1";
    }

    switch(C3_scheme){
        case SchemeType::Equality:{
            switch(column_type){
                case btrblocks::units::ColumnType::INTEGER:{
                    assert(scheme_id < int_schemes.size());
                    return int_schemes[scheme_id];
                } 
                case btrblocks::units::ColumnType::DOUBLE:{
                    assert(scheme_id < double_schemes.size());
                    return double_schemes[scheme_id];
                } 
                case btrblocks::units::ColumnType::STRING:{
                    assert(scheme_id < string_schemes.size());
                    return string_schemes[scheme_id];
                }
                default: std::cerr << "bb_scheme_to_string error" << std::endl; return "-99";
            }
        }
        case SchemeType::Dict_1to1:{
            // both source and target are int encoded
            assert(scheme_id < int_schemes.size());
            return int_schemes[scheme_id];
        }
        case SchemeType::Numerical:{
            // both source and target are int encoded
            assert(scheme_id < int_schemes.size());
            return int_schemes[scheme_id];
        }
        case SchemeType::DFOR:{
            // both source and target are int encoded
            assert(scheme_id < int_schemes.size());
            return int_schemes[scheme_id];
        }
        case SchemeType::Dict_1toN:{
            // both source and target are int encoded
            assert(scheme_id < int_schemes.size());
            return int_schemes[scheme_id];
        }
        case SchemeType::Dict_Sharing:{
            switch(column_type){
                case btrblocks::units::ColumnType::INTEGER:{
                    assert(scheme_id < int_schemes.size());
                    return int_schemes[scheme_id];
                } 
                case btrblocks::units::ColumnType::DOUBLE:{
                    assert(scheme_id < double_schemes.size());
                    return double_schemes[scheme_id];
                } 
                case btrblocks::units::ColumnType::STRING:{
                    assert(scheme_id < string_schemes.size());
                    return string_schemes[scheme_id];
                }
                default: std::cerr << "bb_scheme_to_string error" << std::endl; return "-99";
            }
        }
        default: std::cerr << "bb_scheme_to_string error" << std::endl; return "-99";
    }
}

}
