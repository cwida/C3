#pragma once
#include "scheme/CompressionScheme.hpp"
#include "common/Units.hpp"
#include "storage/InputChunk.hpp"

struct C3Config{
    //---------- GENERAL CONFIG ----------//
    int       C3_WINDOW_SIZE                  = 100;
    double    BYTES_SAVED_MARGIN              = 0.01;
    bool      C3_GRAPH_SHARE_SOURCE_NODES     = true;
    bool      REVERT_BB_IF_C3_BAD             = true; // changed
    bool      USE_PRUNING_RULES               = true; // changed
    bool      FINALIZE_GRAPH_RESORT_EDGES     = false;
    bool      USE_RANDOM_GENERATOR_SAMPLES    = false;
    bool      IGNORE_BB_ECR                   = false; // changed

    // 327	655	3276	6553	16384	32768	49152	65536
    int C3_SAMPLE_NUM_RUNS    = 655;
    int C3_SAMPLE_RUN_SIZE    = 1;

    //---------- SCHEME CONFIG ----------//
    bool ENABLE_EQUALITY      = true;
    bool ENABLE_DICT_1TO1     = true;
    bool ENABLE_DICT_1TON     = true;
    bool ENABLE_NUMERICAL     = true;
    bool ENABLE_DFOR          = true;
    bool ENABLE_DICT_SHARING  = true;

    double DICT_EXCEPTION_RATIO_THRESHOLD     = 0.10;
    double EQUALITY_EXCEPTION_RATIO_THRESHOLD = 0.10;

    // 255 = autoscheme(); 0 = Uncompressed; 5 = BP; 4 = PFOR
    int DICTIONARY_COMPRESSION_SCHEME         = 0; 
    int DICTIONARY_CODES_COMPRESSION_SCHEME   = 5;
    int EXCEPTION_COMPRESSION_SCHEME          = 0;
    int DFOR_CODES_COMPRESSION_SCHEME         = 5;
    int NUMERICAL_CODES_COMPRESSION_SCHEME    = 5;

    double DICTIONARY_CASCADE_COMPRESSION_ESTIMATE = 1;
    double EXCEPTION_CASCADE_COMPRESSION_ESTIMATE = 1;

    //---------- MULTI ROW GROUP CONFIG ----------//
    // 0 = none; 1 = share finalized; 2 = share graph schemes
    int SHARE_ROW_GROUP_SCHEME = 0;

    C3Config(){
        DICTIONARY_CASCADE_COMPRESSION_ESTIMATE = DICTIONARY_COMPRESSION_SCHEME == 255 ? 2 : 1; 
        EXCEPTION_CASCADE_COMPRESSION_ESTIMATE = EXCEPTION_COMPRESSION_SCHEME == 255 ? 2 : 1; 
    }
};

extern struct C3Config config;

namespace c3{

enum class ColumnStatus: uint8_t{
    None = 0,
    Ignore = 1,
    AssignedC3Scheme = 2,
    Compressed = 3,
};
    
enum class SchemeType: uint8_t{
    BB = 0,
    Dict_1to1 = 1,
    Single_Dictionary = 2,
    Equality = 3,
    Numerical = 4,
    DFOR = 5,
    Dict_1toN = 6,
    Dict_Sharing = 7,
};
    
enum class RowGroupsShareScheme: uint8_t{
    None = 0,
    ShareFinalized = 1,
    ShareGraph = 2
};

class SharedDict{
    public:
        std::map<int,std::pair<uint32_t,uint32_t>> intMap;
        std::map<double,std::pair<uint32_t,uint32_t>> doubleMap;
        std::map<std::string_view,uint32_t> stringMap;
        btrblocks::ColumnType type;

        SharedDict(std::map<int,std::pair<uint32_t,uint32_t>> intMap)
        :intMap(intMap), type(btrblocks::ColumnType::INTEGER){}
        
        SharedDict(std::map<double,std::pair<uint32_t,uint32_t>> doubleMap)
        :doubleMap(doubleMap), type(btrblocks::ColumnType::DOUBLE){}
        
        SharedDict(std::map<std::string_view,uint32_t> stringMap)
        :stringMap(stringMap), type(btrblocks::ColumnType::STRING){}
};

class ColumnStats {
    public:

        ColumnStats(btrblocks::ColumnType type, std::shared_ptr<btrblocks::SInteger32Stats> stats, std::vector<std::pair<uint8_t, double>> scr, int uncompressed_size=-1, int compressed_nullmap_size=-1);
        ColumnStats(btrblocks::ColumnType type, std::shared_ptr<btrblocks::DoubleStats> stats, std::vector<std::pair<uint8_t, double>> scr, int uncompressed_size=-1, int compressed_nullmap_size=-1);
        ColumnStats(btrblocks::ColumnType type, std::shared_ptr<btrblocks::StringStats> stats, std::vector<std::pair<uint8_t, double>> scr, int uncompressed_size=-1, int compressed_nullmap_size=-1);

        btrblocks::ColumnType type;
        std::shared_ptr<btrblocks::SInteger32Stats> intStats;
        std::shared_ptr<btrblocks::DoubleStats> doubleStats;
        std::shared_ptr<btrblocks::StringStats> stringStats;
        std::vector<std::pair<uint8_t, double>> scheme_compression_ratios;
        int uncompressed_size;
        int compressed_nullmap_size;

        std::pair<uint8_t, double> get_best_scheme() const;
        double get_dict_compression_ratio() const;
        size_t get_unique_count() const;
        size_t get_null_count() const;
        size_t get_original_chunk_size() const;
        void* get_stats();
        size_t get_BB_compressed_size() const;
};

class Utils{
    public:
        static int lemire_128_bitpack_estimated_compressed_size(std::vector<int> codes, int tuple_count, std::vector<int> samples){            
            // number_of_blocks = tuple_count / 128
            // block_metadata = number_of_blocks * sizeof(uint32_t)
            // assign codes to 32 mini block, get max code per block
            
            size_t compressed_size = 0;
            int block_count = tuple_count / 128;
            compressed_size += block_count * sizeof(uint32_t); // each block has 4x8 bits storing bit count for each mini block

            //
            double avg_bits = 0;
            int mini_block_iterator = 0;
            int mini_block_current_max = 0;
            int mini_block_count = 0;

            assert(codes.size() == samples.size());
            for(int i=0; i<samples.size(); i++){
                if(samples[i] / 32 > mini_block_iterator){
                    compressed_size += mini_block_current_max == 0 ? 0 : sizeof(uint32_t) * (std::floor(std::log2(mini_block_current_max)) + 1);
                    avg_bits += mini_block_current_max == 0 ? 0 : std::floor(std::log2(mini_block_current_max)) + 1;
                    mini_block_count++;
                    mini_block_iterator = samples[i] / 32;
                    mini_block_current_max = codes[i];
                }
                else{
                    mini_block_current_max = std::max(mini_block_current_max, codes[i]);
                }
            }
            avg_bits /= mini_block_count;

            compressed_size += (block_count * 4 - mini_block_count) * sizeof(uint32_t) * std::ceil(avg_bits+1);
            // compressed_size += block_count * 4 * sizeof(uint32_t) * std::ceil(avg_bits);

            return compressed_size;
        };

        static bool is_dict_scheme(SchemeType type);
        static bool is_non_dict_scheme(SchemeType type);
        
        static std::shared_ptr<btrblocks::InputChunk> get_samples(std::shared_ptr<btrblocks::InputChunk> column, int runs, int run_length);
        static std::string scheme_to_string(SchemeType scheme);

        static std::string bb_ColumnType_to_string(btrblocks::units::ColumnType column_type);
        static std::string bb_scheme_to_string(SchemeType C3_scheme, btrblocks::units::ColumnType column_type, int scheme_id);
};

class ChunkDecompression{
    public:
        static uint32_t bb_getDecompressedSize(uint8_t* data);
        static std::unique_ptr<BitmapWrapper> bb_getBitmap(uint8_t* data);
        static bool bb_decompressColumn(std::vector<uint8_t>& output_chunk_v, uint8_t* data, std::vector<btrblocks::units::BITMAP>& null_map);
        static uint32_t bb_getTupleCount(uint8_t* data);
};

}
