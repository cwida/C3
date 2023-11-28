#pragma once
#include "storage/InputChunk.hpp"
#include "Utils.hpp"
#include <map>

namespace c3{



class CompressionScheme{
    public:
        CompressionScheme(SchemeType type, std::vector<size_t> columns, size_t num_columns, int c3_saved_bytes_source, int c3_saved_bytes_target);
        CompressionScheme(SchemeType type);
        virtual std::string to_string(){return "";};
        SchemeType type;

        // estimated bytes saved by using C3 scheme instead of btrblocks
        // int c3_saved_bytes; 
        int estimated_bytes_saved_source = -1;
        int estimated_bytes_saved_target = -1;

        std::vector<size_t> columns; // column indexes
        size_t num_columns; // 0 = not applicable
        bool scheme_applied = false;
            
        // log:
        static std::string print_log_header();
        static std::string print_empty_log();
        std::string print_log(std::vector<btrblocks::units::ColumnType>& column_types);
        double bb_source_ecr = -1;
        double bb_target_ecr = -1;
        double C3_source_ecr = -1;
        double C3_target_ecr = -1;
        int source_bb_scheme = -1;
        int target_bb_scheme = -1;
        int source_unique_count = -1;
        int target_unique_count = -1;
        int estimated_exception_count = -1;
        int real_exception_count = -1;
        int estimated_exception_size = -1;
        int real_exception_size = -1;
        double exception_compression_ratio = -1;
        int estimated_source_target_dict_size = -1; //dict
        int real_source_target_dict_size = -1; //dict
        double source_target_dict_compression_ratio = -1; //dict
        int estimated_target_dict_size = -1; //dict
        int real_target_dict_size = -1; //dict
        double target_dict_compression_ratio = -1; //dict
        double pearson_corr_coef = -1; // numerical
        int source_column_min = -1;
        int source_column_max = -1;
        int target_column_min = -1;
        int target_column_max = -1;
        int source_null_count = -1;
        int target_null_count = -1;
        int estimated_target_compressed_codes_size = -1; // numeric, dfor, 1toNDict
        int real_target_compressed_codes_size = -1;
        int estimated_offsets_size = -1;
        int real_offsets_size = -1;
        int estimated_target_nullmap_size = -1;
        int real_target_nullmap_size = -1;
        int source_nullmap_size = -1;
        int target_nullmap_size = -1;
};

class Dictionary_1to1_CompressionScheme: public CompressionScheme{
    public:
        Dictionary_1to1_CompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target);
        Dictionary_1to1_CompressionScheme();
        std::string to_string();
        std::vector<uint8_t> sourceChunk;
};

class Dict_1toN_CompressionScheme: public CompressionScheme{
    public:
        Dict_1toN_CompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target);
        std::string to_string();
        std::vector<uint8_t> sourceChunk;
};

class EqualityCompressionScheme: public CompressionScheme{
    public:
        EqualityCompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target);
        EqualityCompressionScheme();
        std::string to_string();
};

class NumericalCompressionScheme: public CompressionScheme{
    public:
        NumericalCompressionScheme(size_t source_column, size_t target_column, float slope, float intercept, int c3_saved_bytes_source, int c3_saved_bytes_target);
        float slope;
        float intercept;
        std::string to_string();
};

class DForCompressionScheme: public CompressionScheme{
    public:
        DForCompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target);
        std::string to_string();
        std::vector<uint8_t> sourceChunk;
};

class DictSharingCompressionScheme: public CompressionScheme{
    public:
        DictSharingCompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target);
        std::string to_string();
        std::vector<uint8_t> sourceChunk;
};

// class OneHotCompressionScheme: public CompressionScheme{
//     public:
//         OneHotCompressionScheme(std::vector<size_t> columns, int c3_saved_bytes);
//         std::string to_string();
// };



}
