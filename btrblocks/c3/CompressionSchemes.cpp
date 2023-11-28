#include "CompressionSchemes.hpp"
#include "c3/Utils.hpp"

namespace c3{

std::string CompressionScheme::print_log_header(){
    return std::string("C3scheme|") + 
        "source_column|" + 
        "target_column|" + 
        "bb_source_ecr|" +  
        "bb_target_ecr|" +  
        "c3_source_ecr|" +  
        "c3_target_ecr|" +  
        "estimated_bytes_saved_source|" + 
        "estimated_bytes_saved_target|" + 
        "source_bb_scheme|" + 
        "target_bb_scheme|" + 
        "source_unique_count|" + 
        "target_unique_count|" + 
        "estimated_exception_count|" + 
        "real_exception_count|" + 
        "estimated_exception_size|" +
        "real_exception_size|" +
        "exception_compression_ratio|" + 
        "estimated_source_target_dict_size|" + 
        "real_source_target_dict_size|" + 
        "source_target_dict_compression_ratio|" + 
        "estimated_target_dict_size|" + 
        "real_target_dict_size|" + 
        "target_dict_compression_ratio|" + 
        "pearson_corr_coef|" + 
        "source_column_min|" + 
        "source_column_max|" +
        "target_column_min|" + 
        "target_column_max|" +
        "source_null_count|" +
        "target_null_count|" +
        "estimated_target_compressed_codes_size|" +
        "real_target_compressed_codes_size|" +
        "estimated_offsets_size|" +
        "real_offsets_size|" + 
        "estimated_target_nullmap_size|" +
        "real_target_nullmap_size";
}

std::string CompressionScheme::print_empty_log(){
    return "None||||||||||||||||||||||||||||||||||||";
}

std::string CompressionScheme::print_log(std::vector<btrblocks::units::ColumnType>& column_types){
    return 
        Utils::scheme_to_string(type) + "|" +
        std::to_string(columns[0]) + "|" +
        std::to_string(columns[1]) + "|" +
        std::to_string(bb_source_ecr) + "|" +
        std::to_string(bb_target_ecr) + "|" +
        std::to_string(C3_source_ecr) + "|" +
        std::to_string(C3_target_ecr) + "|" +
        std::to_string(estimated_bytes_saved_source) + "|" +
        std::to_string(estimated_bytes_saved_target) + "|" +
        Utils::bb_scheme_to_string(type, column_types[columns[0]], source_bb_scheme) + "|" +
        Utils::bb_scheme_to_string(type, column_types[columns[1]], target_bb_scheme) + "|" +
        std::to_string(source_unique_count) + "|" +
        std::to_string(target_unique_count) + "|" +
        std::to_string(estimated_exception_count) + "|" +
        std::to_string(real_exception_count) + "|" +
        std::to_string(estimated_exception_size) + "|" +
        std::to_string(real_exception_size) + "|" +
        std::to_string(exception_compression_ratio) + "|" +
        std::to_string(estimated_source_target_dict_size) + "|" +
        std::to_string(real_source_target_dict_size) + "|" +
        std::to_string(source_target_dict_compression_ratio) + "|" +
        std::to_string(estimated_target_dict_size) + "|" +
        std::to_string(real_target_dict_size) + "|" +
        std::to_string(target_dict_compression_ratio) + "|" +
        std::to_string(pearson_corr_coef) + "|" +
        std::to_string(source_column_min) + "|" +
        std::to_string(source_column_max) + "|" +
        std::to_string(target_column_min) + "|" +
        std::to_string(target_column_max) + "|" +
        std::to_string(source_null_count) + "|" +
        std::to_string(target_null_count) + "|" +
        std::to_string(estimated_target_compressed_codes_size) + "|" +
        std::to_string(real_target_compressed_codes_size) + "|" +
        std::to_string(estimated_offsets_size) + "|" +
        std::to_string(real_offsets_size) + "|" + 
        std::to_string(estimated_target_nullmap_size) + "|" + 
    	std::to_string(real_target_nullmap_size) + "|";
}

CompressionScheme::CompressionScheme(SchemeType type, std::vector<size_t> columns, size_t num_columns, int c3_saved_bytes_source, int c3_saved_bytes_target)
:type(type), columns(columns), num_columns(num_columns), estimated_bytes_saved_source(c3_saved_bytes_source), estimated_bytes_saved_target(c3_saved_bytes_target)
{}

CompressionScheme::CompressionScheme(SchemeType type)
:type(type)
{}

Dictionary_1to1_CompressionScheme::Dictionary_1to1_CompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target)
:CompressionScheme(SchemeType::Dict_1to1, {source_column, target_column}, 2, c3_saved_bytes_source, c3_saved_bytes_target)
{}

Dictionary_1to1_CompressionScheme::Dictionary_1to1_CompressionScheme()
:CompressionScheme(SchemeType::Dict_1to1)
{}

std::string Dictionary_1to1_CompressionScheme::to_string(){
    return "Dict_1to1: src " + std::to_string(columns[0]) + " and target " + std::to_string(columns[1]) + "; bytes saved source: " + std::to_string(estimated_bytes_saved_source) + "; bytes saved target: " + std::to_string(estimated_bytes_saved_target); 
}

Dict_1toN_CompressionScheme::Dict_1toN_CompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target)
:CompressionScheme(SchemeType::Dict_1toN, {source_column, target_column}, 2, c3_saved_bytes_source, c3_saved_bytes_target)
{}

std::string Dict_1toN_CompressionScheme::to_string(){
    return "Dict_1toN: src " + std::to_string(columns[0]) + " and target " + std::to_string(columns[1]) + "; bytes saved source: " + std::to_string(estimated_bytes_saved_source) + "; bytes saved target: " + std::to_string(estimated_bytes_saved_target); 
}

EqualityCompressionScheme::EqualityCompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target)
:CompressionScheme(SchemeType::Equality, {source_column, target_column}, 2, c3_saved_bytes_source, c3_saved_bytes_target)
{}

EqualityCompressionScheme::EqualityCompressionScheme()
:CompressionScheme(SchemeType::Equality)
{}

std::string EqualityCompressionScheme::to_string(){
    return "Equal: src " + std::to_string(columns[0]) + " and target " + std::to_string(columns[1]) + "; bytes saved source: " + std::to_string(estimated_bytes_saved_source) + "; bytes saved target: " + std::to_string(estimated_bytes_saved_target);
}

// size_t EqualityCompressionScheme::metadata_size(int i){
//     if(i==1){
//         return exceptions.size();
//     }
//     else{
//         return 0;
//     }
// }

NumericalCompressionScheme::NumericalCompressionScheme(size_t source_column, size_t target_column, float slope, float intercept, int c3_saved_bytes_source, int c3_saved_bytes_target)
:CompressionScheme(SchemeType::Numerical, {source_column, target_column}, 2, c3_saved_bytes_source, c3_saved_bytes_target), slope(slope), intercept(intercept)
{}

std::string NumericalCompressionScheme::to_string(){
    return "Numerical: src " + std::to_string(columns[0]) + " and target " + std::to_string(columns[1]) + "; bytes saved source: " + std::to_string(estimated_bytes_saved_source) + "; bytes saved target: " + std::to_string(estimated_bytes_saved_target);
}

DForCompressionScheme::DForCompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target)
:CompressionScheme(SchemeType::DFOR, {source_column, target_column}, 2, c3_saved_bytes_source, c3_saved_bytes_target)
{}

std::string DForCompressionScheme::to_string(){
    return "DFOR: src " + std::to_string(columns[0]) + " and target " + std::to_string(columns[1]) + "; bytes saved source: " + std::to_string(estimated_bytes_saved_source) + "; bytes saved target: " + std::to_string(estimated_bytes_saved_target);
}

DictSharingCompressionScheme::DictSharingCompressionScheme(size_t source_column, size_t target_column, int c3_saved_bytes_source, int c3_saved_bytes_target)
:CompressionScheme(SchemeType::Dict_Sharing, {source_column, target_column}, 2, c3_saved_bytes_source, c3_saved_bytes_target)
{}

std::string DictSharingCompressionScheme::to_string(){
    return "Dict Sharing: src " + std::to_string(columns[0]) + " and target " + std::to_string(columns[1]) + "; bytes saved source: " + std::to_string(estimated_bytes_saved_source) + "; bytes saved target: " + std::to_string(estimated_bytes_saved_target);
}


// OneHotCompressionScheme::OneHotCompressionScheme(std::vector<size_t> columns, int c3_saved_bytes)
// :CompressionScheme(SchemeType::OneHot, columns, 0, c3_saved_bytes)
// {}


// std::string OneHotCompressionScheme::to_string(){
//     std::string s = "(";
//     for(const auto& c: columns){
//         s += std::to_string(c) + ",";
//     }
//     s += ")";
//     return "OneHot group: " + s;
// }

}
