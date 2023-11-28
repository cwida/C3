#pragma once
#include "storage/InputChunk.hpp"
#include "c3/storage/Datablock.hpp"
#include "c3/Utils.hpp"
#include "c3/CompressionSchemes.hpp"

namespace c3{
namespace single_col{

class Dictionary {
    public:
    // static double expectedDictionaryCompressionRatio(const std::shared_ptr<btrblocks::InputChunk> column, std::shared_ptr<ColumnStats> columnStats);

    static std::shared_ptr<btrblocks::InputChunk> apply_scheme(std::shared_ptr<btrblocks::InputChunk> source_column, DictMeta* dict_meta,  std::shared_ptr<ColumnStats> column_stats, size_t& compressed_dict_size, size_t& uncompressed_dict_size);
    
    static std::shared_ptr<btrblocks::InputChunk> apply_scheme_dict_sharing(std::shared_ptr<btrblocks::InputChunk> source_column, DictMeta* dict_meta, std::shared_ptr<SharedDict> shared_dict, std::unique_ptr<uint8_t[]> dict_buffer, int dict_size, std::unique_ptr<uint8_t[]> dict_nullmap_buffer, size_t& compressed_dict_size);

    static std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> decompress(const std::vector<uint8_t>& column_codes, DictMeta* dict_meta, const uint32_t tuple_count, int32_t original_col_size, std::vector<btrblocks::BITMAP> col_nullmap, bool& requires_copy, std::vector<uint8_t> decompressed_dict_values = {}, std::vector<uint8_t> decompressed_dict_nullmap = {});

};

}
}