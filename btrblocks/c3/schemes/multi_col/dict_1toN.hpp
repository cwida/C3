#pragma once
#include "storage/InputChunk.hpp"
#include "c3/Utils.hpp"
#include "c3/CompressionSchemes.hpp"
#include "c3/storage/Datablock.hpp"

namespace c3{
namespace multi_col{

class Dictionary_1toN {
    public:

        static bool skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j);

        // dict for source, dict for target, dict for cross-column, original values
        static double expectedCompressionRatio(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::vector<int>& samples, std::shared_ptr<ColumnStats> sourceStats, std::shared_ptr<ColumnStats> targetStats, int& estimated_target_dict_size, int& estimated_target_compressed_codes_size, int& estimated_offsets_size, std::shared_ptr<RowGroup> row_group);

        static std::vector<std::vector<uint8_t>> apply_scheme(std::shared_ptr<btrblocks::InputChunk>& source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::shared_ptr<Dict_1toN_CompressionScheme> scheme, bool skip_source_encoding, bool last_source_column_scheme, const int& source_idx, const int& target_idx, std::vector<uint8_t>* sourceChunkVec, std::shared_ptr<ColumnStats> sourceBBSchemes, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes, std::shared_ptr<RowGroup> row_group);

        static std::vector<uint8_t> decompress(const std::vector<uint8_t>& source_column, const std::vector<uint8_t>& target_column, C3Chunk* c3_meta, const std::vector<uint8_t>& source_nullmap, const std::vector<uint8_t>& target_nullmap, uint32_t tuple_count, bool& target_requires_copy);

};

}
}
