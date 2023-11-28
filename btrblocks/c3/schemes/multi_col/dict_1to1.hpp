#pragma once
#include "storage/InputChunk.hpp"
#include "c3/CompressionSchemes.hpp"
#include "c3/Utils.hpp"
#include "c3/storage/Datablock.hpp"

namespace c3{
namespace multi_col{

class Dictionary_1to1 {
    public:
        static bool skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j);

        // dict for source, dict for target, dict for cross-column, original values
        static std::pair<double, double> expectedCompressionRatio(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::vector<int>& samples, std::shared_ptr<ColumnStats> sourceStats, std::shared_ptr<ColumnStats> targetStats, int& estimated_source_target_dict_size, int& estimated_target_dict_size, int& estimated_exception_size);

        static std::vector<std::vector<uint8_t>> apply_scheme(std::shared_ptr<btrblocks::InputChunk>& source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::shared_ptr<Dictionary_1to1_CompressionScheme> scheme, bool skip_source_encoding, bool last_source_column_scheme, double& exception_ratio, const int& source_idx, const int& target_idx, std::vector<uint8_t>* sourceChunkVec, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes, std::shared_ptr<RowGroup> row_group);

        static std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> decompress(const std::vector<uint8_t>& source_column, C3Chunk* c3_meta, const std::vector<uint8_t>& source_nullmap, uint32_t tuple_count, bool& requires_copy);

};

}
}
