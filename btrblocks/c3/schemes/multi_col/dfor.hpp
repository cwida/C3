#pragma once
#include "storage/InputChunk.hpp"
#include "c3/CompressionSchemes.hpp"
#include "c3/Utils.hpp"
#include "c3/storage/Datablock.hpp"

namespace c3{
namespace multi_col{

// Dictionary Frame of References
class DFOR {
    public:
        static bool skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j);

        // dict for source, dict for target, dict for cross-column, original values
        static double expectedCompressionRatio(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, std::vector<int>& samples, std::shared_ptr<ColumnStats> sourceStats, std::shared_ptr<ColumnStats> targetStats, int& estimated_target_compressed_codes_size, int& estimated_source_target_dict_size);

        static std::vector<std::vector<uint8_t>> apply_scheme(std::shared_ptr<btrblocks::InputChunk>& source_column, std::shared_ptr<btrblocks::InputChunk> target_column, int source_idx, int target_idx, bool skip_source_encoding, bool last_source_column_scheme, std::shared_ptr<DForCompressionScheme> scheme, int source_unique_count, std::vector<uint8_t>* sourceChunkVec, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes);

        static std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> decompress(const std::vector<uint8_t>& source_column, C3Chunk* c3_meta, const std::vector<uint8_t>& source_nullmap, uint32_t tuple_count, bool& requires_copy);

};

}
}
