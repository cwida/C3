#pragma once
#include "storage/InputChunk.hpp"
#include "c3/CompressionSchemes.hpp"
#include "c3/Utils.hpp"
#include "c3/storage/Datablock.hpp"

namespace c3{
namespace multi_col{

class Numerical {
    public:
        static bool skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j);

        static double expectedCompressionRatio(std::shared_ptr<RowGroup> row_group, const std::shared_ptr<btrblocks::InputChunk> source_column, const std::shared_ptr<btrblocks::InputChunk> target_column, int target_null_counter, float& slope, float& intercept, double& pearson_corr_coef, int& estimated_target_compressed_codes_size);

        static std::vector<std::vector<uint8_t>> apply_scheme(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, int source_idx, int target_idx, const uint8_t& force_source_scheme, std::shared_ptr<c3::NumericalCompressionScheme> scheme, bool skip_source_compression, bool previous_scheme_uses_source, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes);

        static std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> decompress(const std::vector<uint8_t>& source_column, c3::C3Chunk* targetChunk, const std::vector<btrblocks::BITMAP>& source_null_map, size_t tuple_count, bool source_requires_copy);
};

}
}
