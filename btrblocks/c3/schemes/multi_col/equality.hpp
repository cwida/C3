#pragma once
#include "storage/InputChunk.hpp"
#include "c3/CompressionSchemes.hpp"
#include "c3/Utils.hpp"
#include "c3/storage/Datablock.hpp"

namespace c3{
namespace multi_col{

class Equality {
    public:
        static bool skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j);

        static std::pair<double,double> expectedCompressionRatio(std::shared_ptr<RowGroup> row_group, const std::shared_ptr<btrblocks::InputChunk> source_column, const std::shared_ptr<btrblocks::InputChunk> target_column, int& estimated_exception_size);

        static std::vector<std::vector<uint8_t>> apply_scheme(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, double& exception_ratio, const int& source_idx, const int& target_idx, const uint8_t& force_source_scheme, std::shared_ptr<c3::EqualityCompressionScheme> scheme, bool skip_source_compression, bool previous_scheme_uses_source, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes);

        static std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> decompress(const std::vector<uint8_t>& source_column, c3::C3Chunk* targetChunk, size_t tuple_count, bool source_requires_copy);
};

}
}
