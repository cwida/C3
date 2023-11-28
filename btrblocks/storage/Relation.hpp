#pragma once
#include "Column.hpp"
#include "c3/storage/RowGroup.hpp"
#include "common/Units.hpp"
// -------------------------------------------------------------------------------------
namespace btrblocks {

class Chunk;
class InputChunk;

enum class SplitStrategy : u8 { SEQUENTIAL, RANDOM };
using Range = tuple<u64, u64>;

class Relation {
public:
	Relation();

public:
	[[nodiscard]] vector<Range> getRanges(btrblocks::SplitStrategy strategy, u32 max_chunk_count) const;
	[[nodiscard]] Chunk         getChunk(const vector<Range>& ranges, SIZE chunk_i) const;
	[[nodiscard]] InputChunk    getInputChunk(const Range& range, SIZE chunk_i, u32 column) const;

	void                                       addColumn(Column&& column);
	void                                       addColumn(const string& column_file_path); // TODO string_view
	std::shared_ptr<c3::RowGroup> getRowGroup(btrblocks::Range range);
	std::vector<std::vector<InputChunk>> getInputChunks(std::vector<btrblocks::Range> ranges);

public:
	string         name;
	u64            tuple_count;
	vector<Column> columns;

private:
	void verifyTupleCounts();
};
} // namespace btrblocks
