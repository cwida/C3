#pragma once

#include "Column.hpp"
#include "StringArrayViewer.hpp"
#include "StringPointerArrayViewer.hpp"
#include "extern/RoaringBitmap.hpp"

namespace btrblocks {

class InputChunk {
public:
	InputChunk(unique_ptr<u8[]>&& data, unique_ptr<BITMAP[]>&& bitmap, ColumnType type, u64 tuple_count, SIZE size);

	bool compareContents(u8*                        their_data,
	                     const std::vector<BITMAP>& their_bitmap,
	                     u64                        their_tuple_count,
	                     bool                       requires_copy,
                         int 						col_idx = -1,
						 int						rg_idx = -1) const;

	INTEGER*          integers() { return reinterpret_cast<INTEGER*>(data.get()); };
	DOUBLE*           doubles() { return reinterpret_cast<DOUBLE*>(data.get()); };
	StringArrayViewer strings() { return StringArrayViewer(data.get()); };
	unique_ptr<BITMAP[]> get_nullmap_copy();

public:
	unique_ptr<u8[]>     data;
	unique_ptr<BITMAP[]> nullmap;
	ColumnType           type;
	SIZE                 size;
	const u64            tuple_count;
};

} // namespace btrblocks