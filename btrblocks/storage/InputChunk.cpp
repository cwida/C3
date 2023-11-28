#include "InputChunk.hpp"
#include <iomanip>

namespace btrblocks {

InputChunk::InputChunk(unique_ptr<u8[]>&& data,
                       unique_ptr<BITMAP[]>&& bitmap,
                       ColumnType type,
                       u64 tuple_count,
                       SIZE size)
    : data(std::move(data)),
      nullmap(std::move(bitmap)),
      type(type),
      size(size),
      tuple_count(tuple_count) {}

bool InputChunk::compareContents(u8* their_data,
                                 const std::vector<BITMAP>& their_bitmap,
                                 u64 their_tuple_count,
                                 bool requires_copy,
                                 int col_idx,
                                 int rg_idx) const {
    if (their_tuple_count != this->tuple_count) {
        std::cerr << "row group " << rg_idx << ", col " << col_idx << ": Tuple count is not equal. Expected: " << this->tuple_count
                << ". Got: " << their_tuple_count << std::endl;
        return false;
    }

    for (u64 idx = 0; idx < their_tuple_count; idx++) {
        if (this->nullmap[idx] != their_bitmap[idx]) {
        std::cerr << "row group " << rg_idx << ", col " << col_idx << ": Bitmaps are not equal at index " << idx << ". Expected: " << static_cast<int>(this->nullmap[idx])
                    << " Got: " << static_cast<int>(their_bitmap[idx]) << std::endl;
        return false;
        }
    }

    switch (this->type) {
        case ColumnType::INTEGER: {
        if (requires_copy) {
            throw Generic_Exception("requires_copy not implemented for type INTEGER");
        }

        auto their_ints = reinterpret_cast<INTEGER*>(their_data);
        auto my_ints = reinterpret_cast<INTEGER*>(this->data.get());
        for (u64 idx = 0; idx < their_tuple_count; idx++) {
            if (this->nullmap[idx] && my_ints[idx] != their_ints[idx]) {
            std::cerr << "row group " << rg_idx << ", col " << col_idx << ": Integer data is not equal at index " << idx << " Expected: " << my_ints[idx]
                        << " Got: " << their_ints[idx] << std::endl;
            return false;
            }
        }
        break;
        }
        case ColumnType::DOUBLE: {
        if (requires_copy) {
            throw Generic_Exception("requires_copy not implemented for type DOUBLE");
        }

        auto their_doubles = reinterpret_cast<DOUBLE*>(their_data);
        auto my_doubles = reinterpret_cast<DOUBLE*>(this->data.get());
        for (u64 idx = 0; idx < their_tuple_count; idx++) {
            if (this->nullmap[idx] && my_doubles[idx] != their_doubles[idx]) {
            std::cerr << "row group " << rg_idx << ", col " << col_idx << ": Double data is not equal at index " << idx << std::setprecision(1000)
                        << " Expected: " << my_doubles[idx] << " Got: " << their_doubles[idx]
                        << std::endl;
            return false;
            }
        }
        break;
        }
        case ColumnType::STRING: {
        auto my_view = btrblocks::StringArrayViewer(this->data.get());
        for (u64 idx = 0; idx < their_tuple_count; idx++) {
            if (!this->nullmap[idx]) {
            continue;
            }

            str my_str = my_view(idx);
            str their_str;
            if (requires_copy) {
            auto their_view = btrblocks::StringPointerArrayViewer(their_data);
            their_str = their_view(idx);
            } else {
            auto their_view = btrblocks::StringArrayViewer(their_data);
            their_str = their_view(idx);
            }

            if (my_str.length() != their_str.length()) {
            std::cerr << "row group " << rg_idx << ", col " << col_idx << ": String lengths are not equal at index " << idx << " Expected: " << my_str
                        << " " << my_str.length() << " Got: " << their_str << " " << their_str.length()
                        << std::endl;
            return false;
            }

            if (my_str != their_str) {
            std::cerr << "row group " << rg_idx << ", col " << col_idx << ": Strings are not equal at index " << idx << " Expected: " << my_str
                        << " Got: " << their_str << std::endl;
            return false;
            }
        }
        break;
        }
        default:
        throw Generic_Exception("Type not implemented");
    }
    return true;
}

std::unique_ptr<BITMAP[]> InputChunk::get_nullmap_copy(){
    auto nullmap_copy = std::unique_ptr<uint8_t[]>(new uint8_t[tuple_count]);
    memcpy(nullmap_copy.get(), nullmap.get(), tuple_count);
    return std::move(nullmap_copy);
}


}