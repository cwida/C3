#pragma once

#include "common/Units.hpp"

namespace c3 {

struct NullmapMeta {
    btrblocks::BitmapType nullmap_type;
    uint8_t data[]; // [nullmap]
};

struct ExceptionsMeta {
    uint32_t values_offset;
    uint8_t data[]; // [indexes, values]
};

struct NumericMeta {
    float slope;
    float intercept;
    int32_t reference_value;
};

struct EqualityMeta {
    uint32_t exceptionsOffset;
    uint8_t data[]; // [targetNullMap, exceptions]
};

struct DictMeta {
    btrblocks::ColumnType type; // int -> type
    uint8_t data[]; // [dict]
};

struct MultiDictMeta {
    uint32_t targetDictOffset;
    uint32_t crossDictOffset;
    uint32_t exceptionsOffset;
    uint8_t data[]; // [targetNullMap, ~targetDict~, crossDict, exceptions]
};

struct DForMeta {
    uint32_t null_reference;
    uint8_t data[]; // [referenceDict]
};

struct Dict1toNMeta {
    uint32_t targetDictOffset;
    uint8_t data[]; // [offsets, targetDict]
};

struct C3Chunk {
    uint8_t compression_type;
    btrblocks::ColumnType type; // sollTyp
    uint16_t source_column_id;
    uint32_t original_col_size;
    uint32_t btrblocks_ColumnChunkMeta_offset;
    uint8_t data[]; // [metadata, BB columnChunk]; metadata differs depending on compression type, includes exceptions, dicts, etc. and corresponding offsets for these
};

}