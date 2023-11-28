#include <cmath>
#include "c3/Utils.hpp"
#include "numerical.hpp"
#include "compression/Datablock.hpp"

namespace c3{
namespace multi_col{

bool Numerical::skip_scheme(std::shared_ptr<RowGroup> row_group, std::vector<std::shared_ptr<ColumnStats>>& btrBlocksSchemes, int i, int j){

    if( row_group->columns[i]->type != btrblocks::ColumnType::INTEGER
        || row_group->columns[j]->type != btrblocks::ColumnType::INTEGER){
        return true;
    }
    
    if(config.USE_PRUNING_RULES){
        // trim search space
        if(std::abs((int)btrBlocksSchemes[i]->intStats->unique_count - (int)btrBlocksSchemes[j]->intStats->unique_count) > 0.003 * btrBlocksSchemes[i]->intStats->tuple_count){ // > 200
            return true;
        }
    }

    return false;
}

double Numerical::expectedCompressionRatio(std::shared_ptr<RowGroup> row_group, const std::shared_ptr<btrblocks::InputChunk> source_column, const std::shared_ptr<btrblocks::InputChunk> target_column, int target_null_counter, float& regression_slope, float& regression_intercept, double& pearson_corr_coef, int& estimated_target_compressed_codes_size){
    
    assert(source_column->type == target_column->type);

    double equality_ratio = 0;

    double source_mean = 0;
    double target_mean = 0;
    int null_counter = 0;
    
    // compute mean of source and target
    switch(source_column->type){
        case btrblocks::ColumnType::INTEGER:{
            for(const auto& sample: row_group->samples){
                if(target_column->nullmap[sample]==1){
                    source_mean += source_column->integers()[sample];
                    target_mean += target_column->integers()[sample];
                    if(source_column->integers()[sample] == target_column->integers()[sample]){
                        equality_ratio++;
                    }
                }
                else{
                    null_counter++;
                }
            }
            break;
        }
    }

    // this doesn't seem to make a difference
    // if(1.0 * target_null_counter / row_group->tuple_count > 0.5){
    //     return 0;
    // }

    source_mean /= (row_group->samples.size() - null_counter);
    target_mean /= (row_group->samples.size() - null_counter);
    equality_ratio /= (row_group->samples.size() - null_counter);

    double pearson_numerator = 0;
    double pearson_denominator_source = 0;
    double pearson_denominator_target = 0;

    // compute pearson correlation coefficient
    for(const auto& sample: row_group->samples){
        double source_val;
        double target_val;
        switch(source_column->type){
            case btrblocks::ColumnType::INTEGER:{
                if(target_column->nullmap[sample]==1){
                    source_val = source_column->integers()[sample] - source_mean;
                    target_val = target_column->integers()[sample] - target_mean;
                }
                // if(source_column->integers()[sample] == target_column->integers()[sample]){
                //     equality_ratio++;
                // }
                break;
            }
            default: std::cout << "type error in pearson corelation coefficient" << std::endl;
        }
        pearson_numerator += (source_val) * (target_val);
        pearson_denominator_source += std::pow(source_val, 2);
        pearson_denominator_target += std::pow(target_val, 2);
    }

    // https://wikimedia.org/api/rest_v1/media/math/render/svg/9caed0f59417a425c988764032e5892130e97fa4
    regression_slope = pearson_numerator / pearson_denominator_source;
    regression_intercept = target_mean - regression_slope * source_mean;

    // https://www.gstatic.com/education/formulas2/472522532/en/correlation_coefficient_formula.svg
    double pearson_correlation_coefficient = pearson_numerator / std::sqrt(pearson_denominator_source * pearson_denominator_target);
    pearson_corr_coef = pearson_correlation_coefficient;

    // filter out schemes which will likely decrease CR improvement
    if(!(pearson_correlation_coefficient > 0.7 || pearson_correlation_coefficient < -0.7)){
        return 1;
    }

    // filter out columns which will most likely already be covered by equality scheme
    // if(equality_ratio > 0.90 && 
    // regression_intercept < 0.1 && regression_intercept > -0.1 && 
    // regression_slope < 1.01 && regression_slope > 0.99){
    //     return 1;
    // }

    // ----------------------------------------------------------------------------------

    // compute compression ratio
    std::vector<int> sample_encoded_target;
    int min_val = INT_MAX;
    for(const auto& sample: row_group->samples){
        int source_transformed = std::floor(regression_slope * source_column->integers()[sample] + regression_intercept);
        int target_val = target_column->integers()[sample] - source_transformed;
        min_val = std::min(target_val, min_val);
        sample_encoded_target.push_back(target_val);
    }

    if(min_val < 0){
        for(int i=0; i<sample_encoded_target.size(); i++){
            sample_encoded_target[i] -= min_val;
        }
    }

    size_t compressed_size = sizeof(C3Chunk) + sizeof(NumericMeta);
    compressed_size += Utils::lemire_128_bitpack_estimated_compressed_size(sample_encoded_target, source_column->tuple_count, row_group->samples);
    // compressed_size += 1.0 * target_column->tuple_count * (std::floor(std::log2(max_val - min_val)) + 1) / 8;

    estimated_target_compressed_codes_size = compressed_size;
    double estimated_compression_ratio = 1.0 * target_column->size / compressed_size;

    // ----------------------------------------------------------------------------------

    return estimated_compression_ratio;
}


std::vector<std::vector<uint8_t>> Numerical::apply_scheme(std::shared_ptr<btrblocks::InputChunk> source_column, std::shared_ptr<btrblocks::InputChunk> target_column, int source_idx, int target_idx, const uint8_t& force_source_scheme, std::shared_ptr<c3::NumericalCompressionScheme> scheme, bool skip_source_compression, bool previous_scheme_uses_source, std::vector<std::shared_ptr<ColumnStats>>& bbSchemes){
    
    assert(source_column->type == target_column->type);

    // prepare target chunk
    std::vector<uint8_t> target_output(sizeof(C3Chunk) + 10 * target_column->size);
    C3Chunk* targetChunk = reinterpret_cast<C3Chunk*>(target_output.data());
    targetChunk->compression_type = static_cast<uint8_t>(SchemeType::Numerical);
    targetChunk->original_col_size = target_column->size;
    targetChunk->type = target_column->type;
    targetChunk->source_column_id = source_idx;
    auto numeric_meta = reinterpret_cast<NumericMeta*>(targetChunk->data);
    numeric_meta->slope = scheme->slope;
    numeric_meta->intercept = scheme->intercept;
    // compress and write target signmap to target_meta->data

    auto new_target_data = std::unique_ptr<uint8_t[]>(new uint8_t[target_column->size]());
    auto new_target_writer = reinterpret_cast<btrblocks::units::INTEGER*>(new_target_data.get());
    auto new_target_nullmap = std::unique_ptr<uint8_t[]>(new uint8_t[target_column->tuple_count]());
    // auto target_sign_bitmap = std::unique_ptr<uint8_t[]>(new uint8_t[target_column->tuple_count]());

    // encode target column
    int exception_counter = 0;
    int min_target_val = 0;

    assert(target_column->type == btrblocks::ColumnType::INTEGER);
    for(size_t i=0; i<target_column->tuple_count; i++){
        if(target_column->nullmap[i]==1){
            int source_transformed = std::floor(scheme->slope * source_column->integers()[i] + scheme->intercept);
            int new_target_val = target_column->integers()[i] - source_transformed;
            min_target_val = std::min(min_target_val, new_target_val);
            // if(new_target_val<0){
                // new_target_writer[i] = -new_target_val;
                // target_sign_bitmap[i] = 1; 
            // }
            // else{
                new_target_writer[i] = new_target_val;
                // target_sign_bitmap[i] = 0; 
            // }
        }
        new_target_nullmap[i] = target_column->nullmap[i];
    }

    numeric_meta->reference_value = std::min(min_target_val, 0);
    if(min_target_val < 0){
        // shift all values into >=0 range
        for(size_t i=0; i<target_column->tuple_count; i++){
            new_target_writer[i] -= min_target_val;
        }
    }

    targetChunk->btrblocks_ColumnChunkMeta_offset = sizeof(NumericMeta);

    // compress target col
    auto basic_stats_target = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStatsBasic(target_column->tuple_count));// int BP
    auto target_col_stats = std::make_shared<ColumnStats>(ColumnStats(btrblocks::ColumnType::INTEGER, basic_stats_target, {}));
    
    // 255 = autoscheme()
    if(config.NUMERICAL_CODES_COMPRESSION_SCHEME == 255){
        target_col_stats = nullptr;
    }

    auto encoded_target_uncompressed = btrblocks::InputChunk(std::move(new_target_data), std::move(new_target_nullmap), target_column->type, target_column->tuple_count, target_column->size);
    auto compressed_target_size = btrblocks::Datablock::compress(encoded_target_uncompressed, targetChunk->data + targetChunk->btrblocks_ColumnChunkMeta_offset, static_cast<uint8_t>(config.NUMERICAL_CODES_COMPRESSION_SCHEME), &scheme->target_bb_scheme, &scheme->target_nullmap_size, target_col_stats);

    target_output.resize(sizeof(C3Chunk) + targetChunk->btrblocks_ColumnChunkMeta_offset + compressed_target_size);

    // if worse than BB ECR, cancel C3 compression
    // subtract target_nullmap_size, because BB scheme ECR also doesn't include nullmap
    if(config.REVERT_BB_IF_C3_BAD && (target_output.size() - scheme->target_nullmap_size) - (target_column->size / bbSchemes[target_idx]->get_best_scheme().second) > config.BYTES_SAVED_MARGIN * bbSchemes[target_idx]->get_original_chunk_size()){
        if(!skip_source_compression && previous_scheme_uses_source){
            // prepare source chunk & compress
            std::vector<uint8_t> source_output(sizeof(C3Chunk) + 10 * source_column->size);
            C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(source_output.data());
            sourceChunk->compression_type = static_cast<uint8_t>(SchemeType::BB);
            sourceChunk->type = source_column->type;
            sourceChunk->source_column_id = source_idx; // not used, since compression type set to BB
            sourceChunk->btrblocks_ColumnChunkMeta_offset = 0;
            auto compressed_source_size = btrblocks::Datablock::compress(*source_column, sourceChunk->data, force_source_scheme, &scheme->source_bb_scheme, &scheme->source_nullmap_size, bbSchemes[source_idx]);
            source_output.resize(sizeof(*sourceChunk) + compressed_source_size);
            return {source_output, {}};
        }
        else{
            return {{},{}};
        }
    }
    else{
        scheme->real_target_compressed_codes_size = compressed_target_size;
        if(!skip_source_compression){
            // prepare source chunk & compress
            std::vector<uint8_t> source_output(sizeof(C3Chunk) + 10 * source_column->size);
            C3Chunk* sourceChunk = reinterpret_cast<C3Chunk*>(source_output.data());
            sourceChunk->compression_type = static_cast<uint8_t>(SchemeType::BB);
            sourceChunk->type = source_column->type;
            sourceChunk->source_column_id = source_idx; // not used, since compression type set to None
            sourceChunk->btrblocks_ColumnChunkMeta_offset = 0;
            auto compressed_source_size = btrblocks::Datablock::compress(*source_column, sourceChunk->data, force_source_scheme, &scheme->source_bb_scheme, &scheme->source_nullmap_size, bbSchemes[source_idx]);
            source_output.resize(sizeof(*sourceChunk) + compressed_source_size);
            return {source_output, target_output};
        }

        return {{}, target_output}; // return compressed source and target
    }

}


std::pair<std::vector<uint8_t>, std::vector<btrblocks::BITMAP>> Numerical::decompress(const std::vector<uint8_t>& source_column, c3::C3Chunk* targetChunk, const std::vector<btrblocks::BITMAP>& source_null_map, size_t tuple_count, bool source_requires_copy){
    
    auto target_col_meta = reinterpret_cast<c3::C3Chunk*>(targetChunk);    
    auto numeric_meta = reinterpret_cast<NumericMeta*>(target_col_meta->data);

    std::vector<uint8_t> target_decompressed;
    std::vector<uint8_t> target_nullmap;
    c3::ChunkDecompression::bb_decompressColumn(target_decompressed, target_col_meta->data + target_col_meta->btrblocks_ColumnChunkMeta_offset, target_nullmap);

    assert(target_col_meta->type == btrblocks::ColumnType::INTEGER);
    
    auto target_decompressed_values = reinterpret_cast<btrblocks::INTEGER*>(target_decompressed.data());
    auto source_values = reinterpret_cast<const btrblocks::INTEGER*>(source_column.data());


    for(size_t i=0; i<tuple_count; i++){
        int source_value = source_null_map[i]==0 ? 0 : source_values[i];
        int source_transformed = std::floor(numeric_meta->slope * source_value + numeric_meta->intercept);
        target_decompressed_values[i] = source_transformed + target_decompressed_values[i] + numeric_meta->reference_value;
    }

    return {target_decompressed, target_nullmap};
}

}
}
