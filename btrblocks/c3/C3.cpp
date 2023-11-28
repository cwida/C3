#include "C3.hpp"
#include "c3/Utils.hpp"
#include "compression/SchemePicker.hpp"
#include "c3/storage/Datablock.hpp"
#include "schemes/multi_col/dfor.hpp"
#include "schemes/multi_col/dict_1to1.hpp"
#include "schemes/multi_col/dict_1toN.hpp"
#include "schemes/multi_col/equality.hpp"
#include "schemes/multi_col/numerical.hpp"
#include "schemes/multi_col/dict_sharing.hpp"
#include "schemes/single_col/dictionary.hpp"

#include <cstdlib>
#include <regex>

namespace c3{

C3::C3(std::shared_ptr<RowGroup> row_group_)
:row_group(row_group_), graph(std::make_shared<CorrelationGraph>(row_group_->columns.size()))
{
    logging_info = std::make_shared<C3LoggingInfo>();

    column_status.resize(row_group->columns.size());
    for(size_t i=0; i<column_status.size(); i++){
        column_status[i] = ColumnStatus::None;
        logging_info->original_column_types.push_back(row_group->columns[i]->type);
    }
}

std::shared_ptr<C3LoggingInfo> C3::get_logging_info(){
    logging_info->add_to_graph_counter = add_to_graph_counter;
    logging_info->compute_ecr_counter = compute_ecr_counter;
    logging_info->final_scheme_counter = final_scheme_counter;
    logging_info->column_status = column_status;
    logging_info->graph = graph;
    logging_info->average_schemes_per_source = average_schemes_per_source;
    return logging_info;
}

// bool is_int(const std::string& s){
//     std::string::const_iterator it = s.begin();
//     if(s.size()>1 && *it == '-'){
//         it++;
//     }
//     while(it != s.end() && std::isdigit(*it)){
//         ++it;
//     }
//     return !s.empty() && it == s.end();
// }

bool isNumber(std::string x){
    std::regex e ("^-?\\d*\\.?\\d+");
    if (std::regex_match (x,e)) return true;
    else return false;
}

std::vector<int> C3::get_numeric_string_columns(){
    std::vector<int> int_cols;
    for(size_t i=0; i<row_group->columns.size(); i++){
        const auto& col = row_group->columns[i];

        if(col->type == btrblocks::ColumnType::STRING){
            bool is_int_flag = true;
            for(int j=0; j<col->tuple_count; j++){
                if(col->nullmap[j]==1 && !isNumber(std::string(col->strings()[j]))){
                    is_int_flag = false;
                    break;
                };
            }
            if(is_int_flag){
                int_cols.push_back(i);
            }
        }
    }
    return int_cols;
}

void C3::get_btrblocks_schemes_exact_ECR(){
    for(size_t i=0; i<row_group->columns.size(); i++){
        
        std::vector<std::pair<uint8_t, double>> column_compression_ratios;
        const auto& col = row_group->columns[i];
        switch(col->type){
            case btrblocks::ColumnType::INTEGER:{
                auto stats = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStats(reinterpret_cast<btrblocks::INTEGER*>(col->data.get()), col->nullmap.get(), col->tuple_count));

                for(auto& scheme: btrblocks::TypeWrapper<btrblocks::IntegerScheme, btrblocks::IntegerSchemeType>::getSchemes()){
                    if (!scheme.second->isUsable(*stats)) {
                        continue;
                    }
                
                    auto total_before = stats->total_size;
                    auto out_buffer = std::make_unique<uint8_t[]>(total_before * 2); // create buffer of same size as original col
                    auto total_after = scheme.second->compress(stats->src, stats->bitmap, out_buffer.get(), *stats, 1);
                    double compression_ratio = 1.0 * total_before / total_after;
                    
                    column_compression_ratios.push_back(std::make_pair(static_cast<uint8_t>(scheme.first), compression_ratio));
                }
            
                auto out_buffer = std::make_unique<uint8_t[]>(col->tuple_count);
                auto [nullmap_size, bitmap_type] = btrblocks::bitmap::RoaringBitmap::compress(col->nullmap.get(), out_buffer.get(), col->tuple_count);
                btrBlocksSchemes.push_back(std::make_shared<ColumnStats>(col->type, std::move(stats), column_compression_ratios, stats->total_size, nullmap_size));
                break;
            }
            case btrblocks::ColumnType::DOUBLE:{
                auto stats = std::make_shared<btrblocks::DoubleStats>(btrblocks::DoubleStats::generateStats(reinterpret_cast<btrblocks::DOUBLE*>(col->data.get()), col->nullmap.get(), col->tuple_count));

                for(auto& scheme: btrblocks::TypeWrapper<btrblocks::DoubleScheme, btrblocks::DoubleSchemeType>::getSchemes()){
                    if (!scheme.second->isUsable(*stats)) {
                        continue;
                    }
                
                    auto total_before = stats->total_size;
                    auto out_buffer = std::make_unique<uint8_t[]>(total_before * 2); // create buffer of same size as original col
                    auto total_after = scheme.second->compress(stats->src, stats->bitmap, out_buffer.get(), *stats, 1);
                    double compression_ratio = 1.0 * total_before / total_after;

                    column_compression_ratios.push_back(std::make_pair(static_cast<uint8_t>(scheme.first), compression_ratio));
                }

                auto out_buffer = std::make_unique<uint8_t[]>(col->tuple_count);
                auto [nullmap_size, bitmap_type] = btrblocks::bitmap::RoaringBitmap::compress(col->nullmap.get(), out_buffer.get(), col->tuple_count);
                btrBlocksSchemes.push_back(std::make_shared<ColumnStats>(col->type, std::move(stats), column_compression_ratios, stats->total_size, nullmap_size));
                break;
            }
            case btrblocks::ColumnType::STRING:{
                auto stats = std::make_shared<btrblocks::StringStats>(btrblocks::StringStats::generateStats(btrblocks::StringArrayViewer(col->data.get()), col->nullmap.get(), col->tuple_count, col->size));

                for(auto& scheme: btrblocks::TypeWrapper<btrblocks::StringScheme, btrblocks::StringSchemeType>::getSchemes()){
                    if (!scheme.second->isUsable(*stats)) {
                        continue;
                    }
                
                    auto total_before = stats->total_size;
                    auto out_buffer = std::make_unique<uint8_t[]>(total_before * 2); // create buffer of same size as original col
                    auto total_after = scheme.second->compress(btrblocks::StringArrayViewer(col->data.get()), col->nullmap.get(), out_buffer.get(), *stats);
                    double compression_ratio = 1.0 * total_before / total_after;
                    column_compression_ratios.push_back(std::make_pair(static_cast<uint8_t>(scheme.first), compression_ratio));
                }

                auto out_buffer = std::make_unique<uint8_t[]>(col->tuple_count);
                auto [nullmap_size, bitmap_type] = btrblocks::bitmap::RoaringBitmap::compress(col->nullmap.get(), out_buffer.get(), col->tuple_count);
                btrBlocksSchemes.push_back(std::make_shared<ColumnStats>(col->type, std::move(stats), column_compression_ratios, stats->total_size, nullmap_size));
                break;
            }
        }

    }
}

// void C3::get_btrblocks_schemes(){
//     for(size_t i=0; i<row_group->columns.size(); i++){
        
//         std::vector<std::pair<uint8_t, double>> column_compression_ratios;
//         const auto& col = row_group->columns[i];
//         switch(col->type){
//             case btrblocks::ColumnType::INTEGER:{
//                 auto stats = std::make_shared<btrblocks::SInteger32Stats>(btrblocks::SInteger32Stats::generateStats(reinterpret_cast<btrblocks::INTEGER*>(col->data.get()), col->nullmap.get(), col->tuple_count));

//                 for(auto& scheme: btrblocks::TypeWrapper<btrblocks::IntegerScheme, btrblocks::IntegerSchemeType>::getSchemes()){
//                     if (!scheme.second->isUsable(*stats)) {
//                         continue;
//                     }
//                     column_compression_ratios.push_back(std::make_pair(static_cast<uint8_t>(scheme.first), scheme.second->expectedCompressionRatio(*stats, 1)));
//                 }

//                 btrBlocksSchemes.push_back(std::make_shared<ColumnStats>(col->type, std::move(stats), column_compression_ratios, stats->total_size));
//                 break;
//             }
//             case btrblocks::ColumnType::DOUBLE:{
//                 auto stats = std::make_shared<btrblocks::DoubleStats>(btrblocks::DoubleStats::generateStats(reinterpret_cast<btrblocks::DOUBLE*>(col->data.get()), col->nullmap.get(), col->tuple_count));

//                 for(auto& scheme: btrblocks::TypeWrapper<btrblocks::DoubleScheme, btrblocks::DoubleSchemeType>::getSchemes()){
//                     if (!scheme.second->isUsable(*stats)) {
//                         continue;
//                     }
//                     column_compression_ratios.push_back(std::make_pair(static_cast<uint8_t>(scheme.first), scheme.second->expectedCompressionRatio(*stats, 1)));
//                 }

//                 btrBlocksSchemes.push_back(std::make_shared<ColumnStats>(col->type, std::move(stats), column_compression_ratios, stats->total_size));
//                 break;
//             }
//             case btrblocks::ColumnType::STRING:{
//                 auto stats = std::make_shared<btrblocks::StringStats>(btrblocks::StringStats::generateStats(btrblocks::StringArrayViewer(col->data.get()), col->nullmap.get(), col->tuple_count, col->size));

//                 for(auto& scheme: btrblocks::TypeWrapper<btrblocks::StringScheme, btrblocks::StringSchemeType>::getSchemes()){
//                     if (!scheme.second->isUsable(*stats)) {
//                         continue;
//                     }
//                     column_compression_ratios.push_back(std::make_pair(static_cast<uint8_t>(scheme.first), scheme.second->expectedCompressionRatio(*stats, 1)));
//                 }

//                 btrBlocksSchemes.push_back(std::make_shared<ColumnStats>(col->type, std::move(stats), column_compression_ratios, stats->total_size));
//                 break;
//             }
//         }

//     }
// }

void C3::get_ignore_columns(){
    for(size_t i=0; i<column_status.size(); i++){
        size_t unique_count = 0;
        size_t null_count = 0;  // with too many nulls, sampling will likely catch little info, leading to bad ECR
        size_t tuple_count = 0;
        switch(btrBlocksSchemes[i]->type){
            case btrblocks::ColumnType::INTEGER:{
                unique_count = btrBlocksSchemes[i]->intStats->unique_count;
                null_count = btrBlocksSchemes[i]->intStats->null_count;
                tuple_count = btrBlocksSchemes[i]->intStats->tuple_count;
                break;
            }
            case btrblocks::ColumnType::DOUBLE:{
                unique_count = btrBlocksSchemes[i]->doubleStats->unique_count;
                null_count = btrBlocksSchemes[i]->doubleStats->null_count;
                tuple_count = btrBlocksSchemes[i]->doubleStats->tuple_count;
                break;
            }
            case btrblocks::ColumnType::STRING:{
                unique_count = btrBlocksSchemes[i]->stringStats->unique_count;
                null_count = btrBlocksSchemes[i]->stringStats->null_count;
                tuple_count = btrBlocksSchemes[i]->stringStats->tuple_count;
                break;
            }
        }
        // if(unique_count < 2){ // || null_count > (tuple_count / 2)){
        //     column_status[i] = ColumnStatus::Ignore;
        // }
        if(null_count==tuple_count){
            column_status[i] = ColumnStatus::Ignore;
        }
    }
}

void C3::force_compression_schemes(std::vector<std::shared_ptr<c3::CompressionScheme>> schemes){
    for(const auto& scheme: schemes){
        switch(scheme->type){
            case SchemeType::Equality:{
	            find_equality_correlation(scheme->columns[0], scheme->columns[1]);
                break;
            }
            case SchemeType::Dict_1to1:{
	            find_dict1to1_correlation(scheme->columns[0], scheme->columns[1]);
                break;
            }
            case SchemeType::Dict_1toN:{
	            find_dict1toN_correlation(scheme->columns[0], scheme->columns[1]);
                break;
            }
            case SchemeType::Numerical:{
	            find_numerical_correlation(scheme->columns[0], scheme->columns[1]);
                break;
            }
            case SchemeType::DFOR:{
	            find_dfor_correlation(scheme->columns[0], scheme->columns[1]);
                break;
            }
            case SchemeType::Dict_Sharing:{
	            find_dictShare_correlation(scheme->columns[0], scheme->columns[1]);
                break;
            }
            default: std::cerr << "This scheme is not supposed to be here..." << std::endl;
        }
    }
}

void C3::graph_given_find_correlations(std::shared_ptr<CorrelationGraph> given_graph){
    for(const auto& edge: given_graph->edges){
        switch(edge->scheme->type){
            case SchemeType::Equality:{
	            find_equality_correlation(edge->scheme->columns[0], edge->scheme->columns[1]);
                break;
            }
            case SchemeType::Dict_1to1:{
	            find_dict1to1_correlation(edge->scheme->columns[0], edge->scheme->columns[1]);
                break;
            }
            case SchemeType::Dict_1toN:{
	            find_dict1toN_correlation(edge->scheme->columns[0], edge->scheme->columns[1]);
                break;
            }
            case SchemeType::Numerical:{
	            find_numerical_correlation(edge->scheme->columns[0], edge->scheme->columns[1]);
                break;
            }
            case SchemeType::DFOR:{
	            find_dfor_correlation(edge->scheme->columns[0], edge->scheme->columns[1]);
                break;
            }
            case SchemeType::Dict_Sharing:{
	            find_dictShare_correlation(edge->scheme->columns[0], edge->scheme->columns[1]);
                break;
            }
            default: std::cerr << "This scheme is not supposed to be here... " << std::endl;
        }
    }
}

void C3::get_compression_schemes(std::ofstream& log_stream, bool finalize, bool ignore_bb_ecr, std::vector<std::shared_ptr<c3::CompressionScheme>> force_schemes, std::shared_ptr<CorrelationGraph> c3_graph){
    
    ignore_bb_ecr = ignore_bb_ecr || config.IGNORE_BB_ECR;

    if(!force_schemes.empty()){
        force_compression_schemes(force_schemes);
        if(finalize){
            auto final_edges = graph->finalize();
            for(const auto& edge: final_edges){
                log_stream << edge->scheme->to_string() << std::endl;
                final_scheme_counter++;
                compressionSchemes.push_back(edge->scheme);
                column_status[edge->targetCol->node_index] = ColumnStatus::AssignedC3Scheme;
                column_status[edge->sourceCol->node_index] = ColumnStatus::AssignedC3Scheme;
            }
        }
        else{
            for(auto& column: graph->columnNodes){
                for(auto& edge: column->outgoingEdges){
                    final_scheme_counter++;
                    log_stream << edge->scheme->to_string() << std::endl;
                    compressionSchemes.push_back(edge->scheme);
                    // target column status stores the scheme
                    column_status[edge->targetCol->node_index] = ColumnStatus::AssignedC3Scheme;
                    column_status[edge->sourceCol->node_index] = ColumnStatus::AssignedC3Scheme;
                }
            }
        }
    }
    else if(c3_graph != nullptr){
        graph_given_find_correlations(c3_graph);

        if(finalize){
            auto final_edges = graph->finalize();
            for(const auto& edge: final_edges){
                log_stream << edge->scheme->to_string() << std::endl;
                final_scheme_counter++;
                compressionSchemes.push_back(edge->scheme);
                column_status[edge->targetCol->node_index] = ColumnStatus::AssignedC3Scheme;
                column_status[edge->sourceCol->node_index] = ColumnStatus::AssignedC3Scheme;
            }
        }
        else{
            for(auto& column: graph->columnNodes){
                for(auto& edge: column->outgoingEdges){
                    final_scheme_counter++;
                    log_stream << edge->scheme->to_string() << std::endl;
                    compressionSchemes.push_back(edge->scheme);
                    // target column status stores the scheme
                    column_status[edge->targetCol->node_index] = ColumnStatus::AssignedC3Scheme;
                    column_status[edge->sourceCol->node_index] = ColumnStatus::AssignedC3Scheme;
                }
            }
        }
    }
    else{
        get_ignore_columns();
        find_correlations(ignore_bb_ecr);

        for(auto& status: column_status){
            if(status==ColumnStatus::Ignore){
                status = ColumnStatus::None;
            }
        }
        
        if(finalize){
            auto final_edges = graph->finalize();
            for(const auto& edge: final_edges){
                log_stream << edge->scheme->to_string() << std::endl;
                final_scheme_counter++;
                compressionSchemes.push_back(edge->scheme);
                column_status[edge->targetCol->node_index] = ColumnStatus::AssignedC3Scheme;
                column_status[edge->sourceCol->node_index] = ColumnStatus::AssignedC3Scheme;
            }
        }
        else{
            for(auto& column: graph->columnNodes){
                for(auto& edge: column->outgoingEdges){
                    final_scheme_counter++;
                    log_stream << edge->scheme->to_string() << std::endl;
                    compressionSchemes.push_back(edge->scheme);
                    // target column status stores the scheme
                    column_status[edge->targetCol->node_index] = ColumnStatus::AssignedC3Scheme;
                    column_status[edge->sourceCol->node_index] = ColumnStatus::AssignedC3Scheme;
                }
            }
        }
    }
}

void C3::find_dict1toN_correlation(int i, int j, bool ignore_bb_ecr){
    // 1 to N dict
    if(config.ENABLE_DICT_1TON && !multi_col::Dictionary_1toN::skip_scheme(row_group, btrBlocksSchemes, i, j)){
        compute_ecr_counter++;
        int estimated_dict_size;
        int estimated_target_compressed_codes_size;
        int estimated_offsets_size;
        auto ecr = multi_col::Dictionary_1toN::expectedCompressionRatio(row_group->columns[i], row_group->columns[j], row_group->samples, btrBlocksSchemes[i], btrBlocksSchemes[j], estimated_dict_size, estimated_target_compressed_codes_size, estimated_offsets_size, row_group);

        // source column
        double btrBlocks_compressedSize_source = row_group->columns[i]->size / btrBlocksSchemes[i]->get_best_scheme().second;
        double c3_compressedSize_source = row_group->columns[i]->size / btrBlocksSchemes[i]->get_dict_compression_ratio();

        // target column
        double btrBlocks_compressedSize_target = row_group->columns[j]->size / btrBlocksSchemes[j]->get_best_scheme().second;
        double c3_compressedSize_target = row_group->columns[j]->size / ecr;

        int c3_bytes_saved_source = btrBlocks_compressedSize_source - c3_compressedSize_source;
        int c3_bytes_saved_target = btrBlocks_compressedSize_target - c3_compressedSize_target;
        int c3_bytes_saved = c3_bytes_saved_source + c3_bytes_saved_target;

        // only consider c3 scheme, if better than BB schemes
        if(ignore_bb_ecr || c3_bytes_saved > config.BYTES_SAVED_MARGIN * btrBlocksSchemes[j]->get_original_chunk_size()){
            add_to_graph_counter++;
            auto scheme = std::make_shared<Dict_1toN_CompressionScheme>(i,j,c3_bytes_saved_source, c3_bytes_saved_target);
            auto corr_edge = std::make_shared<CorrelationEdge>(graph->columnNodes[i], graph->columnNodes[j], scheme);
            graph->columnNodes[i]->outgoingEdges.push_back(corr_edge);
            graph->columnNodes[j]->incomingEdges.push_back(corr_edge);
            graph->edges.push_back(corr_edge);
            
            // log for analysis
            scheme->estimated_bytes_saved_source = btrBlocks_compressedSize_source - c3_compressedSize_source;
            scheme->estimated_bytes_saved_target = btrBlocks_compressedSize_target - c3_compressedSize_target;
            scheme->estimated_source_target_dict_size = estimated_dict_size;
            scheme->bb_source_ecr = btrBlocksSchemes[i]->get_best_scheme().second;
            scheme->bb_target_ecr = btrBlocksSchemes[j]->get_best_scheme().second;
            scheme->C3_source_ecr = btrBlocksSchemes[i]->get_dict_compression_ratio();
            scheme->C3_target_ecr = ecr;
            scheme->source_unique_count = btrBlocksSchemes[i]->get_unique_count();
            scheme->target_unique_count = btrBlocksSchemes[j]->get_unique_count();
            scheme->source_null_count = btrBlocksSchemes[i]->get_null_count();
            scheme->target_null_count = btrBlocksSchemes[j]->get_null_count();
            scheme->estimated_target_compressed_codes_size = estimated_target_compressed_codes_size;
            scheme->estimated_offsets_size = estimated_offsets_size;
        }
    }
}

void C3::find_equality_correlation(int i, int j, bool ignore_bb_ecr){
    // equality
    if(config.ENABLE_EQUALITY && !multi_col::Equality::skip_scheme(row_group, btrBlocksSchemes, i, j)){

        compute_ecr_counter++;
        // for equality scheme, only need to consider target column CR, source scheme is same as BB
        int estimated_exception_size;
        auto target_ratios = multi_col::Equality::expectedCompressionRatio(row_group, row_group->columns[i], row_group->columns[j], estimated_exception_size);
        double btrBlocks_compressedSize = row_group->columns[j]->size / btrBlocksSchemes[j]->get_best_scheme().second;
        double c3_compressedSize = row_group->columns[j]->size / target_ratios.second;
        
        int c3_bytes_saved = btrBlocks_compressedSize - c3_compressedSize;
        
        if(target_ratios.first <= config.EQUALITY_EXCEPTION_RATIO_THRESHOLD && (ignore_bb_ecr || c3_bytes_saved > config.BYTES_SAVED_MARGIN * btrBlocksSchemes[j]->get_original_chunk_size())){
            add_to_graph_counter++;
            auto scheme = std::make_shared<EqualityCompressionScheme>(i,j,0,c3_bytes_saved);
            auto corr_edge = std::make_shared<CorrelationEdge>(graph->columnNodes[i], graph->columnNodes[j], scheme);
            graph->columnNodes[i]->outgoingEdges.push_back(corr_edge);
            graph->columnNodes[j]->incomingEdges.push_back(corr_edge);
            graph->edges.push_back(corr_edge);

            // log for analysis
            scheme->estimated_bytes_saved_source = 0;
            scheme->estimated_bytes_saved_target = c3_bytes_saved;
            scheme->estimated_exception_count = row_group->tuple_count * target_ratios.first;
            scheme->estimated_exception_size = estimated_exception_size;
            scheme->bb_source_ecr = btrBlocksSchemes[i]->get_best_scheme().second;
            scheme->bb_target_ecr = btrBlocksSchemes[j]->get_best_scheme().second;
            scheme->C3_source_ecr = scheme->bb_source_ecr;
            scheme->C3_target_ecr = target_ratios.second;
            scheme->source_unique_count = btrBlocksSchemes[i]->get_unique_count();
            scheme->target_unique_count = btrBlocksSchemes[j]->get_unique_count();
            scheme->source_null_count = btrBlocksSchemes[i]->get_null_count();
            scheme->target_null_count = btrBlocksSchemes[j]->get_null_count();
        }
    }
}

void C3::find_dict1to1_correlation(int i, int j, bool ignore_bb_ecr){
    // dict
    if(config.ENABLE_DICT_1TO1 && !multi_col::Dictionary_1to1::skip_scheme(row_group, btrBlocksSchemes, i, j)){

        compute_ecr_counter++;
        // ER, CRs
        int estimated_source_target_dict_size;
        int estimated_target_dict_size;
        int estimated_exception_size;
        std::pair<double, double> ratios = multi_col::Dictionary_1to1::expectedCompressionRatio(row_group->columns[i], row_group->columns[j], row_group->samples, btrBlocksSchemes[i], btrBlocksSchemes[j], estimated_source_target_dict_size, estimated_target_dict_size, estimated_exception_size);
        
        if(ratios.second == 1){ return; };

        // source column
        double btrBlocks_compressedSize_source = row_group->columns[i]->size / btrBlocksSchemes[i]->get_best_scheme().second;
        double c3_compressedSize_source = row_group->columns[i]->size / btrBlocksSchemes[i]->get_dict_compression_ratio();

        // target column
        double btrBlocks_compressedSize_target = row_group->columns[j]->size / btrBlocksSchemes[j]->get_best_scheme().second;
        double c3_compressedSize_target = row_group->columns[j]->size / ratios.second;

        int c3_bytes_saved_source = btrBlocks_compressedSize_source - c3_compressedSize_source;
        int c3_bytes_saved_target = btrBlocks_compressedSize_target - c3_compressedSize_target;
        int c3_bytes_saved = c3_bytes_saved_source + c3_bytes_saved_target;

        // only consider c3 scheme, if better than BB schemes
        if(ratios.first <= config.DICT_EXCEPTION_RATIO_THRESHOLD && (ignore_bb_ecr || c3_bytes_saved > config.BYTES_SAVED_MARGIN * btrBlocksSchemes[j]->get_original_chunk_size())){
            add_to_graph_counter++;
            auto scheme = std::make_shared<Dictionary_1to1_CompressionScheme>(i,j,c3_bytes_saved_source,c3_bytes_saved_target);
            auto corr_edge = std::make_shared<CorrelationEdge>(graph->columnNodes[i], graph->columnNodes[j], scheme);
            graph->columnNodes[i]->outgoingEdges.push_back(corr_edge);
            graph->columnNodes[j]->incomingEdges.push_back(corr_edge);
            graph->edges.push_back(corr_edge);
            
            // log for analysis
            scheme->estimated_bytes_saved_source = btrBlocks_compressedSize_source - c3_compressedSize_source;
            scheme->estimated_bytes_saved_target = btrBlocks_compressedSize_target - c3_compressedSize_target;
            scheme->estimated_exception_count = row_group->tuple_count * ratios.first;
            scheme->estimated_exception_size = estimated_exception_size;
            scheme->estimated_source_target_dict_size = estimated_source_target_dict_size;
            scheme->estimated_target_dict_size = estimated_target_dict_size;
            scheme->bb_source_ecr = btrBlocksSchemes[i]->get_best_scheme().second;
            scheme->bb_target_ecr = btrBlocksSchemes[j]->get_best_scheme().second;
            scheme->C3_source_ecr = btrBlocksSchemes[i]->get_dict_compression_ratio();
            scheme->C3_target_ecr = ratios.second;
            scheme->source_unique_count = btrBlocksSchemes[i]->get_unique_count();
            scheme->target_unique_count = btrBlocksSchemes[j]->get_unique_count();
            scheme->source_null_count = btrBlocksSchemes[i]->get_null_count();
            scheme->target_null_count = btrBlocksSchemes[j]->get_null_count();
        }
    }
}

void C3::find_numerical_correlation(int i, int j, bool ignore_bb_ecr){
    // numerical
    if(config.ENABLE_NUMERICAL && !multi_col::Numerical::skip_scheme(row_group, btrBlocksSchemes, i, j)){

        compute_ecr_counter++;
        float slope;
        float intercept;
        double pearson_corr_coef;
        int estimated_target_compressed_codes_size;
        double ecr = multi_col::Numerical::expectedCompressionRatio(row_group, row_group->columns[i], row_group->columns[j], btrBlocksSchemes[j]->intStats->null_count, slope, intercept, pearson_corr_coef, estimated_target_compressed_codes_size);
        
        if(ecr == 1){ return; };

        double btrBlocks_compressedSize = row_group->columns[j]->size / btrBlocksSchemes[j]->get_best_scheme().second;
        double c3_compressedSize = row_group->columns[j]->size / ecr;
        int c3_bytes_saved = btrBlocks_compressedSize - c3_compressedSize;

        if(ignore_bb_ecr || (c3_bytes_saved > config.BYTES_SAVED_MARGIN * btrBlocksSchemes[j]->get_original_chunk_size())){
            add_to_graph_counter++;
            auto scheme = std::make_shared<NumericalCompressionScheme>(i,j,slope,intercept,0,c3_bytes_saved);
            auto corr_edge = std::make_shared<CorrelationEdge>(graph->columnNodes[i], graph->columnNodes[j], scheme);
            graph->columnNodes[i]->outgoingEdges.push_back(corr_edge); //
            graph->columnNodes[j]->incomingEdges.push_back(corr_edge); //
            graph->edges.push_back(corr_edge);

            // log for analysis
            scheme->estimated_bytes_saved_source = 0;
            scheme->estimated_bytes_saved_target = c3_bytes_saved;
            scheme->pearson_corr_coef = pearson_corr_coef;
            scheme->bb_source_ecr = btrBlocksSchemes[i]->get_best_scheme().second;
            scheme->bb_target_ecr = btrBlocksSchemes[j]->get_best_scheme().second;
            scheme->C3_source_ecr = scheme->bb_source_ecr;
            scheme->C3_target_ecr = ecr;
            scheme->source_column_min = btrBlocksSchemes[i]->intStats->min;
            scheme->source_column_max = btrBlocksSchemes[i]->intStats->max;
            scheme->target_column_min = btrBlocksSchemes[j]->intStats->min;
            scheme->target_column_max = btrBlocksSchemes[j]->intStats->max;
            scheme->source_unique_count = btrBlocksSchemes[i]->get_unique_count();
            scheme->target_unique_count = btrBlocksSchemes[j]->get_unique_count();
            scheme->source_null_count = btrBlocksSchemes[i]->get_null_count();
            scheme->target_null_count = btrBlocksSchemes[j]->get_null_count();
            scheme->estimated_target_compressed_codes_size = estimated_target_compressed_codes_size;
        }
    }
}

void C3::find_dfor_correlation(int i, int j, bool ignore_bb_ecr){
    // dfor
    if(config.ENABLE_DFOR && !multi_col::DFOR::skip_scheme(row_group, btrBlocksSchemes, i, j)){
        compute_ecr_counter++;
        int estimated_target_compressed_codes_size;
        int estimated_source_target_dict_size;
        double ecr_target = multi_col::DFOR::expectedCompressionRatio(row_group->columns[i], row_group->columns[j], row_group->samples, btrBlocksSchemes[i], btrBlocksSchemes[j], estimated_target_compressed_codes_size, estimated_source_target_dict_size);
    
        if(ecr_target == 1){ return; };

        // source
        double btrBlocks_compressedSize_source = row_group->columns[i]->size / btrBlocksSchemes[i]->get_best_scheme().second;
        double c3_compressedSize_source = row_group->columns[i]->size / btrBlocksSchemes[i]->get_dict_compression_ratio();
        

        // target
        double btrBlocks_compressedSize_target = row_group->columns[j]->size / btrBlocksSchemes[j]->get_best_scheme().second;
        double c3_compressedSize_target = row_group->columns[j]->size / ecr_target;
        
        int c3_bytes_saved_source = btrBlocks_compressedSize_source - c3_compressedSize_source;
        int c3_bytes_saved_target = btrBlocks_compressedSize_target - c3_compressedSize_target;
        int c3_bytes_saved = c3_bytes_saved_source + c3_bytes_saved_target;

        if(ignore_bb_ecr || (c3_bytes_saved > config.BYTES_SAVED_MARGIN * btrBlocksSchemes[j]->get_original_chunk_size())){
            add_to_graph_counter++;
            auto scheme = std::make_shared<DForCompressionScheme>(i,j,c3_bytes_saved_source,c3_bytes_saved_target);
            auto corr_edge = std::make_shared<CorrelationEdge>(graph->columnNodes[i], graph->columnNodes[j], scheme);
            graph->columnNodes[i]->outgoingEdges.push_back(corr_edge); //
            graph->columnNodes[j]->incomingEdges.push_back(corr_edge); //
            graph->edges.push_back(corr_edge);

            // log for analysis
            scheme->estimated_bytes_saved_source = 0;
            scheme->estimated_bytes_saved_target = c3_bytes_saved;
            scheme->target_column_min = btrBlocksSchemes[j]->intStats->min;
            scheme->target_column_max = btrBlocksSchemes[j]->intStats->max;
            scheme->bb_source_ecr = btrBlocksSchemes[i]->get_best_scheme().second;
            scheme->bb_target_ecr = btrBlocksSchemes[j]->get_best_scheme().second;
            scheme->C3_source_ecr = btrBlocksSchemes[i]->get_dict_compression_ratio();
            scheme->C3_target_ecr = ecr_target;
            scheme->source_unique_count = btrBlocksSchemes[i]->get_unique_count();
            scheme->target_unique_count = btrBlocksSchemes[j]->get_unique_count();
            scheme->source_null_count = btrBlocksSchemes[i]->get_null_count();
            scheme->target_null_count = btrBlocksSchemes[j]->get_null_count();
            scheme->estimated_target_compressed_codes_size = estimated_target_compressed_codes_size;
            scheme->estimated_source_target_dict_size = estimated_source_target_dict_size;
        }
    }
}

void C3::find_dictShare_correlation(int i, int j, bool ignore_bb_ecr){
    // dictShare
    if(config.ENABLE_DICT_SHARING && !multi_col::DictSharing::skip_scheme(row_group, btrBlocksSchemes, i, j)){

        compute_ecr_counter++;
        int estimated_source_target_dict_size;
        int estimated_target_codes_size;
        auto ecrs = multi_col::DictSharing::expectedCompressionRatio(row_group->columns[i], row_group->columns[j], row_group->samples, btrBlocksSchemes[i], btrBlocksSchemes[j], estimated_source_target_dict_size, estimated_target_codes_size);

        // btrblocks compressed size
        double btrBlocks_compressedSize_source = row_group->columns[i]->size / btrBlocksSchemes[i]->get_best_scheme().second;
        double btrBlocks_compressedSize_target = row_group->columns[j]->size / btrBlocksSchemes[j]->get_best_scheme().second;
        
        // c3 compressed size
        double c3_compressedSize_source = row_group->columns[i]->size / ecrs.first;
        double c3_compressedSize_target = row_group->columns[j]->size / ecrs.second;
        
        int c3_bytes_saved_source = btrBlocks_compressedSize_source - c3_compressedSize_source;
        int c3_bytes_saved_target = btrBlocks_compressedSize_target - c3_compressedSize_target;

        int c3_bytes_saved = c3_bytes_saved_source + c3_bytes_saved_target;

        if(ignore_bb_ecr || (c3_bytes_saved > config.BYTES_SAVED_MARGIN * btrBlocksSchemes[j]->get_original_chunk_size())){
            add_to_graph_counter++;
            auto scheme = std::make_shared<DictSharingCompressionScheme>(i,j,c3_bytes_saved_source,c3_bytes_saved_target);
            auto corr_edge = std::make_shared<CorrelationEdge>(graph->columnNodes[i], graph->columnNodes[j], scheme);
            graph->columnNodes[i]->outgoingEdges.push_back(corr_edge); //
            graph->columnNodes[j]->incomingEdges.push_back(corr_edge); //
            graph->edges.push_back(corr_edge);

            // log for analysis
            scheme->estimated_bytes_saved_source = c3_bytes_saved_source;
            scheme->estimated_bytes_saved_target = c3_bytes_saved_target;
            scheme->bb_source_ecr = btrBlocksSchemes[i]->get_best_scheme().second;
            scheme->bb_target_ecr = btrBlocksSchemes[j]->get_best_scheme().second;
            scheme->C3_source_ecr = ecrs.first;
            scheme->C3_target_ecr = ecrs.second;
            scheme->source_unique_count = btrBlocksSchemes[i]->get_unique_count();
            scheme->target_unique_count = btrBlocksSchemes[j]->get_unique_count();
            scheme->source_null_count = btrBlocksSchemes[i]->get_null_count();
            scheme->target_null_count = btrBlocksSchemes[j]->get_null_count();
            scheme->estimated_source_target_dict_size = estimated_source_target_dict_size;
            scheme->estimated_target_compressed_codes_size = estimated_target_codes_size;
        }
    }
}


void C3::find_correlations(bool ignore_bb_ecr){
    for(int i=0; i<column_status.size(); i++){

        if(column_status[i] == ColumnStatus::Ignore){
            continue;
        }
        for(int j=0; j<column_status.size(); j++){

            if(std::abs(i-j) > config.C3_WINDOW_SIZE){
                continue;
            }

            if(i == j || column_status[j] == ColumnStatus::Ignore){
                continue;
            }

            find_dict1toN_correlation(i, j, ignore_bb_ecr);
            find_equality_correlation(i, j, ignore_bb_ecr);
            find_dict1to1_correlation(i, j, ignore_bb_ecr);
            find_numerical_correlation(i, j, ignore_bb_ecr);
            find_dfor_correlation(i, j, ignore_bb_ecr);
            find_dictShare_correlation(i, j, ignore_bb_ecr);
        }
    }
}

std::vector<std::vector<uint8_t>> C3::compress(std::ofstream& log_stream){
    // 1. Go through C3 schemes, apply and compress
        // 1.1 first compress non-dict schemes
        // 1.2 compress dict sharing schemes
        // 1.3 compress other dict schemes
    // 2. Go through columns, find uncompressed, and apply BB compression (can force scheme we already found from sampling?), wrap in C3Chunk

    std::vector<std::vector<uint8_t>> compressed_column_chunks;
    compressed_column_chunks.resize(row_group->columns.size());

    // 1.
    for(int i=0; i<3; i++){
        for(const auto& scheme: compressionSchemes){
            if(i==0 && !Utils::is_non_dict_scheme(scheme->type)){
                continue;
            }
            else if(i==1 && !(scheme->type==SchemeType::Dict_Sharing)){
                continue;
            }
            else if(i==2 && (!Utils::is_dict_scheme(scheme->type) || scheme->type==SchemeType::Dict_Sharing)){
                continue;
            }
            // std::cout << scheme->to_string() << std::endl;
            std::vector<std::vector<uint8_t>> new_columns = apply_scheme(scheme);
            assert(new_columns.size()==2);
            if(new_columns[1].size()==0){
                // C3 scheme was cancelled
                log_stream << "Cancelled " << Utils::scheme_to_string(scheme->type) << ": " << scheme->columns[0] << ", " << scheme->columns[1] << std::endl;
                // remove scheme from graph final out/in edges
                graph->remove_final_edge(scheme, scheme->columns[0], scheme->columns[1]);
                
                // set columnStatus to None (check source first)
                column_status[scheme->columns[1]] = ColumnStatus::None;
                if(graph->columnNodes[scheme->columns[0]]->final_outgoingEdges.empty()){
                    column_status[scheme->columns[0]] = ColumnStatus::None;
                    assert(new_columns[0].size()==0);
                }
                else if(new_columns[0].size()>0){
                    // if source col needed by another scheme, save the compressed source column
                    compressed_column_chunks[scheme->columns[0]] = new_columns[0];   
                }            
                else if(new_columns[0].size()==0){
                    // if all other outgoingedges are applied, but new_columns size is 0
                    bool all_schemes_applied = true;
                    for(auto& edge : graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                        if(!edge->scheme->scheme_applied){
                            all_schemes_applied = false;
                        }
                    }
                    if(all_schemes_applied){
                        // all schemes using source have been applied, but source has not been compressed yet, mark for BB compression
                        column_status[scheme->columns[0]] = ColumnStatus::None;
                    }
                }
            }
            else{
                for(size_t i=0; i<new_columns.size(); i++){
                    compressed_column_chunks[scheme->columns[i]] = std::move(new_columns[i]);   
                }
            }
        }
    }

    // 2.
    for(size_t i=0; i<compressed_column_chunks.size(); i++){
        if(column_status[i]==ColumnStatus::None){
            assert(compressed_column_chunks[i].size()==0);

            std::vector<uint8_t> output(sizeof(C3Chunk) + 2 * row_group->columns[i]->size);
            C3Chunk* outputChunk = reinterpret_cast<C3Chunk*>(output.data());
            outputChunk->compression_type = static_cast<uint8_t>(SchemeType::BB);
            outputChunk->btrblocks_ColumnChunkMeta_offset = 0;

            auto compressed_size = btrblocks::Datablock::compress(*row_group->columns[i], outputChunk->data, btrBlocksSchemes[i]->get_best_scheme().first, nullptr);
            
            output.resize(sizeof(C3Chunk) + compressed_size);
            compressed_column_chunks[i] = std::move(output);
        }
    }

    double source_scheme_count = 0;
    for(auto node: graph->columnNodes){
        if(node->final_outgoingEdges.size()>0){
            average_schemes_per_source += node->final_outgoingEdges.size();
            source_scheme_count++;
        };
    }
    if(source_scheme_count > 0){
        average_schemes_per_source /= source_scheme_count;
    }

    return compressed_column_chunks;

}

std::vector<std::vector<uint8_t>> C3::apply_scheme(std::shared_ptr<CompressionScheme> scheme){
    std::vector<std::vector<uint8_t>> compressed;
    switch(scheme->type){
        case SchemeType::Equality:{

            bool skip_source_compression = false;
            // int target_columns_left = 0;
            int schemes_already_applied = 0;
            for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                if(scheme != out_edge->scheme && !out_edge->scheme->scheme_applied){
                    // assert(out_edge->scheme->type == SchemeType::Equality || out_edge->scheme->type == SchemeType::Numerical);
                    skip_source_compression = true;
                    break;
                }
                else if(out_edge->scheme->scheme_applied){
                    schemes_already_applied++;
                }
            }
            bool previous_scheme_uses_source = schemes_already_applied > 0 ? true : false;

            auto equality_scheme = std::static_pointer_cast<EqualityCompressionScheme>(scheme);

            double exception_ratio;
            compressed = multi_col::Equality::apply_scheme(row_group->columns[scheme->columns[0]], row_group->columns[scheme->columns[1]], exception_ratio, scheme->columns[0], scheme->columns[1], btrBlocksSchemes[scheme->columns[0]]->get_best_scheme().first, equality_scheme,skip_source_compression, previous_scheme_uses_source, btrBlocksSchemes);

            scheme->scheme_applied = true;

            break;
        }
        case SchemeType::Dict_1to1:{
        
            // if not skip source encoding: store source chunk in this scheme
            // if skip source encoding: find scheme which did not skip source encoding
            // pass source chunk to applyscheme (dict & dfor)
            std::vector<uint8_t>* sourceChunk;
            bool skip_source_encoding = false;
            int target_columns_left = 0;
            for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                if(Utils::is_dict_scheme(out_edge->scheme->type)){
                    if(out_edge->scheme->scheme_applied){
                        skip_source_encoding = true;
                    }
                    else{
                        target_columns_left++;
                    }
                }
            }
            bool final_target_column = target_columns_left > 1 ? false : true;
            auto dict_scheme = std::static_pointer_cast<Dictionary_1to1_CompressionScheme>(scheme);
           
            if(!skip_source_encoding){
                sourceChunk = &dict_scheme->sourceChunk;
            }
            else{
                for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                    if(out_edge->scheme->type==SchemeType::Dict_1to1){
                        auto temp_scheme = std::static_pointer_cast<Dictionary_1to1_CompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::DFOR){
                        auto temp_scheme = std::static_pointer_cast<DForCompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::Dict_1toN){
                        auto temp_scheme = std::static_pointer_cast<Dict_1toN_CompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::Dict_Sharing){
                        auto temp_scheme = std::static_pointer_cast<DictSharingCompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                }
            }
            assert(sourceChunk != nullptr);

            double exception_ratio;
            compressed = multi_col::Dictionary_1to1::apply_scheme(row_group->columns[scheme->columns[0]], row_group->columns[scheme->columns[1]], dict_scheme, skip_source_encoding, final_target_column, exception_ratio, scheme->columns[0], scheme->columns[1], sourceChunk, btrBlocksSchemes, row_group);

            scheme->scheme_applied = true;
            
            break;
        }
        case SchemeType::Numerical:{

            bool skip_source_compression = false;
            // int target_columns_left = 0;
            int schemes_already_applied = 0;
            for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                if(scheme != out_edge->scheme && !out_edge->scheme->scheme_applied){
                    // assert(out_edge->scheme->type == SchemeType::Equality || out_edge->scheme->type == SchemeType::Numerical);
                    skip_source_compression = true;
                    break;
                }
                else if(out_edge->scheme->scheme_applied){
                    schemes_already_applied++;
                }
            }
            bool previous_scheme_uses_source = schemes_already_applied > 0 ? true : false;

            auto numerical_scheme = std::static_pointer_cast<NumericalCompressionScheme>(scheme);

            compressed = multi_col::Numerical::apply_scheme(row_group->columns[scheme->columns[0]], row_group->columns[scheme->columns[1]], scheme->columns[0], scheme->columns[1], btrBlocksSchemes[scheme->columns[0]]->get_best_scheme().first, numerical_scheme,skip_source_compression, previous_scheme_uses_source, btrBlocksSchemes);

            scheme->scheme_applied = true;
                
            break;
        }
        case SchemeType::DFOR:{
        
            // if not skip source encoding: store source chunk in this scheme
            // if skip source encoding: find scheme which did not skip source encoding
            // pass source chunk to applyscheme (dict & dfor)
            std::vector<uint8_t>* sourceChunk;
            bool skip_source_encoding = false;
            int target_columns_left = 0;
            for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                if(Utils::is_dict_scheme(out_edge->scheme->type)){
                    if(out_edge->scheme->scheme_applied){
                        skip_source_encoding = true;
                    }
                    else{
                        target_columns_left++;
                    }
                }
            }
            bool final_target_column = target_columns_left > 1 ? false : true;
            auto dfor_scheme = std::static_pointer_cast<DForCompressionScheme>(scheme);

            if(!skip_source_encoding){
                sourceChunk = &dfor_scheme->sourceChunk;
            }
            else{
                for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                    if(out_edge->scheme->type==SchemeType::Dict_1to1){
                        auto temp_scheme = std::static_pointer_cast<Dictionary_1to1_CompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::DFOR){
                        auto temp_scheme = std::static_pointer_cast<DForCompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::Dict_1toN){
                        auto temp_scheme = std::static_pointer_cast<Dict_1toN_CompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::Dict_Sharing){
                        auto temp_scheme = std::static_pointer_cast<DictSharingCompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                }
            }
            assert(sourceChunk != nullptr);

            auto source_unique_count = btrBlocksSchemes[scheme->columns[0]]->get_unique_count();

            compressed = multi_col::DFOR::apply_scheme(row_group->columns[scheme->columns[0]], row_group->columns[scheme->columns[1]], scheme->columns[0], scheme->columns[1], skip_source_encoding, final_target_column, dfor_scheme, source_unique_count, sourceChunk, btrBlocksSchemes);

            scheme->scheme_applied = true;
                
            break;
        }
        case SchemeType::Dict_1toN:{
        
            // if not skip source encoding: store source chunk in this scheme
            // if skip source encoding: find scheme which did not skip source encoding
            // pass source chunk to applyscheme (dict & dfor)
            std::vector<uint8_t>* sourceChunk = nullptr;
            bool skip_source_encoding = false;
            int target_columns_left = 0;
            auto a = scheme->columns[0];
            for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                if(Utils::is_dict_scheme(out_edge->scheme->type)){   
                    if(out_edge->scheme->scheme_applied){
                        skip_source_encoding = true;
                    }
                    else{
                        target_columns_left++;
                    }
                }
            }
            bool final_target_column = target_columns_left > 1 ? false : true;
            auto dict_1toN_scheme = std::static_pointer_cast<Dict_1toN_CompressionScheme>(scheme);

            if(!skip_source_encoding){
                sourceChunk = &dict_1toN_scheme->sourceChunk;
            }
            else{
                for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                    if(out_edge->scheme->type==SchemeType::Dict_1to1){
                        auto temp_scheme = std::static_pointer_cast<Dictionary_1to1_CompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::DFOR){
                        auto temp_scheme = std::static_pointer_cast<DForCompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::Dict_1toN){
                        auto temp_scheme = std::static_pointer_cast<Dict_1toN_CompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                    else if(out_edge->scheme->type == SchemeType::Dict_Sharing){
                        auto temp_scheme = std::static_pointer_cast<DictSharingCompressionScheme>(out_edge->scheme);
                        if(temp_scheme->sourceChunk.size()>0){
                            sourceChunk = &temp_scheme->sourceChunk;
                            break;
                        }
                    }
                }
            }
            assert(sourceChunk != nullptr);

            auto source_unique_count = btrBlocksSchemes[scheme->columns[0]]->get_unique_count();

            compressed = multi_col::Dictionary_1toN::apply_scheme(row_group->columns[scheme->columns[0]], row_group->columns[scheme->columns[1]], dict_1toN_scheme, skip_source_encoding, final_target_column, scheme->columns[0], scheme->columns[1], sourceChunk, btrBlocksSchemes[scheme->columns[0]], btrBlocksSchemes, row_group);

            scheme->scheme_applied = true;
                
            break;
        }
        case SchemeType::Dict_Sharing:{

            // if not skip source encoding: store source chunk in this scheme
            // if skip source encoding: find scheme which did not skip source encoding
            // pass source chunk to applyscheme (dict & dfor)
            std::vector<uint8_t>* sourceChunk = nullptr;
            int target_columns_left = 0;
            auto a = scheme->columns[0];
            for(const auto& out_edge: graph->columnNodes[scheme->columns[0]]->final_outgoingEdges){
                if(!out_edge->scheme->scheme_applied){
                    target_columns_left++;
                }
            }
            bool final_target_column = target_columns_left > 1 ? false : true;
            auto dictSharing_scheme = std::static_pointer_cast<DictSharingCompressionScheme>(scheme);

            sourceChunk = &dictSharing_scheme->sourceChunk;
            assert(sourceChunk != nullptr);

            compressed = multi_col::DictSharing::apply_scheme(row_group->columns[scheme->columns[0]], row_group->columns[scheme->columns[1]], std::static_pointer_cast<DictSharingCompressionScheme>(scheme), scheme->columns[0], scheme->columns[1], false, final_target_column, sourceChunk, btrBlocksSchemes);

            scheme->scheme_applied = true;
                
            break;
        }
        default: std::cout << "Can't apply C3 scheme" << std::endl;
    }

    assert(compressed.size() == scheme->columns.size());
    return compressed;
}

}
