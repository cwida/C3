#include <limits.h>
#include <algorithm>
#include "c3/Utils.hpp"
#include "CorrelationGraph.hpp"

namespace c3 {

CorrelationEdge::CorrelationEdge(GraphNode sourceCol, GraphNode targetCol, std::shared_ptr<CompressionScheme> scheme)
:sourceCol(sourceCol), targetCol(targetCol), scheme(scheme)
{}

CorrelationNode::CorrelationNode(int idx)
:node_index(idx)
{}

// size_t CorrelationNode::metadata_size(){
//     if(final_outgoingEdges.size()>0){
//         // this is source node
//         assert(final_incomingEdge == nullptr);
//         return final_outgoingEdges[0]->scheme->metadata_size(0);
//     }
//     else if(final_incomingEdge != nullptr){
//         // this is target node
//         return final_incomingEdge->scheme->metadata_size(1);
//     }
//     else{
//         return 0;
//     }
// }

CorrelationGraph::CorrelationGraph(size_t num_columns)
{
    for(size_t i=0; i<num_columns; i++){
        columnNodes.push_back(std::make_shared<CorrelationNode>(i));
    }
}

// void CorrelationGraph::print_edges(){
    // for(const auto& node: columnNodes){
    //     std::cout << node->sourceCol->name << std::endl;
    //     if(node->one_hot_group!=-1){
    //         std::cout << "-> One-Hot (Group " << node->one_hot_group << ")" << std::endl;
    //     }
    //     else{
    //         for(const auto& edge: node->outgoingEdges){
    //             std::cout << "->" << edge->targetCol->sourceCol->name << ": " << correlationType_toString(edge->corr_type) << "(" << edge->scheme->estimatedCompressionRatio << ")" << std::endl;
    //         }
    //     }
    // }
// }

// // remove edge from source node and target node 
// void remove_edges(std::vector<std::shared_ptr<CorrelationEdge>> edges){

//     for(auto& edge: edges){

//         bool found = false;
//         for(size_t i=0; i<edge->sourceCol->outgoingEdges.size(); i++){
//             if(edge == edge->sourceCol->outgoingEdges[i]){
//                 edge->sourceCol->outgoingEdges.erase(edge->sourceCol->outgoingEdges.begin()+i);
//                 found = true;
//                 break;
//             }
//         }
//         assert(found);
//         found = false;
//         for(size_t i=0; i<edge->targetCol->incomingEdges.size(); i++){
//             if(edge == edge->targetCol->incomingEdges[i]){
//                 edge->targetCol->incomingEdges.erase(edge->targetCol->incomingEdges.begin()+i);
//                 found = true;
//                 break;
//             }
//         }
//         assert(found);
//     }

// }

// std::shared_ptr<CorrelationNode> CorrelationGraph::get_best_target_node(){
    
//     // nodes with minimum outdegree
//     int min_outdegree = INT_MAX;
//     std::vector<std::shared_ptr<CorrelationNode>> nodes_min_outdegree;
//     for(const auto& node: columnNodes){
//         if(node->is_deleted){
//             continue;
//         }

//         int outdegree = node->outgoingEdges.size();
//         if(outdegree < min_outdegree){
//             min_outdegree = outdegree;
//             nodes_min_outdegree = {node};
//         }
//         else if(outdegree == min_outdegree){
//             nodes_min_outdegree.push_back(node);
//         }
//     }
    
//     // nodes with minimum indegree of parents
//     int min_indegree_parents = INT_MAX;
//     std::vector<std::shared_ptr<CorrelationNode>> nodes_min_indegree_parents;
//     for(const auto& node: nodes_min_outdegree){
//         int min_indegree_parents_local = INT_MAX;
//         for(const auto& incoming_edge: node->incomingEdges){
//             int indegree_parents = incoming_edge->sourceCol->incomingEdges.size();
//             if(indegree_parents < min_indegree_parents_local){
//                 min_indegree_parents_local = indegree_parents;
//             }
//         }
//         if(min_indegree_parents_local < min_indegree_parents){
//             min_indegree_parents = min_indegree_parents_local;
//             nodes_min_indegree_parents = {node};
//         }
//         else if(min_indegree_parents_local == min_indegree_parents){
//             nodes_min_indegree_parents.push_back(node);
//         }
//     }

//     // nodes with minimum indegree
//     int min_indegree = INT_MAX;
//     std::vector<std::shared_ptr<CorrelationNode>> nodes_min_indegree;
//     for(const auto& node: nodes_min_indegree_parents){
//         int indegree = node->incomingEdges.size();
//         if(indegree < min_indegree){
//             min_indegree = indegree;
//             nodes_min_indegree = {node};
//         }
//         else if(indegree == min_indegree){
//             nodes_min_indegree.push_back(node);
//         }
//     }

//     // assert(!nodes_min_indegree.empty());
//     if(nodes_min_indegree.empty()){
//         return nullptr;
//     }
//     else{
//         return nodes_min_indegree[0];
//     }
// }

// std::shared_ptr<CorrelationEdge> CorrelationGraph::get_best_incoming_edge(std::shared_ptr<CorrelationNode> target_node){
    
//     if(target_node->incomingEdges.empty()){
//         return nullptr;
//     }

//     // get minimum parent indegree edge
//     int min_parent_indegree = INT_MAX;
//     std::vector<std::shared_ptr<CorrelationEdge>> edges_min_parent_indegree;
//     for(const auto& edge: target_node->incomingEdges){
//         int parent_indegree = edge->sourceCol->incomingEdges.size();
//         if(parent_indegree < min_parent_indegree){
//             min_parent_indegree = parent_indegree;
//             edges_min_parent_indegree = {edge};
//         }
//         else if(parent_indegree == min_parent_indegree){
//             edges_min_parent_indegree.push_back(edge);
//         }
//     }

//     // get maximum corr_coef edge
//     int max_corr_coef = 0;
//     std::vector<std::shared_ptr<CorrelationEdge>> edges_max_corr_coef;
//     for(const auto& edge: edges_min_parent_indegree){
//         if(edge->scheme->estimatedCompressionRatio > max_corr_coef){
//             max_corr_coef = edge->scheme->estimatedCompressionRatio;
//             edges_max_corr_coef = {edge};
//         }
//         else if(edge->scheme->estimatedCompressionRatio == max_corr_coef){
//             edges_max_corr_coef.push_back(edge);
//         }
//     }

//     // get equality correlation edges if present, faster decompression (?)
//     std::vector<std::shared_ptr<CorrelationEdge>> edges_equality_corr;
//     for(const auto& edge: edges_max_corr_coef){
//         if(edge->scheme->type == SchemeType::Equality){
//             edges_equality_corr.push_back(edge);
//         }
//     }
//     if(edges_equality_corr.empty()){
//         edges_equality_corr = edges_max_corr_coef;
//     }

//     // get max parent outdegree edge
//     int max_outdegree = 0;
//     std::vector<std::shared_ptr<CorrelationEdge>> edges_max_outdegree;
//     for(const auto& edge: edges_equality_corr){
//         int outdegree = edge->sourceCol->outgoingEdges.size();
//         if(outdegree > max_outdegree){
//             max_corr_coef = outdegree;
//             edges_max_outdegree = {edge};
//         }
//         else if(outdegree == max_outdegree){
//             edges_max_outdegree.push_back(edge);
//         }
//     }
    
//     assert(edges_max_outdegree.size()>0);
//     return edges_max_outdegree[0];
// }

// void CorrelationGraph::finalize_bogdan(){

//     while(true){
//         std::shared_ptr<CorrelationNode> target_node = get_best_target_node();
        
//         if(target_node==nullptr){
//             break;
//         }

//         std::shared_ptr<CorrelationEdge> incoming_edge = get_best_incoming_edge(target_node);
        
//         if(incoming_edge != nullptr){
//             assert(incoming_edge->targetCol->node_index == target_node->node_index);
            
//             auto corr_edge = std::make_shared<CorrelationEdge>(columnNodes[incoming_edge->sourceCol->node_index], columnNodes[incoming_edge->targetCol->node_index], incoming_edge->scheme);
//             columnNodes[incoming_edge->sourceCol->node_index]->final_outgoingEdges.push_back(corr_edge);
//             columnNodes[incoming_edge->targetCol->node_index]->final_incomingEdges.push_back(corr_edge);
            
//             // remove edges from graph
//             remove_edges(target_node->incomingEdges);
//             remove_edges(target_node->outgoingEdges);
//             remove_edges(incoming_edge->sourceCol->incomingEdges);
//         }

//         // remove node from graph
//         target_node->is_deleted = true;
//     }
// }

bool CorrelationGraph::noConflict(std::shared_ptr<c3::CorrelationEdge> edge){
    
    // each target can only have one source
    if(edge->targetCol->final_incomingEdge != nullptr){
        return false;
    }

    // only length 1 paths
    if(edge->sourceCol->final_incomingEdge != nullptr || !edge->targetCol->final_outgoingEdges.empty()){
        return false;
    }

    // multiple dict sharing schemes on same source not allowed
    if(edge->scheme->type==SchemeType::Dict_Sharing){
        for(const auto& out_edge: edge->sourceCol->final_outgoingEdges){
            if(out_edge->scheme->type==SchemeType::Dict_Sharing){
                return false;
            }
        }
    }

    // shared source nodes
    if(!edge->sourceCol->final_outgoingEdges.empty()){
        if(!config.C3_GRAPH_SHARE_SOURCE_NODES){
            return false;
        }
        
        // bool has_non_dict_scheme = false;
        // bool has_dict_scheme = false;

        // for(const auto& out_edge: edge->sourceCol->final_outgoingEdges){
        //     if(Utils::is_dict_scheme(out_edge->scheme->type)){
        //             has_dict_scheme = true;
        //     }
        //     else if(Utils::is_non_dict_scheme(out_edge->scheme->type)){
        //             has_non_dict_scheme = true;
        //     }
            
        //     // if((edge->scheme->type==SchemeType::DFOR
        //     //     ||  edge->scheme->type==SchemeType::Dict_1to1
        //     //     ||  edge->scheme->type==SchemeType::Dict_1toN)
        //     //     && (out_edge->scheme->type==SchemeType::Equality
        //     //     || out_edge->scheme->type==SchemeType::Numerical)){
        //     //     return false;
        //     // }
        //     // else if((edge->scheme->type==SchemeType::Equality
        //     //     ||  edge->scheme->type==SchemeType::Numerical)
        //     //     && (out_edge->scheme->type==SchemeType::DFOR
        //     //     || out_edge->scheme->type==SchemeType::Dict_1to1
        //     //     ||  out_edge->scheme->type==SchemeType::Dict_1toN)){
        //     //     return false;
        //     // }
        // }

        // // only have conflict if incoming scheme is dict, but source column already compressed by non-dict scheme
        // if(Utils::is_dict_scheme(edge->scheme->type)){
        //     if(has_non_dict_scheme && !has_dict_scheme){
        //         // source column compressed by non-dict scheme
        //         return false;
        //     }
        // }
    }

    return true;
}

std::vector<c3::GraphEdge> CorrelationGraph::finalize(){
    std::vector<c3::GraphEdge> final_edges;

    // simple greedy: from sorted edges, keep picking best edge if compatible
    std::sort(edges.begin(), edges.end(), [&](const GraphEdge a, const GraphEdge b){
        int bytes_saved_a = a->scheme->estimated_bytes_saved_source + a->scheme->estimated_bytes_saved_target;
        int bytes_saved_b = b->scheme->estimated_bytes_saved_source + b->scheme->estimated_bytes_saved_target;
        return bytes_saved_a > bytes_saved_b;
    });

    for(size_t i=0; i<edges.size(); i++){
        auto source = edges[i]->sourceCol->node_index;
        auto target = edges[i]->targetCol->node_index;
        auto bytes_saved = edges[i]->scheme->estimated_bytes_saved_source + edges[i]->scheme->estimated_bytes_saved_target;
        if(noConflict(edges[i])){
            edges[i]->sourceCol->final_outgoingEdges.push_back(edges[i]);
            edges[i]->targetCol->final_incomingEdge = edges[i];
            final_edges.push_back(edges[i]);

            if(config.FINALIZE_GRAPH_RESORT_EDGES){
                if(Utils::is_dict_scheme(edges[i]->scheme->type)){
                    // update all edges using same source: set source bytes saved to 0
                    for(auto& edge: edges[i]->sourceCol->outgoingEdges){
                        edge->scheme->estimated_bytes_saved_source = 0;
                    }

                    // re-sort remaining edges
                    std::sort(edges.begin()+i+1, edges.end(), [&](const GraphEdge a, const GraphEdge b){
                        int bytes_saved_a = a->scheme->estimated_bytes_saved_source + a->scheme->estimated_bytes_saved_target;
                        int bytes_saved_b = b->scheme->estimated_bytes_saved_source + b->scheme->estimated_bytes_saved_target;
                        return bytes_saved_a > bytes_saved_b;
                    });
                }
            }
        }
    }

    return final_edges;
}

// std::pair<size_t, CorrelationGraph> find_optimum(CorrelationGraph picked_schemes, size_t picked_schemes_bytes_saved, GraphEdges available_schemes){
    
    // auto foo = available_schemes.size();
    // static int counter = 0;
    // counter++;
    // if(counter%100000==0){
    //     std::cout << counter << std::endl;
    // }

    // std::pair<size_t, CorrelationGraph> best_picked_schemes = {picked_schemes_bytes_saved, picked_schemes};
    // for(size_t i=0; i<available_schemes.size(); i++){
    //     if(picked_schemes.noConflict(available_schemes[i])){

    //         auto new_picked_schemes = picked_schemes.get_copy();
            
    //         size_t source_col_idx = available_schemes[i]->scheme->columns[0];
    //         size_t target_col_idx = available_schemes[i]->scheme->columns[1];
    //         auto new_edge = std::make_shared<CorrelationEdge>(new_picked_schemes.columnNodes[source_col_idx], new_picked_schemes.columnNodes[target_col_idx], available_schemes[i]->scheme);
    //         new_picked_schemes.columnNodes[source_col_idx]->final_outgoingEdges.push_back(new_edge);
    //         new_picked_schemes.columnNodes[target_col_idx]->final_incomingEdge = new_edge;
    //         new_picked_schemes.edges.push_back(new_edge);
            
    //         auto new_picked_schemes_bytes_saved = picked_schemes_bytes_saved + available_schemes[i]->scheme->c3_saved_bytes;
      
    //         auto new_availale_schemes = available_schemes;
    //         new_availale_schemes.erase(new_availale_schemes.begin()+i);

    //         auto optimal_picked_schemes = find_optimum(new_picked_schemes, new_picked_schemes_bytes_saved, new_availale_schemes);
    //         if(optimal_picked_schemes.first > best_picked_schemes.first){
    //             best_picked_schemes = optimal_picked_schemes;
    //         }
    //     }
    // }

    // return best_picked_schemes;
// }

// std::pair<size_t, CorrelationGraph> CorrelationGraph::finalize_optimum(std::vector<size_t> bb_compressed_sizes){
//     return find_optimum(get_copy(), 0, edges);
// }

// // copy only final edges
// CorrelationGraph CorrelationGraph::get_copy(){
//     CorrelationGraph new_graph(columnNodes.size());

//     for(const auto& col: columnNodes){
//         for(const auto& final_edge: col->final_outgoingEdges){
//             size_t source_col_idx = final_edge->scheme->columns[0];
//             size_t target_col_idx = final_edge->scheme->columns[1];
//             auto new_edge = std::make_shared<CorrelationEdge>(new_graph.columnNodes[source_col_idx], new_graph.columnNodes[target_col_idx], final_edge->scheme);
//             new_graph.columnNodes[source_col_idx]->final_outgoingEdges.push_back(new_edge);
//             new_graph.columnNodes[target_col_idx]->final_incomingEdge = new_edge;
//             new_graph.edges.push_back(new_edge);
//         }
//     }

//     return new_graph;
// }

GraphEdge CorrelationGraph::get_deep_copy_edge(GraphEdge edge, CorrelationGraph& graph){

    std::shared_ptr<CompressionScheme> scheme;
    switch(edge->scheme->type){
        case SchemeType::Equality:{
            scheme = std::make_shared<EqualityCompressionScheme>(edge->scheme->columns[0], edge->scheme->columns[1], edge->scheme->estimated_bytes_saved_source, edge->scheme->estimated_bytes_saved_target);
            break;
        }
        case SchemeType::Dict_1toN:{
            scheme = std::make_shared<Dict_1toN_CompressionScheme>(edge->scheme->columns[0], edge->scheme->columns[1], edge->scheme->estimated_bytes_saved_source, edge->scheme->estimated_bytes_saved_target);   
            break;
        }
        case SchemeType::Dict_1to1:{
            scheme = std::make_shared<Dictionary_1to1_CompressionScheme>(edge->scheme->columns[0], edge->scheme->columns[1], edge->scheme->estimated_bytes_saved_source, edge->scheme->estimated_bytes_saved_target);   
            break;
        }
        case SchemeType::Numerical:{
            auto numerical_scheme = std::static_pointer_cast<NumericalCompressionScheme>(edge->scheme);
            scheme = std::make_shared<NumericalCompressionScheme>(edge->scheme->columns[0], edge->scheme->columns[1], numerical_scheme->slope, numerical_scheme->intercept, edge->scheme->estimated_bytes_saved_source, edge->scheme->estimated_bytes_saved_target);   
            break;
        }
        case SchemeType::DFOR :{
            scheme = std::make_shared<DForCompressionScheme>(edge->scheme->columns[0], edge->scheme->columns[1], edge->scheme->estimated_bytes_saved_source, edge->scheme->estimated_bytes_saved_target);   
            break;
        }
        case SchemeType::Dict_Sharing :{
            scheme = std::make_shared<DictSharingCompressionScheme>(edge->scheme->columns[0], edge->scheme->columns[1], edge->scheme->estimated_bytes_saved_source, edge->scheme->estimated_bytes_saved_target);   
            break;
        }
        default: std::cerr << "This scheme is not supposed to be here..." << std::endl;
    }

    return std::make_shared<CorrelationEdge>(graph.columnNodes[scheme->columns[0]], graph.columnNodes[scheme->columns[1]], scheme);
}

// copy all edges and schemes, don't finalize
CorrelationGraph CorrelationGraph::get_deep_copy_edges(){
    CorrelationGraph new_graph(columnNodes.size());

    for(const auto& col: columnNodes){
        for(const auto& edge: col->outgoingEdges){
            size_t source_col_idx = edge->scheme->columns[0];
            size_t target_col_idx = edge->scheme->columns[1];

            auto new_edge = get_deep_copy_edge(edge, new_graph);            
            new_graph.columnNodes[source_col_idx]->outgoingEdges.push_back(new_edge);
            new_graph.columnNodes[target_col_idx]->incomingEdges.push_back(new_edge);
            new_graph.edges.push_back(new_edge);
        }
    }

    return new_graph;
}

void CorrelationGraph::remove_final_edge(std::shared_ptr<CompressionScheme> scheme, size_t source_idx, size_t target_idx){
    // remove from outgoing edges
    bool found_out = false;
    c3::GraphEdges* out_edges = &columnNodes[source_idx]->final_outgoingEdges;
    for(size_t i=0; i<(*out_edges).size(); i++){
        if(scheme == (*out_edges)[i]->scheme){
            out_edges->erase(out_edges->begin()+i);
            found_out = true;
            break;
        }
    }
    // assert(found_out);

    // remove from incoming edge
    // assert(columnNodes[target_idx]->final_incomingEdge->scheme == scheme);
    columnNodes[target_idx]->final_incomingEdge->scheme = nullptr;
}



} // namespace c3
