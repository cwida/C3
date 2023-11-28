#pragma once

#include "CompressionSchemes.hpp"

namespace c3 {

class CorrelationEdge;
class CorrelationNode;

using GraphEdge = std::shared_ptr<CorrelationEdge>;
using GraphEdges = std::vector<std::shared_ptr<CorrelationEdge>>;
using GraphNode = std::shared_ptr<CorrelationNode>;
using GraphNodes = std::vector<std::shared_ptr<CorrelationNode>>;

class CorrelationEdge{
    public:
        CorrelationEdge(GraphNode sourceCol, GraphNode targetCol, std::shared_ptr<CompressionScheme> scheme);
        GraphNode sourceCol;
        GraphNode targetCol;
        std::shared_ptr<CompressionScheme> scheme;
        // bool is_deleted = false;
};

class CorrelationNode{
    public:
        CorrelationNode(int idx);
        GraphEdges outgoingEdges;
        GraphEdges incomingEdges;
        GraphEdges final_outgoingEdges;
        GraphEdge final_incomingEdge = nullptr;
        int node_index;
        // bool is_deleted = false;
        // size_t metadata_size();
};

class CorrelationGraph{
    public:
        CorrelationGraph() = default;
        CorrelationGraph(size_t num_columns);
        GraphNodes columnNodes;
        GraphEdges edges;
        void print_edges();
        std::vector<c3::GraphEdge> finalize();
        // void finalize_bogdan();
        // std::pair<size_t, CorrelationGraph> finalize_optimum(std::vector<size_t> bb_compressed_sizes);
        // CorrelationGraph get_copy();
        CorrelationGraph get_deep_copy_edges();
        bool noConflict(std::shared_ptr<c3::CorrelationEdge> edge);
        void remove_final_edge(std::shared_ptr<CompressionScheme> scheme, size_t source_idx, size_t target_idx);

    private:
        GraphNode get_best_target_node();
        GraphEdge get_best_incoming_edge(GraphNode target_node);
        GraphEdge get_deep_copy_edge(GraphEdge edge, CorrelationGraph& graph);
};

} // namespace cengine
