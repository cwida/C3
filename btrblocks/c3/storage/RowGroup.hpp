#pragma once
#include "common/Units.hpp"
#include "storage/InputChunk.hpp"
#include <iostream>
#include <memory>

namespace c3 {

class RowGroup {

public:
	RowGroup(std::vector<std::shared_ptr<btrblocks::InputChunk>> columns,
	         std::vector<std::string>                            column_names,
	         size_t                                              tuple_count);

	std::vector<std::shared_ptr<btrblocks::InputChunk>> columns; // TODO why shared pointers?
	std::vector<std::string>                            column_names;
	std::vector<size_t>                                 original_column_sizes;
	// std::vector<bool>                                   is_compressed;
	size_t                                              tuple_count;

	// TODO : Row group samplers should be separate thing.
	std::vector<int> samples;
	void init_samples();
	void printSamples(const size_t& limit);

	RowGroup deep_copy();
};

} // namespace c3
