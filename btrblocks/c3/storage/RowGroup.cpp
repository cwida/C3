#include "RowGroup.hpp"
#include "c3/Utils.hpp"
#include <algorithm>
#include <utility>

namespace c3 {

RowGroup::RowGroup(std::vector<std::shared_ptr<btrblocks::InputChunk>> a_columns,
                   std::vector<std::string>                            column_names,
                   size_t                                              tuple_count)
    : columns(std::move(a_columns))
    , column_names(std::move(column_names))
    , tuple_count(tuple_count) {
	init_samples();
	for (auto& col : columns) {
		original_column_sizes.push_back(col->size);
		// is_compressed.push_back(false);
	}
}

RowGroup RowGroup::deep_copy(){
	// need to deep copy std::vector<std::shared_ptr<btrblocks::InputChunk>> columns

	std::vector<std::shared_ptr<btrblocks::InputChunk>> copied_columns;
	for(auto input_chunk: columns){
		// data
	    auto copied_data = std::unique_ptr<uint8_t[]>(new uint8_t[input_chunk->size]); 
		memcpy(copied_data.get(), input_chunk->data.get(), input_chunk->size);

		// nullmap
	    auto copied_nullmap = std::unique_ptr<uint8_t[]>(new uint8_t[input_chunk->tuple_count]); 
		memcpy(copied_nullmap.get(), input_chunk->nullmap.get(), input_chunk->tuple_count);

		auto copied_input_chunk = std::make_shared<btrblocks::InputChunk>(std::move(copied_data), std::move(copied_nullmap), input_chunk->type, input_chunk->tuple_count, input_chunk->size);
		copied_columns.push_back(copied_input_chunk);
	}

	RowGroup copy(copied_columns, column_names, tuple_count);
	return std::move(copy);
}


void RowGroup::printSamples(const size_t& limit) {
	int numSamples = std::min(samples.size(), limit);
	for (const auto col : column_names) {
		std::cout << col << "\t|";
	}
	std::cout << std::endl;
	for (const auto& sample : samples) {
		for (auto col : columns) {
			switch (col->type) {
			case btrblocks::ColumnType::INTEGER: {
				std::cout << col->integers()[sample];
				break;
			}
			case btrblocks::ColumnType::DOUBLE: {
				std::cout << col->doubles()[sample];
				break;
			}
			case btrblocks::ColumnType::STRING: {
				std::cout << col->strings()(sample);
				break;
			}
			default:
				std::cerr << "data type not supported" << std::endl;
			}
			std::cout << "\t|";
		}
		std::cout << std::endl;
	}
	std::cout << std::endl;
}

void RowGroup::init_samples() {
	samples.clear();

	// less tuples than requested sample
	if (config.C3_SAMPLE_NUM_RUNS * config.C3_SAMPLE_RUN_SIZE > tuple_count) {
		for (size_t i = 0; i < tuple_count; i++) {
			samples.push_back(i);
		}
	} else {
		double windowSize = 1.0 * tuple_count / config.C3_SAMPLE_NUM_RUNS;
		
		int random_offset = 0;

		if(config.USE_RANDOM_GENERATOR_SAMPLES){
			// generate random number betwen 0 and excl(windowSize - config.C3_SAMPLE_RUN_SIZE)
			std::random_device rd;   // Used to obtain a seed for the random number engine
			std::mt19937 gen(rd());
			std::uniform_int_distribution<> dis(0, static_cast<int>(windowSize) - config.C3_SAMPLE_RUN_SIZE - 1);
			random_offset = dis(gen);
		}

		for (int run_i = 0; run_i < config.C3_SAMPLE_NUM_RUNS; run_i++) {
			for (int sample_i = 0; sample_i < config.C3_SAMPLE_RUN_SIZE; sample_i++) {
				samples.push_back(static_cast<int>(run_i * windowSize) + random_offset + sample_i);
			}
		}
	}
}

} // namespace c3
