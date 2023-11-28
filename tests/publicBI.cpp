#include "c3/C3Compressor.hpp"
#include <gtest/gtest.h>


class C3PublicBIParameterizedTestFixture :public ::testing::TestWithParam<c3_bench::Dataset> {
protected:
    c3_bench::Dataset dataset;
};

int public_bi_verify_c3(c3_bench::Dataset& dataset) {
    c3::C3Compressor compressor;
    std::ofstream verify_log_stream(c3_bench::result_path + "tests_log.txt", std::ios::app); 
    verify_log_stream << "Verifying dataset " << dataset.dataset_name << std::endl;

    auto relation_range = compressor.get_btrblocks_relation(dataset);
    relation_range.first.getInputChunks(relation_range.second);
    return compressor.compress_and_verify_c3(verify_log_stream, relation_range.first, relation_range.second, dataset);
}

TEST_P(C3PublicBIParameterizedTestFixture, PublicBIVerifyDecompression) {
    c3_bench::Dataset dataset = GetParam();
    EXPECT_EQ(public_bi_verify_c3(dataset), 0);
}

INSTANTIATE_TEST_CASE_P(
    PublicBITests,
    C3PublicBIParameterizedTestFixture,
    ::testing::ValuesIn(
            c3_bench::datasets_public_bi_small
    ),
    [](const testing::TestParamInfo<c3_bench::Dataset>& info) {
        // use info.param here to generate the test suffix
        std::string dataset_id = std::to_string(std::abs(info.param.dataset_id)); 
        std::string name = dataset_id + "_" + info.param.dataset_name;
        return name;
    }
);

int main(int argc, char **argv) {
  std::ofstream verify_log_stream(c3_bench::result_path + "tests_log.txt"); // clear file
  ::testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}