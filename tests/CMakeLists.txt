add_executable(test_public_bi ${CMAKE_CURRENT_SOURCE_DIR}/publicBI.cpp)
target_include_directories(test_public_bi PRIVATE ${CMAKE_SOURCE_DIR}/public_bi/include)
target_link_libraries(test_public_bi PUBLIC btrfiles gtest)