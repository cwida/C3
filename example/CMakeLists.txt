add_executable(c3_example ${CMAKE_CURRENT_SOURCE_DIR}/c3_example.cpp)
target_include_directories(c3_example PRIVATE ${CMAKE_SOURCE_DIR}/public_bi/include)
target_link_libraries(c3_example PUBLIC btrfiles)