add_executable(thomas ${CMAKE_CURRENT_SOURCE_DIR}/thomas.cpp)
target_include_directories(thomas PRIVATE ${CMAKE_SOURCE_DIR}/public_bi/include)
target_link_libraries(thomas PUBLIC btrfiles)

add_executable(compressC3 ${CMAKE_CURRENT_SOURCE_DIR}/compressC3.cpp)
target_include_directories(compressC3 PRIVATE ${CMAKE_SOURCE_DIR}/public_bi/include)
target_link_libraries(compressC3 PUBLIC btrfiles)

add_executable(compressBB ${CMAKE_CURRENT_SOURCE_DIR}/compressBB.cpp)
target_include_directories(compressBB PRIVATE ${CMAKE_SOURCE_DIR}/public_bi/include)
target_link_libraries(compressBB PUBLIC btrfiles)

add_executable(benchmarks ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks.cpp)
target_include_directories(benchmarks PRIVATE ${CMAKE_SOURCE_DIR}/public_bi/include)
target_link_libraries(benchmarks PUBLIC btrfiles)