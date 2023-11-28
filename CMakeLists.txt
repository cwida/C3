cmake_minimum_required(VERSION 3.24)
project(C3 VERSION 0.0.1)

#-----------------------------------------------------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -march=native -Wall -Wextra")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")
#-----------------------------------------------------------------------------------------------------------------------
include(FetchContent)
include(CheckCXXCompilerFlag)
include(CMakePrintHelpers)
include(CTest)
#-----------------------------------------------------------------------------------------------------------------------
FetchContent_Declare(
        roaring
        GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
        GIT_TAG v0.2.66
        GIT_SHALLOW TRUE)

set(ENABLE_ROARING_TESTS OFF CACHE INTERNAL "")

set(ROARING_BUILD_STATIC ON CACHE INTERNAL "")
FetchContent_MakeAvailable(roaring)

FetchContent_GetProperties(roaring)
SET(CPP_ROARING_HEADERS ${roaring_SOURCE_DIR}/cpp/roaring64map.hh ${roaring_SOURCE_DIR}/cpp/roaring.hh)
file(COPY ${CPP_ROARING_HEADERS} DESTINATION ${roaring_SOURCE_DIR}/include/roaring)

include_directories(roaring)
include_directories(${roaring_SOURCE_DIR}/include)
#-----------------------------------------------------------------------------------------------------------------------

# TODO REMOVE
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
# btrblocks ------------------------------------------------------------------------------------------------------------

option(NO_SIMD "NO_SIMD" OFF)
option(WITH_LOGGING "WITH_LOGGING" OFF)
option(SAMPLING_TEST_MODE "SAMPLING_TEST_MODE" OFF)
option(ENABLE_FOR_SCHEME "ENABLE_FOR_SCHEME" OFF)
option(BUILD_SHARED_LIBRARY "BUILD_SHARED_LIBRARY" OFF)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG ON)

set(CMAKE_VENDOR_DIR "${CMAKE_SOURCE_DIR}/cmake/")


# Compression library dependencies
include("${CMAKE_VENDOR_DIR}/fastpfor.cmake")
include("${CMAKE_VENDOR_DIR}/fsst.cmake")
include("${CMAKE_VENDOR_DIR}/dynamic_bitset.cmake")
include("${CMAKE_VENDOR_DIR}/tbb.cmake")
include("${CMAKE_VENDOR_DIR}/gflags.cmake")
include("${CMAKE_VENDOR_DIR}/spdlog.cmake")
include("${CMAKE_VENDOR_DIR}/yaml-cpp.cmake")
include("${CMAKE_VENDOR_DIR}/csv-parser.cmake")
include("${CMAKE_VENDOR_DIR}/googletest.cmake")
include("${CMAKE_VENDOR_DIR}/benchmark.cmake")
include("${CMAKE_VENDOR_DIR}/gdouble.cmake")
include("${CMAKE_VENDOR_DIR}/turbo.cmake")
include("${CMAKE_VENDOR_DIR}/lz4.cmake")

# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/btrblocks/local.cmake")  # btrblocks library
include("${CMAKE_SOURCE_DIR}/btrfiles/local.cmake")   # btrblocks files library

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
cmake_print_variables(CMAKE_CXX_COMPILER_ID)
cmake_print_variables(CMAKE_CXX_COMPILER_VERSION)
cmake_print_variables(CMAKE_CXX_FLAGS_DEBUG)
cmake_print_variables(CMAKE_CXX_FLAGS_RELEASE)
cmake_print_variables(CMAKE_C_FLAGS_DEBUG)
cmake_print_variables(CMAKE_C_FLAGS_RELEASE)
cmake_print_variables(CMAKE_CXX_FLAGS)
cmake_print_variables(CMAKE_C_FLAGS)
cmake_print_variables(CMAKE_BUILD_TYPE)
cmake_print_variables(NO_SIMD)
cmake_print_variables(WITH_LOGGING)
cmake_print_variables(SAMPLING_TEST_MODE)
cmake_print_variables(ENABLE_FOR_SCHEME)
cmake_print_variables(BUILD_SHARED_LIBRARY)
cmake_print_variables(GTEST_INCLUDE_DIR)
cmake_print_variables(GTEST_LIBRARY_PATH)

# C3 -------------------------------------------------------------------------------------------------------------------

# tests: ---------------------------------------------------------------------------------------------------------------
add_subdirectory(tests)

# example: -------------------------------------------------------------------------------------------------------------
add_subdirectory(example)

# playground: -------------------------------------------------------------------------------------------------------------
add_subdirectory(playground)