# <div align="center"> C3: Compressing Correlated Columns</div>

## C3:
C3 is a project initiated at CWI, to explore the compression benefits of exploiting column correlations. We introduce six new multi-column lightweight compression schemes that exploit different types of correlations between pairs of columns. We implemented a compression framework which uses both our multi-column compression schemes and conventional "single-column" compression schemes in combination. The addition of the multi-column schemes allows the framework to exploit any correlations it detects, and fall back to single-column schemes if none are detected for a column. The code base is forked from the [BtrBlocks](https://github.com/maxi-k/btrblocks) project from TU Munich.

## How to Build:
### Requirements: 
1) __Clang++__
2) __CMake__ 3.20 or higher

```shell
    mkdir build ; cd build
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make
```

## Build and Run Example:

```shell
    cd build/example
    make
    ./c3_example
```