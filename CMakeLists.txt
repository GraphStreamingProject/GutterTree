cmake_minimum_required(VERSION 3.15)
include(FetchContent)

project(GutterTree)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

# Make the default build type Release. If user or another
# project sets a different value than use that
if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "Setting build type to default -- Release")
  set(CMAKE_BUILD_TYPE "Release" CACHE
      STRING "Choose the type of build." FORCE)
endif()
message(STATUS "GutterTree Build Type: ${CMAKE_BUILD_TYPE}")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  message(STATUS "Adding GNU compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  message(STATUS "Adding MSVC compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
  message(STATUS "Adding AppleClang compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
else()
  message(STATUS "${CMAKE_CXX_COMPILER_ID} not recognized, no flags added")
endif()

# Check if this project is the top directory or build type is Debug
# If so, build executables, otherwise, only build libraries
get_directory_property(not_root PARENT_DIRECTORY)
if (not_root AND "${CMAKE_BUILD_TYPE}" STREQUAL "Release")
  set(BUILD_EXE OFF)
else()
  set(BUILD_EXE ON)
  message (STATUS "GutterTree building executables")
endif()

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        release-1.11.0
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
endif()

# Common types and components
FetchContent_Declare(
  GraphZeppelinCommon

  GIT_REPOSITORY  https://github.com/GraphStreamingProject/GraphZeppelinCommon.git
  GIT_TAG         main
)

FetchContent_MakeAvailable(googletest GraphZeppelinCommon)

add_library(GutterTree
  src/work_queue.cpp
  include/work_queue.h
  include/guttering_system.h
  src/guttering_configuration.cpp
  include/guttering_configuration.h
  src/gutter_tree.cpp
  include/gutter_tree.h
  src/buffer_control_block.cpp
  include/buffer_control_block.h
  src/buffer_flusher.cpp
  include/buffer_flusher.h
  src/standalone_gutters.cpp
  include/standalone_gutters.h
  src/cache_guttering.cpp
  include/cache_guttering.h
  include/types.h)
add_dependencies(GutterTree GraphZeppelinCommon)
target_link_libraries(GutterTree PUBLIC gtest GraphZeppelinCommon)
# different OSes have different ways of efficiently pre-allocating large files
if (UNIX AND NOT APPLE)
  message(STATUS "Enabling Fallocate for a linux system")
  target_link_options(GutterTree PUBLIC -fopenmp)
  target_compile_options(GutterTree PRIVATE -fopenmp -DLINUX_FALLOCATE)
elseif(WIN32)
  message(STATUS "Using fileapi for Windows")
  target_compile_options(GutterTree PRIVATE -DWINDOWS_FILEAPI)
else()
  message(STATUS "Using fcntl for an Apple or other unknown system")
  target_compile_options(GutterTree PRIVATE -DPOSIX_FCNTL)
endif ()
target_include_directories(GutterTree PUBLIC include/)

if (BUILD_EXE)
  add_executable(buffering_tests
    test/runner.cpp
    test/guttering_systems_test.cpp
    # test/cache_gutter_tree_test.cpp
  )
  target_link_libraries(buffering_tests PRIVATE GutterTree)

  add_executable(buffering_experiment
    experiment/runner.cpp
    experiment/cache_exp.cpp
    experiment/standalone_exp.cpp)
  target_link_libraries(buffering_experiment PRIVATE GutterTree)
  if (UNIX AND NOT APPLE)
    target_compile_options(buffering_experiment PRIVATE -DLINUX_FALLOCATE)
  endif()
endif()
