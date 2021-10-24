cmake_minimum_required(VERSION 3.5)
project(GutterTree)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

#Uncomment to enable debug
#set(ENV{DEBUG} ON)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  message("Adding GNU compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  message("Adding MSVC compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
  message("Adding AppleClang compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
else()
  message("${CMAKE_CXX_COMPILER_ID} not recognized, no flags added")
endif()

message("${CMAKE_MODULE_PATH}")
include("DownloadProject.cmake")

#Find or download GTest
find_package(GTest CONFIG)
if(NOT GTest_FOUND)
  if(NOT AUTODOWNLOAD)
    message(FATAL_ERROR "GTest config not found.\n"
      "Set GTest_DIR to a directory containing one of \n"
      "  GTestConfig.cmake\n"
      "  gtest-config.cmake\n"
      "or use -DAUTODOWNLOAD:BOOL=ON to automatically download")
  else()
    #Download googletest
    download_project(
      PROJ googletest
      GIT_REPOSITORY "https://github.com/google/googletest.git"
      GIT_TAG "release-1.10.0"
    )

    #As specified in the googletest docs, need to set
    #gtest_force_shared_crt for windows visual studio.
    if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
      set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    endif()

    add_subdirectory(
      "${googletest_SOURCE_DIR}"
      "${googletest_BINARY_DIR}"
      EXCLUDE_FROM_ALL
    )

    #Add aliases to targets
    add_library(GTest::gtest ALIAS gtest)
    add_library(GTest::gtest_main ALIAS gtest_main)
    add_library(GTest::gmock ALIAS gmock)
    add_library(GTest::gmock_main ALIAS gmock_main)
  endif()
endif()

add_library(GutterTree
    src/gutter_tree.cpp
    include/gutter_tree.h
    src/buffer_control_block.cpp
    include/buffer_control_block.h
    src/standalone_gutters.cpp
    include/standalone_gutters.h
    src/work_queue.cpp
    include/work_queue.h
    include/types.h)
target_link_libraries(GutterTree PRIVATE GTest::gtest)
# optimize unless debug
if (DEFINED ENV{DEBUG})
  message("Disabling optimizations and enabling debug symbols")
  target_compile_options(GutterTree PRIVATE -g)
else ()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    target_compile_options(GutterTree PRIVATE -O3)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    target_compile_options(GutterTree PRIVATE /O2)
endif()
endif()
# different OSes have different ways of efficiently pre-allocating large files
if (UNIX AND NOT APPLE)
  message("Enabling Fallocate for a linux system")
  target_compile_options(GutterTree PRIVATE -DLINUX_FALLOCATE)
elseif(WIN32)
  message("Using fileapi for Windows")
  target_compile_options(GutterTree PRIVATE -DWINDOWS_FILEAPI)
else()
  message("Using fcntl for an Apple or other unknown system")
  target_compile_options(GutterTree PRIVATE -DPOSIX_FCNTL)
endif ()
set_target_properties(GutterTree PROPERTIES PUBLIC_HEADER 
  "include/gutter_tree.h;include/buffer_control_block.h;include/standalone_gutters.h;include/work_queue.h;include/types.h"
)

add_executable(buffering_tests
  test/runner.cpp
  test/gutter_tree_test.cpp
  test/standalone_gutters_test.cpp)
target_link_libraries(buffering_tests PRIVATE GTest::gtest GutterTree)
# optimize unless debug
if (DEFINED ENV{DEBUG})
  message("Disabling optimizations and enabling debug symbols")
  target_compile_options(buffering_tests PRIVATE -g)
else ()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    target_compile_options(buffering_tests PRIVATE -O3)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    target_compile_options(buffering_tests PRIVATE /O2)
  endif()
endif ()

add_executable(guttertree_experiment
  experiment/runner.cpp
  experiment/experiment.cpp)
target_link_libraries(guttertree_experiment PRIVATE GTest::gtest GutterTree)
# optimize unless debug
if (DEFINED ENV{DEBUG})
  message("Disabling optimizations and enabling debug symbols")
  target_compile_options(guttertree_experiment PRIVATE -g)
else ()
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    target_compile_options(guttertree_experiment PRIVATE -O3)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    target_compile_options(guttertree_experiment PRIVATE /O2)
  endif()
endif ()


#uncomment if manually installing project
#without specifying INSTALL_PREFIX elsewhere
#set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/guttertree/prefix)

# for use when building as an external project
install(TARGETS GutterTree
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
