# CMakeList.txt for the ROOTExamples project
# CMake version 3 is required
cmake_minimum_required (VERSION 3.10 FATAL_ERROR)
project(ROOTExamples CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add in CMAKE_PREFIX_PATH the installation prefix for ROOT 
list(APPEND CMAKE_PREFIX_PATH $ENV{ROOTSYS})
# You need to add COMPONENTS according to what you want to do.
find_package(ROOT REQUIRED COMPONENTS Core RIO Tree)
# Include ROOT cmake macros
include(${ROOT_USE_FILE})

# Set the default install prefix to [source_dir]/install
# (You usually don't want to install this project to /usr/local/)
set(CMAKE_INSTALL_PREFIX ${CMAKE_SOURCE_DIR}/install CACHE PATH "install dir" FORCE)
set(PAASS_DIR "$ENV{PAASSSYS}" CACHE PATH "Paass install dir")

add_subdirectory(sources)
