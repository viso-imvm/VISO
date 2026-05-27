cmake_minimum_required(VERSION 3.10)
project(MQO_CROWN)

set(BOOST_ROOT "$ENV{HOME}/miniconda3")
set(Boost_NO_SYSTEM_PATHS ON)

if(NOT EXISTS "${BOOST_ROOT}/include/boost")
    unset(BOOST_ROOT)
    unset(Boost_NO_SYSTEM_PATHS)
endif()

find_package(Boost REQUIRED)

if(NOT Boost_FOUND)
    message(STATUS "Using system default Boost include paths")
    set(Boost_INCLUDE_DIRS "/usr/include")
endif()
include_directories(${Boost_INCLUDE_DIRS})

message(STATUS "Boost include directories: ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost library directories: ${Boost_LIBRARY_DIRS}")
message(STATUS "Boost libraries: ${Boost_LIBRARIES}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")

set(Structure
Structure/Query.cpp
Structure/QueryInfo.cpp
Structure/PlanRoot.cpp
Structure/QueryHandler.cpp
Structure/FinalNode.cpp
Structure/SetOutputAttr.cpp
Structure/Optimizer.cpp
)
set(IO
IO/SingleOut.cpp
IO/MQOOut.cpp
IO/Input.cpp
)
set(Util
Util/utils.cpp
Util/IsoCheck.cpp
Util/SubqManager.cpp
)

add_executable(MQO_CROWN
    main.cpp
    ${Structure}
    ${IO}
    ${Util}
)

target_include_directories(MQO_CROWN PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/IO
    ${CMAKE_CURRENT_SOURCE_DIR}/Structure
    ${CMAKE_CURRENT_SOURCE_DIR}/Util
)
