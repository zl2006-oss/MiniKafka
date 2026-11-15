cmake_minimum_required(VERSION 3.10)

project(minikafka LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra)
endif()

# 未指定时默认带调试信息 + 优化；压测请显式 -DCMAKE_BUILD_TYPE=Release
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

find_package(Threads REQUIRED)

set(MK_MODULES common protocol net storage broker group client)

foreach(module ${MK_MODULES})
  add_library(mk_${module} STATIC src/${module}/${module}.cpp)
  target_include_directories(mk_${module} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_link_libraries(mk_${module} PUBLIC Threads::Threads)
endforeach()

target_sources(mk_common PRIVATE
  src/common/status.cpp
  src/common/json.cpp
  src/common/args.cpp
  src/common/clock.cpp
  src/common/backoff.cpp
  src/common/event_sink.cpp
  src/common/hash.cpp
)

# 依赖方向按 03 §7.3：只允许上层依赖下层。
target_link_libraries(mk_protocol PUBLIC mk_common)
target_link_libraries(mk_net PUBLIC mk_protocol)
target_link_libraries(mk_storage PUBLIC mk_common)
target_link_libraries(mk_group PUBLIC mk_storage)
target_link_libraries(mk_broker PUBLIC mk_group mk_storage mk_net mk_protocol mk_common)
target_link_libraries(mk_client PUBLIC mk_net mk_protocol mk_common)

# M8 压测代码独立目录，编译为静态库后链接进唯一产品可执行文件。
add_library(mk_bench STATIC src/bench/bench.cpp)
target_include_directories(mk_bench PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mk_bench PUBLIC mk_client mk_net mk_protocol mk_common)

add_executable(minikafka src/main.cpp)
target_include_directories(minikafka PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(minikafka PRIVATE mk_broker mk_client mk_bench)

include(CTest)
if(BUILD_TESTING)
  add_executable(mk_unit_tests test/unit/test_smoke.cpp)
  target_include_directories(mk_unit_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_link_libraries(mk_unit_tests PRIVATE mk_broker mk_client mk_bench)
  add_test(NAME unit_smoke COMMAND mk_unit_tests)

  add_executable(mk_common_tests
    test/unit/test_common_main.cpp
    test/unit/test_common_status.cpp
    test/unit/test_common_json.cpp
    test/unit/test_common_args.cpp
    test/unit/test_common_clock.cpp
    test/unit/test_common_backoff.cpp
    test/unit/test_common_event_sink.cpp
    test/unit/test_common_hash.cpp
  )
  target_include_directories(mk_common_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/test/unit
  )
  target_link_libraries(mk_common_tests PRIVATE mk_common)
  add_test(NAME common_unit COMMAND mk_common_tests)
endif()
