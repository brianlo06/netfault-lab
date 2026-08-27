cmake_minimum_required(VERSION 3.25)

project(NetFaultLab VERSION 0.1.0 LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "NetFault Lab requires Linux because it uses epoll, eventfd, and Linux socket semantics. Build with the provided container on non-Linux hosts.")
endif()

option(NETFAULT_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(netfault_warnings INTERFACE)
target_compile_options(netfault_warnings INTERFACE
  -Wall
  -Wextra
  -Wpedantic
  -Wconversion
  -Wsign-conversion
  -Wshadow
)
if(NETFAULT_WARNINGS_AS_ERRORS)
  target_compile_options(netfault_warnings INTERFACE -Werror)
endif()

add_library(netfault_core STATIC
  src/common/endpoint.cpp
  src/common/logger.cpp
  src/common/relay.cpp
  src/common/socket_io.cpp
)
target_include_directories(netfault_core PUBLIC include)
target_link_libraries(netfault_core PUBLIC netfault_warnings)

add_library(netfault_proxy_lib STATIC src/proxy/proxy.cpp)
target_include_directories(netfault_proxy_lib PUBLIC include)
target_link_libraries(netfault_proxy_lib PUBLIC netfault_core netfault_warnings)

add_executable(netfault-proxy src/proxy/main.cpp)
target_link_libraries(netfault-proxy PRIVATE netfault_proxy_lib netfault_warnings)

add_executable(netfault-server src/server/main.cpp)
target_link_libraries(netfault-server PRIVATE netfault_core netfault_warnings)

add_executable(netfault-client src/client/main.cpp)
target_link_libraries(netfault-client PRIVATE netfault_core netfault_warnings)

include(CTest)
if(BUILD_TESTING)
  find_package(Catch2 3 REQUIRED)
  find_package(Python3 3.10 REQUIRED COMPONENTS Interpreter)

  add_executable(netfault-unit-tests
    tests/unit/backpressure_test.cpp
    tests/unit/byte_queue_test.cpp
    tests/unit/connection_state_test.cpp
    tests/unit/endpoint_test.cpp
    tests/unit/relay_test.cpp
  )
  target_link_libraries(netfault-unit-tests PRIVATE netfault_core Catch2::Catch2WithMain netfault_warnings)

  add_test(NAME unit COMMAND netfault-unit-tests)
  add_test(
    NAME milestone1-integration
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/integration/milestone1.py
      --proxy $<TARGET_FILE:netfault-proxy>
      --server $<TARGET_FILE:netfault-server>
      --client $<TARGET_FILE:netfault-client>
  )
  set_tests_properties(milestone1-integration PROPERTIES TIMEOUT 45)

  add_test(
    NAME milestone2-backpressure
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/integration/milestone2_backpressure.py
      --proxy $<TARGET_FILE:netfault-proxy>
      --server $<TARGET_FILE:netfault-server>
      --client $<TARGET_FILE:netfault-client>
  )
  set_tests_properties(milestone2-backpressure PROPERTIES TIMEOUT 45)
endif()
