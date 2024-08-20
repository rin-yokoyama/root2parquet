find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

function(addExec exec_name)
    add_executable(${exec_name} ${exec_name}.cpp)
    #target_include_directories(${exec_name} PRIVATE ${CMAKE_SOURCE_DIR}/sources/anacore)
    #target_link_directories(${exec_name} PUBLIC ${RDKAFKA_LIB_DIR})
    target_link_libraries(${exec_name} arrow parquet ${ROOT_LIBRARIES})

    install(TARGETS ${exec_name}
        RUNTIME DESTINATION bin)
endfunction()

addExec(root2parquet)