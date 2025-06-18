find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

function(addExec exec_name)
    add_executable(${exec_name} ${exec_name}.cpp)
    target_include_directories(${exec_name} PRIVATE ${PAASS_DIR}/include ${CMAKE_SOURCE_DIR}/sources)
    target_link_directories(${exec_name} PRIVATE ${PAASS_DIR}/lib)
    target_link_libraries(${exec_name} arrow parquet ${ROOT_LIBRARIES} PaassRootStruct)
    target_compile_options(${exec_name} PRIVATE -fpermissive)

    install(TARGETS ${exec_name}
        RUNTIME DESTINATION bin)
endfunction()

addExec(root2parquet)
addExec(parquet2root)
addExec(paass2parquet)
