find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
set(LIB_NAME root2parquet)
set(HEADERS
bigripsData.h
brikenData.h
)

ROOT_GENERATE_DICTIONARY(G__${LIB_NAME}
    ${HEADERS}
    LINKDEF "linkdef_user.h"
    MODULE ${LIB_NAME}
)

function(addExec exec_name)
    add_executable(${exec_name} ${exec_name}.cpp G__${LIB_NAME}.cxx)
    #target_include_directories(${exec_name} PRIVATE ${CMAKE_SOURCE_DIR}/sources/anacore)
    #target_link_directories(${exec_name} PUBLIC ${RDKAFKA_LIB_DIR})
    target_include_directories(${exec_name} PUBLIC /home/ryokoyam/opt/arrow/include ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_directories(${exec_name} PUBLIC /home/ryokoyam/opt/arrow/lib64)
    target_link_libraries(${exec_name} arrow parquet ${ROOT_LIBRARIES})

    install(TARGETS ${exec_name}
        RUNTIME DESTINATION bin)
endfunction()

addExec(bigrips2parquet)
addExec(briken2parquet)
