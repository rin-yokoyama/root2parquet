/**
 * @file root2parquet.cpp
 * @author Rin Yokoyama (yokoyama@cns.s.u-tokyo.ac.jp)
 * @brief A converter of simple root tree to Apache parquet file
 * @version 0.1
 * @date 2024-08-19
 *
 * @copyright Copyright (c) 2024
 *
 */
#include <iostream>
#include <map>
#include <functional>
#include "TROOT.h"
#include "TFile.h"
#include "TTreeReader.h"
#include "TTreeReaderValue.h"
#include "TTreeReaderArray.h"
#include "ROOT/RDataFrame.hxx"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/type.h>
#include <parquet/arrow/writer.h>
#include "bigripsData.h"

/** prints usage **/
void usage(char *argv0)
{
    std::cout << "[root2parquet]: Usage: \n"
              << argv0 << " -i [input_root_file_name]\n"
              << "-t [input_tree_name] (default: tree)"
              << "-o [output_file_name] (default: [input_ridf_file_name].parquet)"
              << std::endl;
}

// Main function
int main(int argc, char **argv)
{
    /** parsing commandline arguments **/
    if (argc < 3)
    {
        usage(argv[0]);
        return 1;
    }

    std::string input_file_name = "";
    std::string tree_name = "tree";
    std::string output_file_name = "default";
    int opt = 0;
    while ((opt = getopt(argc, argv, "i:o:t:")) != -1)
    {
        switch (opt)
        {
        case 'i':
            input_file_name = optarg;
            break;
        case 'o':
            output_file_name = optarg;
            break;
        case 't':
            tree_name = optarg;
            break;
        default:
            usage(argv[0]);
            return 1;
            break;
        }
    }
    // The default output file name will be [input_file_name -.root].parquet
    if (output_file_name == "default")
    {
        output_file_name = input_file_name.substr(0, input_file_name.length() - 4) + "parquet";
        std::cout << "output_file_name = " << output_file_name << std::endl;
    }

    int runnumber = std::atoi(input_file_name.substr(input_file_name.length() - 9, input_file_name.length() - 5).c_str());

    // Map of Apache Arrow Builders, and corresponding fill functions
    auto pool = arrow::default_memory_pool();
    arrow::FieldVector fieldVector;
    arrow::ArrayVector arrayVector;
    auto aoqBuilder = std::make_shared<arrow::DoubleBuilder>(pool);
    auto zetBuilder = std::make_shared<arrow::DoubleBuilder>(pool);
    auto tsBuilder = std::make_shared<arrow::UInt64Builder>(pool);
    auto runnumberBuilder = std::make_shared<arrow::Int32Builder>(pool);

    ROOT::RDataFrame d(tree_name, input_file_name);

    d.Foreach([&](const TreeData &input)
              {
            PARQUET_THROW_NOT_OK(aoqBuilder->Append(input.aoq));
            PARQUET_THROW_NOT_OK(zetBuilder->Append(input.zet));
            PARQUET_THROW_NOT_OK(tsBuilder->Append(input.ts));
            PARQUET_THROW_NOT_OK(runnumberBuilder->Append(runnumber)); }, {"bigrips"});

    // Finalize arrays
    {
        std::shared_ptr<arrow::Array> array;
        PARQUET_THROW_NOT_OK(aoqBuilder->Finish(&array));
        arrayVector.emplace_back(array);
        fieldVector.emplace_back(arrow::field("aoq", arrow::float64()));
    }
    {
        std::shared_ptr<arrow::Array> array;
        PARQUET_THROW_NOT_OK(zetBuilder->Finish(&array));
        arrayVector.emplace_back(array);
        fieldVector.emplace_back(arrow::field("zet", arrow::float64()));
    }
    {
        std::shared_ptr<arrow::Array> array;
        PARQUET_THROW_NOT_OK(tsBuilder->Finish(&array));
        arrayVector.emplace_back(array);
        fieldVector.emplace_back(arrow::field("ts", arrow::uint64()));
    }
    {
        std::shared_ptr<arrow::Array> array;
        PARQUET_THROW_NOT_OK(runnumberBuilder->Finish(&array));
        arrayVector.emplace_back(array);
        fieldVector.emplace_back(arrow::field("run", arrow::int32()));
    }

    // Generate schema from fields
    auto schema = arrow::schema(fieldVector);
    // Create arrow::Table from finalized arrays
    auto table = arrow::Table::Make(schema, arrayVector);

    // Open output parquet file
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(output_file_name));

    // Write table to the file
    // auto writer_properties = parquet::WriterProperties::Builder()
    //                             .compression(parquet::Compression::ZSTD)
    //                             ->build();
    // PARQUET_THROW_NOT_OK(
    //    parquet::arrow::WriteTable(*table, pool, outfile, 1048576L, writer_properties));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile));

    return 0;
}