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
#include "MTASTotalsDataBuilder.hpp"
#include "SiPMSummaryDataBuilder.hpp"
#include "PIDDataBuilder.hpp"

/** prints usage **/
void usage(char *argv0)
{
    std::cout << "[vandle2parquet]: Usage: \n"
              << argv0 << " -i [input_root_file_name]\n"
              << "-t [input_tree_name] (default: tree)"
              << "-b [input_branch_name] (default: Gamma)"
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
    std::string tree_name = "PixTree";
    std::string branch_name = "mtastotals_vec_";
    std::string output_file_name = "default";
    int opt = 0;
    while ((opt = getopt(argc, argv, "i:o:t:b:")) != -1)
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
        case 'b':
            branch_name = optarg;
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

    int runnumber = std::atoi(input_file_name.substr(input_file_name.length() - 8, 3).c_str());
    if (!runnumber)
        runnumber = std::atoi(input_file_name.substr(input_file_name.length() - 7, 3).c_str());
    if (!runnumber)
        runnumber = std::atoi(input_file_name.substr(input_file_name.length() - 6, 3).c_str());
    if (!runnumber)
        runnumber = std::atoi(input_file_name.substr(input_file_name.length() - 5, 3).c_str());
    if (!runnumber)
        runnumber = std::atoi(input_file_name.substr(input_file_name.length() - 4, 3).c_str());

    // Map of Apache Arrow Builders, and corresponding fill functions
    auto pool = arrow::default_memory_pool();
    arrow::FieldVector fieldVector;
    arrow::ArrayVector arrayVector;
    auto eventIdBuilder = std::make_shared<arrow::UInt64Builder>(pool);

    ROOT::RDataFrame d(tree_name, input_file_name);

    auto sipmSumBuilder = std::make_unique<SiPMSummaryDataBuilder>();
    auto mtasTotalsBuilder = std::make_unique<MTASTotalsDataBuilder>();
    auto pidBuilder = std::make_unique<PIDDataBuilder>();

    // Finalize arrays
    auto FinalizeEventId = [&]()
    {
        std::shared_ptr<arrow::Array> array;
        PARQUET_THROW_NOT_OK(eventIdBuilder->Finish(&array));
        arrayVector.emplace_back(array);
        fieldVector.emplace_back(arrow::field("event_id", arrow::uint64()));
    };

    if (branch_name == "sipmsum_vec_")
    {
        auto FillSiPMSummary = [&](const std::vector<processor_struct::SIPMSUMMARY> &input, const double &eventNum)
        {
            auto entry = sipmSumBuilder->FillArrow(input);
            if (entry)
                PARQUET_THROW_NOT_OK(eventIdBuilder->Append(eventNum));
        };
        d.Foreach(FillSiPMSummary, {branch_name, "eventNum"});
        FinalizeEventId();
        sipmSumBuilder->Finalize(fieldVector, arrayVector);
    }
    else if (branch_name == "mtastotals_vec_")
    {
        auto FillMTASTotals = [&](const std::vector<processor_struct::MTASTOTALS> &input, const double &eventNum)
        {
            auto entry = mtasTotalsBuilder->FillArrow(input);
            if (entry)
                PARQUET_THROW_NOT_OK(eventIdBuilder->Append(eventNum));
        };
        d.Foreach(FillMTASTotals, {branch_name, "eventNum"});
        FinalizeEventId();
        mtasTotalsBuilder->Finalize(fieldVector, arrayVector);
    }
    else if (branch_name == "pid_vec_")
    {
        auto FillPID = [&](const std::vector<processor_struct::PID> &input, const double &eventNum)
        {
            auto entry = pidBuilder->FillArrow(input);
            if (entry)
                PARQUET_THROW_NOT_OK(eventIdBuilder->Append(eventNum));
        };
        d.Foreach(FillPID, {branch_name, "eventNum"});
        FinalizeEventId();
        pidBuilder->Finalize(fieldVector, arrayVector);
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

    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile));

    return 0;
}