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
#include <string>
#include <algorithm>
#include "TROOT.h"
#include "TFile.h"
#include "TTreeReader.h"
#include "TTreeReaderValue.h"
#include "TTreeReaderArray.h"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/type.h>
#include <parquet/arrow/writer.h>

/** prints usage **/
void usage(char *argv0)
{
    std::cout << "[root2parquet]: Usage: \n"
              << argv0 << " -i [input_root_file_name]\n"
              << "-t [input_tree_name] (default: tree)"
              << "-o [output_file_name] (default: [input_ridf_file_name].parquet)"
              << std::endl;
}

/** Helper function to parse array information from leaf title */
struct ArrayInfo
{
    bool isArray = false;
    bool isFixedSize = false;
    int fixedSize = 0;
    std::string sizeBranch = "";
    std::string baseName = "";
};

ArrayInfo parseArrayInfo(const std::string &leafTitle, const std::string &leafName)
{
    ArrayInfo info;
    info.baseName = leafName;

    size_t bracketStart = leafTitle.find('[');
    if (bracketStart != std::string::npos)
    {
        info.isArray = true;
        size_t bracketEnd = leafTitle.find(']', bracketStart);
        if (bracketEnd != std::string::npos)
        {
            std::string sizeStr = leafTitle.substr(bracketStart + 1, bracketEnd - bracketStart - 1);

            // Try to parse as integer (fixed size array)
            try
            {
                info.fixedSize = std::stoi(sizeStr);
                info.isFixedSize = true;
                std::cout << "Fixed size array: " << leafName << " with size " << info.fixedSize << std::endl;
            }
            catch (const std::exception &)
            {
                // Not a number, assume it's a variable name
                info.sizeBranch = sizeStr;
                info.isFixedSize = false;
                std::cout << "Variable size array: " << leafName << " with size branch " << info.sizeBranch << std::endl;
            }
        }
    }
    return info;
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

    // Map of Apache Arrow Builders, and corresponding fill functions
    auto pool = arrow::default_memory_pool();
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> builders;
    std::map<std::string, std::shared_ptr<arrow::Field>> fields;
    std::map<std::string, std::function<void(const std::string &)>> functions;
    // Map of ROOT TTreeReaderValues and Arrays
    std::map<std::string, ROOT::Internal::TTreeReaderArrayBase *> arrayReaders;
    std::map<std::string, ROOT::Internal::TTreeReaderValueBase *> valueReaders;
    // Map to store array size information
    std::map<std::string, std::string> arraySizeBranches; // maps array name to size branch name
    std::map<std::string, int> fixedArraySizes;           // maps array name to fixed size

    // Open input ROOT file
    TFile rfile(input_file_name.c_str());
    auto tree = (TTree *)rfile.Get(tree_name.c_str());
    TTreeReader reader(tree);

    // Scans branches and leaves in the TTree
    auto branches = tree->GetListOfBranches();
    for (int i = 0; i < branches->GetEntries(); ++i)
    {
        TBranch *br = (TBranch *)branches->At(i);
        TList *lvList = (TList *)br->GetListOfLeaves();
        for (int j = 0; j < lvList->GetEntries(); ++j)
        {
            TLeaf *l = (TLeaf *)lvList->At(j);
            std::string lName = l->GetName();
            std::string lTitle = l->GetTitle();
            std::string lType = l->GetTypeName();

            ArrayInfo arrayInfo = parseArrayInfo(lTitle, lName);
            std::cout << "Branch Name: " << lTitle << ", Type: " << lType << ", isArray: " << arrayInfo.isArray;
            if (arrayInfo.isArray)
            {
                if (arrayInfo.isFixedSize)
                {
                    std::cout << " (fixed size: " << arrayInfo.fixedSize << ")";
                }
                else
                {
                    std::cout << " (variable size, controlled by: " << arrayInfo.sizeBranch << ")";
                }
            }
            std::cout << std::endl;

            // Store array information for later use
            if (arrayInfo.isArray)
            {
                if (arrayInfo.isFixedSize)
                {
                    fixedArraySizes[lName] = arrayInfo.fixedSize;
                }
                else
                {
                    arraySizeBranches[lName] = arrayInfo.sizeBranch;
                }
            }
            /**
             * Type specific procedure to fill tree entries to Apache arrow
             *
             * builders[lName] = std::make_shared<arrow::ArrayBuilder>(pool); : Create ArrayBuilder for the specific type
             * fields.emlace_back(arrow::field(column_name, type)); : Append arrow::field object to the FieldVector for generating schema
             * valueReaders[lName] = new TTreeReaderValue<type>(reader, lName.c_str()); : Create TTreeReaderValue for the branch
             * function[lName] =[](const std::string &name) {}; : Define a function to fill a branch entry to the arrow builder
             */
            if (lTitle == lName && !arrayInfo.isArray)
            {
                if (lType == "Double_t")
                {
                    builders[lName] = std::make_shared<arrow::DoubleBuilder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::float64()));
                    valueReaders[lName] = new TTreeReaderValue<Double_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder *>(builders[name].get())->Append(*((TTreeReaderValue<Double_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "Float_t")
                {
                    builders[lName] = std::make_shared<arrow::FloatBuilder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::float32()));
                    valueReaders[lName] = new TTreeReaderValue<Float_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder *>(builders[name].get())->Append(*((TTreeReaderValue<Float_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "Int_t")
                {
                    builders[lName] = std::make_shared<arrow::Int32Builder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::int32()));
                    valueReaders[lName] = new TTreeReaderValue<Int_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::Int32Builder *>(builders[name].get())->Append(*((TTreeReaderValue<Int_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "Long64_t")
                {
                    builders[lName] = std::make_shared<arrow::Int64Builder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::int64()));
                    valueReaders[lName] = new TTreeReaderValue<Long64_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::Int64Builder *>(builders[name].get())->Append(*((TTreeReaderValue<Long64_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "ULong64_t")
                {
                    builders[lName] = std::make_shared<arrow::UInt64Builder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::uint64()));
                    valueReaders[lName] = new TTreeReaderValue<ULong64_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::UInt64Builder *>(builders[name].get())->Append(*((TTreeReaderValue<ULong64_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "Short_t")
                {
                    builders[lName] = std::make_shared<arrow::Int16Builder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::int16()));
                    valueReaders[lName] = new TTreeReaderValue<Short_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::Int16Builder *>(builders[name].get())->Append(*((TTreeReaderValue<Short_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "UShort_t")
                {
                    builders[lName] = std::make_shared<arrow::UInt16Builder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::uint16()));
                    valueReaders[lName] = new TTreeReaderValue<UShort_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::UInt16Builder *>(builders[name].get())->Append(*((TTreeReaderValue<UShort_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "Bool_t")
                {
                    builders[lName] = std::make_shared<arrow::BooleanBuilder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::boolean()));
                    valueReaders[lName] = new TTreeReaderValue<Bool_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::BooleanBuilder *>(builders[name].get())->Append(*((TTreeReaderValue<Bool_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "UInt_t")
                {
                    builders[lName] = std::make_shared<arrow::UInt32Builder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::uint32()));
                    valueReaders[lName] = new TTreeReaderValue<UInt_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::UInt32Builder *>(builders[name].get())->Append(*((TTreeReaderValue<UInt_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "Char_t")
                {
                    builders[lName] = std::make_shared<arrow::Int8Builder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::int8()));
                    valueReaders[lName] = new TTreeReaderValue<Char_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::Int8Builder *>(builders[name].get())->Append(*((TTreeReaderValue<Char_t> *)valueReaders[name])->Get()));
                    };
                }
                else if (lType == "UChar_t")
                {
                    builders[lName] = std::make_shared<arrow::UInt8Builder>(pool);
                    fields[lName] = (arrow::field(lName, arrow::uint8()));
                    valueReaders[lName] = new TTreeReaderValue<UChar_t>(reader, lName.c_str());
                    functions[lName] = [&builders, &valueReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::UInt8Builder *>(builders[name].get())->Append(*((TTreeReaderValue<UChar_t> *)valueReaders[name])->Get()));
                    };
                }
            }
            // Definitions for array types (improved handling for different array formats)
            if (lType == "ROOT::VecOps::RVec<double>" || lType == "vector<double>" || (arrayInfo.isArray && lType == "Double_t"))
            {
                builders[lName] = std::make_shared<arrow::DoubleBuilder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::float64())));
                arrayReaders[lName] = new TTreeReaderArray<Double_t>(reader, lName.c_str());

                // Enhanced function to handle different array types
                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<Double_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize); // Use the smaller size for safety
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<Double_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::DoubleBuilder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
            else if (lType == "ROOT::VecOps::RVec<float>" || lType == "vector<float>" || (arrayInfo.isArray && lType == "Float_t"))
            {
                builders[lName] = std::make_shared<arrow::FloatBuilder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::float32())));
                arrayReaders[lName] = new TTreeReaderArray<Float_t>(reader, lName.c_str());

                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<Float_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize);
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<Float_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::FloatBuilder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
            else if (lType == "ROOT::VecOps::RVec<int>" || lType == "vector<int>" || (arrayInfo.isArray && lType == "Int_t"))
            {
                builders[lName] = std::make_shared<arrow::Int32Builder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::int32())));
                arrayReaders[lName] = new TTreeReaderArray<Int_t>(reader, lName.c_str());

                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<Int_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize);
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::Int32Builder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<Int_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::Int32Builder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
            else if (lType == "ROOT::VecOps::RVec<short>" || lType == "vector<short>" || (arrayInfo.isArray && lType == "Short_t"))
            {
                builders[lName] = std::make_shared<arrow::Int16Builder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::int16())));
                arrayReaders[lName] = new TTreeReaderArray<Short_t>(reader, lName.c_str());

                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<Short_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize);
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::Int16Builder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<Short_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::Int16Builder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
            else if (lType == "ROOT::VecOps::RVec<int64_t>" || lType == "vector<int64_t>" || (arrayInfo.isArray && lType == "Long64_t"))
            {
                builders[lName] = std::make_shared<arrow::Int64Builder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::int64())));
                arrayReaders[lName] = new TTreeReaderArray<Long64_t>(reader, lName.c_str());

                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<Long64_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize);
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::Int64Builder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<Long64_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::Int64Builder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
            else if (lType == "ROOT::VecOps::RVec<unsigned int>" || lType == "vector<unsigned int>" || (arrayInfo.isArray && lType == "UInt_t"))
            {
                builders[lName] = std::make_shared<arrow::UInt32Builder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::uint32())));
                arrayReaders[lName] = new TTreeReaderArray<UInt_t>(reader, lName.c_str());

                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<UInt_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize);
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::UInt32Builder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<UInt_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::UInt32Builder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
            else if (lType == "ROOT::VecOps::RVec<uint64_t>" || lType == "vector<uint64_t>" || (arrayInfo.isArray && lType == "ULong64_t"))
            {
                builders[lName] = std::make_shared<arrow::UInt64Builder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::uint64())));
                arrayReaders[lName] = new TTreeReaderArray<ULong64_t>(reader, lName.c_str());

                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<ULong64_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize);
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::UInt64Builder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<ULong64_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::UInt64Builder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
            else if (lType == "ROOT::VecOps::RVec<unsigned short>" || lType == "vector<unsigned short>" || (arrayInfo.isArray && lType == "UShort_t"))
            {
                builders[lName] = std::make_shared<arrow::UInt16Builder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::uint16())));
                arrayReaders[lName] = new TTreeReaderArray<UShort_t>(reader, lName.c_str());

                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<UShort_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize);
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::UInt16Builder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<UShort_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::UInt16Builder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
            else if (lType == "ROOT::VecOps::RVec<bool>" || lType == "vector<bool>" || (arrayInfo.isArray && lType == "Bool_t"))
            {
                builders[lName] = std::make_shared<arrow::BooleanBuilder>(pool);
                builders[lName + "L"] = std::make_shared<arrow::ListBuilder>(pool, builders[lName]);
                fields[lName + "L"] = (arrow::field(lName, arrow::list(arrow::boolean())));
                arrayReaders[lName] = new TTreeReaderArray<Bool_t>(reader, lName.c_str());

                if (arrayInfo.isFixedSize)
                {
                    functions[lName] = [&builders, &arrayReaders, &fixedArraySizes](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        auto *array = (TTreeReaderArray<Bool_t> *)arrayReaders[name];
                        int expectedSize = fixedArraySizes[name];
                        int actualSize = array->GetSize();
                        int size = std::min(expectedSize, actualSize);
                        for (int i = 0; i < size; ++i)
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::BooleanBuilder *>(builders[name].get())->Append((*array)[i]));
                        }
                    };
                }
                else
                {
                    functions[lName] = [&builders, &arrayReaders](const std::string &name)
                    {
                        PARQUET_THROW_NOT_OK(static_cast<arrow::ListBuilder *>(builders[name + "L"].get())->Append());
                        for (auto &v : *(TTreeReaderArray<Bool_t> *)arrayReaders[name])
                        {
                            PARQUET_THROW_NOT_OK(static_cast<arrow::BooleanBuilder *>(builders[name].get())->Append(v));
                        }
                    };
                }
            }
        }
    }

    // Print array size information summary
    std::cout << "\nArray size information summary:" << std::endl;
    for (const auto &pair : fixedArraySizes)
    {
        std::cout << "  " << pair.first << ": fixed size array [" << pair.second << "]" << std::endl;
    }
    for (const auto &pair : arraySizeBranches)
    {
        std::cout << "  " << pair.first << ": variable size array, controlled by branch '" << pair.second << "'" << std::endl;
        // Check if the size branch exists
        bool sizeBranchFound = false;
        for (const auto &vr : valueReaders)
        {
            if (vr.first == pair.second)
            {
                sizeBranchFound = true;
                break;
            }
        }
        if (!sizeBranchFound)
        {
            std::cout << "    WARNING: Size branch '" << pair.second << "' not found in scalar branches!" << std::endl;
        }
    }
    std::cout << std::endl;

    // Event loop
    long long eventCount = 0;
    while (reader.Next())
    {
        // Fill scalar values first (including size variables)
        for (auto &r : valueReaders)
        {
            std::string brName = r.first;
            functions[brName](brName);
        }

        // Then fill arrays (they may depend on size variables)
        for (auto &r : arrayReaders)
        {
            std::string brName = r.first;
            functions[brName](brName);
        }

        eventCount++;
        if (eventCount % 10000 == 0)
        {
            std::cout << "Processed " << eventCount << " events..." << std::endl;
        }
    }
    std::cout << "Total events processed: " << eventCount << std::endl;

    arrow::FieldVector fieldVec;
    // Finalize arrays
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto &builder : builders)
    {
        std::string brName = builder.first;
        std::shared_ptr<arrow::Array> array;
        if (builders.find(brName + "L") == builders.end())
        {
            PARQUET_THROW_NOT_OK(builder.second->Finish(&array));
            fieldVec.emplace_back(fields[brName]);
            arrays.emplace_back(array);
        }
    }
    // Generate schema from fields
    auto schema = arrow::schema(fieldVec);
    // Create arrow::Table from finalized arrays
    auto table = arrow::Table::Make(schema, arrays);

    // Open output parquet file
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(output_file_name));

    // Write table to the file
    // auto writer_properties = parquet::WriterProperties::Builder()
    //                              .compression(parquet::Compression::ZSTD)
    //                              ->build();
    // PARQUET_THROW_NOT_OK(
    //     parquet::arrow::WriteTable(*table, pool_, outfile, 1048576L, writer_properties));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, pool, outfile));

    return 0;
}
