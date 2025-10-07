#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <TFile.h>
#include <TTree.h>
#include <iostream>
#include <filesystem>
#include <vector>
#include <map>
#include <string>
#include <memory>

// Function to read the Parquet file and convert it to ROOT TTree
void ConvertParquetToRoot(const std::string &parquet_filename, const std::string &root_filename)
{
    // Open ROOT file and create TTree
    TFile root_file(root_filename.c_str(), "RECREATE");
    TTree tree("tree", "Converted Parquet Data");

    // Data containers for scalar columns
    std::vector<float> float_columns;
    std::vector<double> double_columns;
    std::vector<int> int_columns;
    std::vector<int16_t> short_columns;
    std::vector<uint64_t> ull_columns;
    std::vector<int64_t> ll_columns;
    std::vector<uint32_t> uint_columns;
    std::vector<uint16_t> ushort_columns;
    std::vector<char> bool_columns;

    // Data containers for array columns
    struct ArrayColumn
    {
        arrow::Type::type element_type;
        std::vector<float> float_array;
        std::vector<double> double_array;
        std::vector<int> int_array;
        std::vector<int16_t> short_array;
        std::vector<int64_t> long_array;
        std::vector<uint32_t> uint_array;
        std::vector<uint16_t> ushort_array;
        std::vector<uint64_t> ulong_array;
        std::vector<char> bool_array;
        int array_size;
        std::string branch_spec;
    };

    std::map<int, ArrayColumn> array_columns;      // column_index -> ArrayColumn
    std::map<int, arrow::Type::type> column_types; // column_index -> type

    bool branches_created = false;

    for (const auto &entry : std::filesystem::directory_iterator(parquet_filename))
    {
        std::cout << "Scanning file: " << entry.path() << std::endl;
        if (entry.path().extension() == ".parquet")
        {
            std::string filename = entry.path().string();

            arrow::MemoryPool *pool = arrow::default_memory_pool();
            std::shared_ptr<arrow::io::RandomAccessFile> input;
            ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(filename));

            // Open Parquet file reader
            std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
            ARROW_ASSIGN_OR_RAISE(arrow_reader, parquet::arrow::OpenFile(input, pool));

            // Read entire file as a single Arrow table
            std::shared_ptr<arrow::Table> table;
            ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

            if (!branches_created)
            {
                int ncol = table->num_columns();
                std::cout << "Num columns: " << ncol << std::endl;

                // Resize scalar containers
                float_columns.resize(ncol);
                double_columns.resize(ncol);
                int_columns.resize(ncol);
                short_columns.resize(ncol);
                ull_columns.resize(ncol);
                ll_columns.resize(ncol);
                uint_columns.resize(ncol);
                ushort_columns.resize(ncol);
                bool_columns.resize(ncol);

                // Analyze columns and create branches
                for (int i = 0; i < table->num_columns(); ++i)
                {
                    auto column = table->column(i);
                    auto column_name = table->schema()->field(i)->name();
                    auto column_type = column->type();

                    std::cout << "Column '" << column_name << "' Type Id = " << column_type->id() << std::endl;
                    column_types[i] = column_type->id();

                    if (column_type->id() == arrow::Type::LIST)
                    {
                        // Handle list/array columns
                        auto list_type = std::static_pointer_cast<arrow::ListType>(column_type);
                        auto element_type = list_type->value_type();

                        std::cout << "Array column '" << column_name << "' with element type: " << element_type->id() << std::endl;

                        ArrayColumn array_col;
                        array_col.element_type = element_type->id();

                        // Determine max array size by scanning the data
                        int max_size = 0;
                        for (int chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx)
                        {
                            auto list_array = std::static_pointer_cast<arrow::ListArray>(column->chunk(chunk_idx));
                            for (int64_t row = 0; row < list_array->length(); ++row)
                            {
                                if (!list_array->IsNull(row))
                                {
                                    int size = list_array->value_length(row);
                                    max_size = std::max(max_size, size);
                                }
                            }
                        }

                        array_col.array_size = std::max(max_size, 1); // At least size 1 to avoid issues
                        std::cout << "Max array size for '" << column_name << "': " << array_col.array_size << std::endl;

                        // Create appropriate branch based on element type
                        switch (element_type->id())
                        {
                        case arrow::Type::FLOAT:
                            array_col.float_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/F";
                            tree.Branch(column_name.c_str(), array_col.float_array.data(), array_col.branch_spec.c_str());
                            break;
                        case arrow::Type::DOUBLE:
                            array_col.double_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/D";
                            tree.Branch(column_name.c_str(), array_col.double_array.data(), array_col.branch_spec.c_str());
                            break;
                        case arrow::Type::INT32:
                            array_col.int_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/I";
                            tree.Branch(column_name.c_str(), array_col.int_array.data(), array_col.branch_spec.c_str());
                            break;
                        case arrow::Type::INT16:
                            array_col.short_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/S";
                            tree.Branch(column_name.c_str(), array_col.short_array.data(), array_col.branch_spec.c_str());
                            break;
                        case arrow::Type::INT64:
                            array_col.long_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/L";
                            tree.Branch(column_name.c_str(), array_col.long_array.data(), array_col.branch_spec.c_str());
                            break;
                        case arrow::Type::UINT32:
                            array_col.uint_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/i";
                            tree.Branch(column_name.c_str(), array_col.uint_array.data(), array_col.branch_spec.c_str());
                            break;
                        case arrow::Type::UINT16:
                            array_col.ushort_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/s";
                            tree.Branch(column_name.c_str(), array_col.ushort_array.data(), array_col.branch_spec.c_str());
                            break;
                        case arrow::Type::UINT64:
                            array_col.ulong_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/l";
                            tree.Branch(column_name.c_str(), array_col.ulong_array.data(), array_col.branch_spec.c_str());
                            break;
                        case arrow::Type::BOOL:
                            array_col.bool_array.resize(array_col.array_size);
                            array_col.branch_spec = column_name + "[" + std::to_string(array_col.array_size) + "]/B";
                            tree.Branch(column_name.c_str(), array_col.bool_array.data(), array_col.branch_spec.c_str());
                            break;
                        default:
                            std::cout << "Unsupported array element type: " << element_type->id() << std::endl;
                            continue;
                        }

                        array_columns[i] = array_col;
                        std::cout << "Added array branch '" << column_name << "' with spec: " << array_col.branch_spec << std::endl;
                    }
                    else
                    {
                        // Handle scalar columns
                        switch (column_type->id())
                        {
                        case arrow::Type::FLOAT:
                            std::cout << "Adding scalar branch " << column_name << " (float)" << std::endl;
                            tree.Branch(column_name.c_str(), &float_columns[i]);
                            break;
                        case arrow::Type::DOUBLE:
                            std::cout << "Adding scalar branch " << column_name << " (double)" << std::endl;
                            tree.Branch(column_name.c_str(), &double_columns[i]);
                            break;
                        case arrow::Type::INT32:
                            std::cout << "Adding scalar branch " << column_name << " (int32)" << std::endl;
                            tree.Branch(column_name.c_str(), &int_columns[i]);
                            break;
                        case arrow::Type::INT16:
                            std::cout << "Adding scalar branch " << column_name << " (int16)" << std::endl;
                            tree.Branch(column_name.c_str(), &short_columns[i]);
                            break;
                        case arrow::Type::INT64:
                            std::cout << "Adding scalar branch " << column_name << " (int64)" << std::endl;
                            tree.Branch(column_name.c_str(), &ll_columns[i]);
                            break;
                        case arrow::Type::UINT32:
                            std::cout << "Adding scalar branch " << column_name << " (uint32)" << std::endl;
                            tree.Branch(column_name.c_str(), &uint_columns[i]);
                            break;
                        case arrow::Type::UINT16:
                            std::cout << "Adding scalar branch " << column_name << " (uint16)" << std::endl;
                            tree.Branch(column_name.c_str(), &ushort_columns[i]);
                            break;
                        case arrow::Type::UINT64:
                            std::cout << "Adding scalar branch " << column_name << " (uint64)" << std::endl;
                            tree.Branch(column_name.c_str(), &ull_columns[i]);
                            break;
                        case arrow::Type::BOOL:
                            std::cout << "Adding scalar branch " << column_name << " (bool)" << std::endl;
                            tree.Branch(column_name.c_str(), &bool_columns[i], (column_name + "/B").c_str());
                            break;
                        default:
                            std::cout << "Unsupported scalar type: " << column_type->id() << std::endl;
                            break;
                        }
                    }
                }
                branches_created = true;
            }
            // Fill the TTree with data
            for (int row = 0; row < table->num_rows(); ++row)
            {
                for (int col = 0; col < table->num_columns(); ++col)
                {
                    auto column_type = column_types[col];

                    if (column_type == arrow::Type::LIST)
                    {
                        // Handle array columns
                        if (array_columns.find(col) != array_columns.end())
                        {
                            auto &array_col = array_columns[col];
                            auto list_array = std::static_pointer_cast<arrow::ListArray>(table->column(col)->chunk(0));

                            // Clear previous values (set to 0/false)
                            switch (array_col.element_type)
                            {
                            case arrow::Type::FLOAT:
                                std::fill(array_col.float_array.begin(), array_col.float_array.end(), 0.0f);
                                break;
                            case arrow::Type::DOUBLE:
                                std::fill(array_col.double_array.begin(), array_col.double_array.end(), 0.0);
                                break;
                            case arrow::Type::INT32:
                                std::fill(array_col.int_array.begin(), array_col.int_array.end(), 0);
                                break;
                            case arrow::Type::INT16:
                                std::fill(array_col.short_array.begin(), array_col.short_array.end(), 0);
                                break;
                            case arrow::Type::INT64:
                                std::fill(array_col.long_array.begin(), array_col.long_array.end(), 0);
                                break;
                            case arrow::Type::UINT32:
                                std::fill(array_col.uint_array.begin(), array_col.uint_array.end(), 0);
                                break;
                            case arrow::Type::UINT16:
                                std::fill(array_col.ushort_array.begin(), array_col.ushort_array.end(), 0);
                                break;
                            case arrow::Type::UINT64:
                                std::fill(array_col.ulong_array.begin(), array_col.ulong_array.end(), 0);
                                break;
                            case arrow::Type::BOOL:
                                std::fill(array_col.bool_array.begin(), array_col.bool_array.end(), 0);
                                break;
                            }

                            // Fill with actual values if not null
                            if (!list_array->IsNull(row))
                            {
                                int list_length = list_array->value_length(row);
                                int start_offset = list_array->value_offset(row);
                                auto values_array = list_array->values();

                                int elements_to_copy = std::min(list_length, array_col.array_size);

                                switch (array_col.element_type)
                                {
                                case arrow::Type::FLOAT:
                                {
                                    auto float_values = std::static_pointer_cast<arrow::FloatArray>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.float_array[i] = float_values->Value(start_offset + i);
                                    }
                                    break;
                                }
                                case arrow::Type::DOUBLE:
                                {
                                    auto double_values = std::static_pointer_cast<arrow::DoubleArray>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.double_array[i] = double_values->Value(start_offset + i);
                                    }
                                    break;
                                }
                                case arrow::Type::INT32:
                                {
                                    auto int_values = std::static_pointer_cast<arrow::Int32Array>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.int_array[i] = int_values->Value(start_offset + i);
                                    }
                                    break;
                                }
                                case arrow::Type::INT16:
                                {
                                    auto short_values = std::static_pointer_cast<arrow::Int16Array>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.short_array[i] = short_values->Value(start_offset + i);
                                    }
                                    break;
                                }
                                case arrow::Type::INT64:
                                {
                                    auto long_values = std::static_pointer_cast<arrow::Int64Array>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.long_array[i] = long_values->Value(start_offset + i);
                                    }
                                    break;
                                }
                                case arrow::Type::UINT32:
                                {
                                    auto uint_values = std::static_pointer_cast<arrow::UInt32Array>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.uint_array[i] = uint_values->Value(start_offset + i);
                                    }
                                    break;
                                }
                                case arrow::Type::UINT16:
                                {
                                    auto ushort_values = std::static_pointer_cast<arrow::UInt16Array>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.ushort_array[i] = ushort_values->Value(start_offset + i);
                                    }
                                    break;
                                }
                                case arrow::Type::UINT64:
                                {
                                    auto ulong_values = std::static_pointer_cast<arrow::UInt64Array>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.ulong_array[i] = ulong_values->Value(start_offset + i);
                                    }
                                    break;
                                }
                                case arrow::Type::BOOL:
                                {
                                    auto bool_values = std::static_pointer_cast<arrow::BooleanArray>(values_array);
                                    for (int i = 0; i < elements_to_copy; ++i)
                                    {
                                        array_col.bool_array[i] = bool_values->Value(start_offset + i) ? 1 : 0;
                                    }
                                    break;
                                }
                                }
                            }
                        }
                    }
                    else
                    {
                        // Handle scalar columns
                        switch (column_type)
                        {
                        case arrow::Type::FLOAT:
                        {
                            auto float_array = std::static_pointer_cast<arrow::FloatArray>(table->column(col)->chunk(0));
                            float_columns[col] = float_array->Value(row);
                            break;
                        }
                        case arrow::Type::DOUBLE:
                        {
                            auto double_array = std::static_pointer_cast<arrow::DoubleArray>(table->column(col)->chunk(0));
                            double_columns[col] = double_array->Value(row);
                            break;
                        }
                        case arrow::Type::INT32:
                        {
                            auto int_array = std::static_pointer_cast<arrow::Int32Array>(table->column(col)->chunk(0));
                            int_columns[col] = int_array->Value(row);
                            break;
                        }
                        case arrow::Type::INT16:
                        {
                            auto short_array = std::static_pointer_cast<arrow::Int16Array>(table->column(col)->chunk(0));
                            short_columns[col] = short_array->Value(row);
                            break;
                        }
                        case arrow::Type::INT64:
                        {
                            auto long_array = std::static_pointer_cast<arrow::Int64Array>(table->column(col)->chunk(0));
                            ll_columns[col] = long_array->Value(row);
                            break;
                        }
                        case arrow::Type::UINT32:
                        {
                            auto uint_array = std::static_pointer_cast<arrow::UInt32Array>(table->column(col)->chunk(0));
                            uint_columns[col] = uint_array->Value(row);
                            break;
                        }
                        case arrow::Type::UINT16:
                        {
                            auto ushort_array = std::static_pointer_cast<arrow::UInt16Array>(table->column(col)->chunk(0));
                            ushort_columns[col] = ushort_array->Value(row);
                            break;
                        }
                        case arrow::Type::UINT64:
                        {
                            auto ull_array = std::static_pointer_cast<arrow::UInt64Array>(table->column(col)->chunk(0));
                            ull_columns[col] = ull_array->Value(row);
                            break;
                        }
                        case arrow::Type::BOOL:
                        {
                            auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(table->column(col)->chunk(0));
                            bool_columns[col] = bool_array->Value(row) ? 1 : 0;
                            break;
                        }
                        }
                    }
                }
                tree.Fill();
            }
        }
    }
    // Write and close ROOT file
    tree.Write();
    root_file.Close();
    std::cout << "Conversion complete: " << root_filename << " created." << std::endl;
}

/** prints usage **/
void usage(char *argv0)
{
    std::cout << "[parquet2root]: Usage: \n"
              << argv0 << " -i [input_root_file_name]\n"
              << "-o [output_file_name] (default: [input_file_name].root)"
              << std::endl;
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        usage(argv[0]);
        return 1;
    }

    std::string input_file_name = "";
    std::string output_file_name = "default";
    int opt = 0;
    while ((opt = getopt(argc, argv, "i:o:")) != -1)
    {
        switch (opt)
        {
        case 'i':
            input_file_name = optarg;
            break;
        case 'o':
            output_file_name = optarg;
            break;
        default:
            usage(argv[0]);
            return 1;
            break;
        }
    }
    // The default output file name will be [input_file_name -.parquet].root
    if (output_file_name == "default")
    {
        output_file_name = input_file_name.substr(0, input_file_name.length() - 7) + "root";
    }
    std::cout << "output_file_name = " << output_file_name << std::endl;
    ConvertParquetToRoot(input_file_name, output_file_name);
    return 0;
}
