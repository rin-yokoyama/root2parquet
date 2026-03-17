#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <TFile.h>
#include <TTree.h>
#include <TROOT.h>
#include <iostream>
#include <filesystem>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>

// Global mutex for protecting ROOT operations (not thread-safe by default)
std::mutex root_mutex;

// Thread Pool for managing worker threads
class ThreadPool
{
private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop{false};

    void worker_thread()
    {
        while (true)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                condition.wait(lock, [this]
                               { return !tasks.empty() || stop; });

                if (stop && tasks.empty())
                    return;

                if (!tasks.empty())
                {
                    task = std::move(tasks.front());
                    tasks.pop();
                }
            }

            if (task)
                task();
        }
    }

public:
    ThreadPool(size_t num_threads = 0)
    {
        if (num_threads == 0)
        {
            num_threads = std::thread::hardware_concurrency();
            if (num_threads == 0)
                num_threads = 4;
        }

        std::cout << "Creating thread pool with " << num_threads << " threads" << std::endl;

        for (size_t i = 0; i < num_threads; ++i)
            workers.emplace_back([this]
                                 { worker_thread(); });
    }

    ~ThreadPool()
    {
        stop = true;
        condition.notify_all();
        for (auto &worker : workers)
            if (worker.joinable())
                worker.join();
    }

    void enqueue(std::function<void()> task)
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.push(task);
        }
        condition.notify_one();
    }

    void wait_for_completion()
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        condition.wait(lock, [this]
                       { return tasks.empty(); });
    }
};

// Helper struct for Arrow table data and metadata (for thread-safe passing)
struct ParquetData
{
    std::shared_ptr<arrow::Table> table;
    std::map<int, arrow::Type::type> column_types;
};

// Function to safely read Arrow Parquet data (no ROOT operations, fully parallelizable)
ParquetData ReadParquetFile(const std::string &parquet_filename)
{
    ParquetData result;

    try
    {
        arrow::MemoryPool *pool = arrow::default_memory_pool();
        std::shared_ptr<arrow::io::RandomAccessFile> input;
        auto status_input = arrow::io::ReadableFile::Open(parquet_filename);
        if (!status_input.ok())
        {
            throw std::runtime_error("Failed to open parquet file: " + status_input.status().message());
        }
        input = std::move(status_input.ValueOrDie());

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        parquet::arrow::FileReaderBuilder reader_builder;
        reader_builder.memory_pool(pool);
        auto status_open = reader_builder.Open(input);
        if (!status_open.ok())
        {
            throw std::runtime_error("Failed to open parquet reader: " + status_open.message());
        }

        auto status_build = reader_builder.Build();
        if (!status_build.ok())
        {
            throw std::runtime_error("Failed to build parquet reader: " + status_build.status().message());
        }
        arrow_reader = std::move(status_build.ValueOrDie());

        auto status_table = arrow_reader->ReadTable(&result.table);
        if (!status_table.ok())
        {
            throw std::runtime_error("Failed to read table: " + status_table.message());
        }

        // Store column types
        for (int i = 0; i < result.table->num_columns(); ++i)
        {
            result.column_types[i] = result.table->column(i)->type()->id();
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error reading parquet file: " << e.what() << std::endl;
        result.table = nullptr;
    }

    return result;
}

// Function to convert Arrow table to ROOT file (needs ROOT mutex)
void WriteRootFile(const std::string &root_filename, const ParquetData &parquet_data)
{
    if (!parquet_data.table)
    {
        std::cerr << "Error: Null table provided to WriteRootFile" << std::endl;
        return;
    }

    std::unique_lock<std::mutex> root_lock(root_mutex);

    try
    {
        TFile root_file(root_filename.c_str(), "RECREATE");
        TTree tree("tree", "Converted Parquet Data");

        auto &table = parquet_data.table;
        auto &column_types = parquet_data.column_types;

        // Data containers for scalar columns
        std::vector<float> float_columns(table->num_columns());
        std::vector<double> double_columns(table->num_columns());
        std::vector<int> int_columns(table->num_columns());
        std::vector<int16_t> short_columns(table->num_columns());
        std::vector<uint64_t> ull_columns(table->num_columns());
        std::vector<int64_t> ll_columns(table->num_columns());
        std::vector<uint32_t> uint_columns(table->num_columns());
        std::vector<uint16_t> ushort_columns(table->num_columns());
        std::vector<char> bool_columns(table->num_columns());

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

        std::map<int, ArrayColumn> array_columns;

        // Create branches based on schema
        for (int i = 0; i < table->num_columns(); ++i)
        {
            auto column = table->column(i);
            auto column_name = table->schema()->field(i)->name();
            auto column_type = column->type();

            if (column_type->id() == arrow::Type::LIST)
            {
                auto list_type = std::static_pointer_cast<arrow::ListType>(column_type);
                auto element_type = list_type->value_type();

                ArrayColumn array_col;
                array_col.element_type = element_type->id();

                // Determine max array size
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

                array_col.array_size = std::max(max_size, 1);

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
                    continue;
                }

                array_columns[i] = array_col;
            }
            else
            {
                // Handle scalar columns
                switch (column_type->id())
                {
                case arrow::Type::FLOAT:
                    tree.Branch(column_name.c_str(), &float_columns[i]);
                    break;
                case arrow::Type::DOUBLE:
                    tree.Branch(column_name.c_str(), &double_columns[i]);
                    break;
                case arrow::Type::INT32:
                    tree.Branch(column_name.c_str(), &int_columns[i]);
                    break;
                case arrow::Type::INT16:
                    tree.Branch(column_name.c_str(), &short_columns[i]);
                    break;
                case arrow::Type::INT64:
                    tree.Branch(column_name.c_str(), &ll_columns[i]);
                    break;
                case arrow::Type::UINT32:
                    tree.Branch(column_name.c_str(), &uint_columns[i]);
                    break;
                case arrow::Type::UINT16:
                    tree.Branch(column_name.c_str(), &ushort_columns[i]);
                    break;
                case arrow::Type::UINT64:
                    tree.Branch(column_name.c_str(), &ull_columns[i]);
                    break;
                case arrow::Type::BOOL:
                    tree.Branch(column_name.c_str(), &bool_columns[i], (column_name + "/B").c_str());
                    break;
                default:
                    break;
                }
            }
        }

        // Fill the TTree with data
        for (int row = 0; row < table->num_rows(); ++row)
        {
            for (int col = 0; col < table->num_columns(); ++col)
            {
                auto column_type = column_types.at(col);

                if (column_type == arrow::Type::LIST)
                {
                    if (array_columns.find(col) != array_columns.end())
                    {
                        auto &array_col = array_columns[col];
                        auto list_array = std::static_pointer_cast<arrow::ListArray>(table->column(col)->chunk(0));

                        // Clear previous values
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
                                    array_col.float_array[i] = float_values->Value(start_offset + i);
                                break;
                            }
                            case arrow::Type::DOUBLE:
                            {
                                auto double_values = std::static_pointer_cast<arrow::DoubleArray>(values_array);
                                for (int i = 0; i < elements_to_copy; ++i)
                                    array_col.double_array[i] = double_values->Value(start_offset + i);
                                break;
                            }
                            case arrow::Type::INT32:
                            {
                                auto int_values = std::static_pointer_cast<arrow::Int32Array>(values_array);
                                for (int i = 0; i < elements_to_copy; ++i)
                                    array_col.int_array[i] = int_values->Value(start_offset + i);
                                break;
                            }
                            case arrow::Type::INT16:
                            {
                                auto short_values = std::static_pointer_cast<arrow::Int16Array>(values_array);
                                for (int i = 0; i < elements_to_copy; ++i)
                                    array_col.short_array[i] = short_values->Value(start_offset + i);
                                break;
                            }
                            case arrow::Type::INT64:
                            {
                                auto long_values = std::static_pointer_cast<arrow::Int64Array>(values_array);
                                for (int i = 0; i < elements_to_copy; ++i)
                                    array_col.long_array[i] = long_values->Value(start_offset + i);
                                break;
                            }
                            case arrow::Type::UINT32:
                            {
                                auto uint_values = std::static_pointer_cast<arrow::UInt32Array>(values_array);
                                for (int i = 0; i < elements_to_copy; ++i)
                                    array_col.uint_array[i] = uint_values->Value(start_offset + i);
                                break;
                            }
                            case arrow::Type::UINT16:
                            {
                                auto ushort_values = std::static_pointer_cast<arrow::UInt16Array>(values_array);
                                for (int i = 0; i < elements_to_copy; ++i)
                                    array_col.ushort_array[i] = ushort_values->Value(start_offset + i);
                                break;
                            }
                            case arrow::Type::UINT64:
                            {
                                auto ulong_values = std::static_pointer_cast<arrow::UInt64Array>(values_array);
                                for (int i = 0; i < elements_to_copy; ++i)
                                    array_col.ulong_array[i] = ulong_values->Value(start_offset + i);
                                break;
                            }
                            case arrow::Type::BOOL:
                            {
                                auto bool_values = std::static_pointer_cast<arrow::BooleanArray>(values_array);
                                for (int i = 0; i < elements_to_copy; ++i)
                                    array_col.bool_array[i] = bool_values->Value(start_offset + i) ? 1 : 0;
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

        // Write and close ROOT file
        tree.Write();
        root_file.Close();
        std::cout << "  Conversion complete: " << root_filename << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error writing ROOT file " << root_filename << ": " << e.what() << std::endl;
    }
}

// Main conversion function called by thread pool
void ConvertSingleParquetToRoot(const std::string &parquet_filename, const std::string &root_filename)
{
    try
    {
        std::cout << "Reading: " << parquet_filename << std::endl;

        // Read Arrow/Parquet data (fully parallelizable, no locking needed)
        ParquetData parquet_data = ReadParquetFile(parquet_filename);

        std::cout << "  Read " << parquet_data.table->num_rows() << " rows, " 
                  << parquet_data.table->num_columns() << " columns" << std::endl;

        // Write to ROOT file (requires mutex lock)
        WriteRootFile(root_filename, parquet_data);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error processing " << parquet_filename << ": " << e.what() << std::endl;
    }
}

/** prints usage **/
void usage(char *argv0)
{
    std::cout << "[parquet2root]: Usage: \n"
              << argv0 << " -i [input_parquet_directory] -o [output_directory] [-t num_threads]\n"
              << "  -i: input directory containing parquet files (required)\n"
              << "  -o: output directory for root files (will be created if not exists)\n"
              << "  -t: number of threads (default: auto-detect CPU cores)"
              << std::endl;
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        usage(argv[0]);
        return 1;
    }

    std::string input_dir = "";
    std::string output_dir = "./output_root_files";
    size_t num_threads = 0;

    int opt = 0;
    while ((opt = getopt(argc, argv, "i:o:t:")) != -1)
    {
        switch (opt)
        {
        case 'i':
            input_dir = optarg;
            break;
        case 'o':
            output_dir = optarg;
            break;
        case 't':
            num_threads = std::stoul(optarg);
            break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    // Validate input directory
    if (input_dir.empty())
    {
        std::cerr << "Error: Input directory must be specified with -i flag" << std::endl;
        usage(argv[0]);
        return 1;
    }

    if (!std::filesystem::is_directory(input_dir))
    {
        std::cerr << "Error: Input directory does not exist: " << input_dir << std::endl;
        return 1;
    }

    // Create output directory if it doesn't exist
    if (!std::filesystem::exists(output_dir))
    {
        std::filesystem::create_directories(output_dir);
        std::cout << "Created output directory: " << output_dir << std::endl;
    }

    // Collect all parquet files
    std::vector<std::string> parquet_files;
    for (const auto &entry : std::filesystem::directory_iterator(input_dir))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".parquet")
        {
            parquet_files.push_back(entry.path().string());
        }
    }

    if (parquet_files.empty())
    {
        std::cerr << "No parquet files found in directory: " << input_dir << std::endl;
        return 1;
    }

    std::cout << "Found " << parquet_files.size() << " parquet files" << std::endl;

    // Initialize ROOT in main thread (required for thread safety)
    ROOT::EnableThreadSafety();

    // Create thread pool
    ThreadPool pool(num_threads);

    // Queue conversion tasks
    for (size_t i = 0; i < parquet_files.size(); ++i)
    {
        const auto &parquet_file = parquet_files[i];
        std::string filename = std::filesystem::path(parquet_file).stem().string();
        std::string root_file = std::filesystem::path(output_dir) / (filename + ".root");

        // Enqueue the conversion task
        pool.enqueue([parquet_file, root_file]()
                     { ConvertSingleParquetToRoot(parquet_file, root_file); });
    }

    // Wait for all conversions to complete
    std::cout << "Processing " << parquet_files.size() << " files..." << std::endl;
    pool.wait_for_completion();

    std::cout << "All conversions completed! Output files are in: " << output_dir << std::endl;
    return 0;
}
