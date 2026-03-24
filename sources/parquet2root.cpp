#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <TFile.h>
#include <TTree.h>
#include <TROOT.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <sstream>

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
#include <stdexcept>
#include <cstdint>
#include <unistd.h>

// Thread Pool for managing worker threads
class ThreadPool
{
private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop{false};
    std::atomic<size_t> active_tasks{0};

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
                    active_tasks++;
                }
            }

            if (task)
            {
                task();
                active_tasks--;

                bool has_more_work = false;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    if (!tasks.empty())
                    {
                        has_more_work = true;
                    }
                    else if (active_tasks == 0)
                    {
                        condition.notify_all();
                    }
                }

                if (has_more_work)
                    continue;
            }
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
        {
            workers.emplace_back([this]()
                                 { worker_thread(); });
        }
    }

    ~ThreadPool()
    {
        stop = true;
        condition.notify_all();
        for (auto &worker : workers)
        {
            if (worker.joinable())
                worker.join();
        }
    }

    void enqueue(std::function<void()> task)
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.push(std::move(task));
        }
        condition.notify_all();
    }

    void wait_for_completion()
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        condition.wait(lock, [this]
                       { return tasks.empty() && active_tasks == 0; });
    }
};

// Helper struct for Arrow table data and metadata
struct ParquetData
{
    std::shared_ptr<arrow::Table> table;
    std::map<int, arrow::Type::type> column_types;
};

ParquetData ReadParquetFile(const std::string &parquet_filename)
{
    ParquetData result;

    try
    {
        arrow::MemoryPool *pool = arrow::default_memory_pool();

        auto status_input = arrow::io::ReadableFile::Open(parquet_filename);
        if (!status_input.ok())
        {
            throw std::runtime_error("Failed to open parquet file: " + status_input.status().message());
        }
        std::shared_ptr<arrow::io::RandomAccessFile> input = status_input.ValueOrDie();

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
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader = std::move(status_build).ValueOrDie();

        auto status_table = arrow_reader->ReadTable(&result.table);
        if (!status_table.ok())
        {
            throw std::runtime_error("Failed to read table: " + status_table.message());
        }

        for (int i = 0; i < result.table->num_columns(); ++i)
        {
            result.column_types[i] = result.table->column(i)->type()->id();
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error reading parquet file " << parquet_filename << ": " << e.what() << std::endl;
        result.table = nullptr;
    }

    return result;
}

// NOTE: global ROOT mutex removed for testing
void WriteRootFile(const std::string &root_filename, const ParquetData &parquet_data)
{
    if (!parquet_data.table)
    {
        std::cerr << "Error: Null table provided to WriteRootFile" << std::endl;
        return;
    }

    try
    {
        TFile root_file(root_filename.c_str(), "RECREATE");
        if (root_file.IsZombie())
        {
            throw std::runtime_error("Failed to create ROOT file");
        }

        TTree tree("tree", "Converted Parquet Data");

        auto &table = parquet_data.table;
        auto &column_types = parquet_data.column_types;

        std::vector<float> float_columns(table->num_columns());
        std::vector<double> double_columns(table->num_columns());
        std::vector<int> int_columns(table->num_columns());
        std::vector<int16_t> short_columns(table->num_columns());
        std::vector<uint64_t> ull_columns(table->num_columns());
        std::vector<int64_t> ll_columns(table->num_columns());
        std::vector<uint32_t> uint_columns(table->num_columns());
        std::vector<uint16_t> ushort_columns(table->num_columns());
        std::vector<char> bool_columns(table->num_columns());
        std::vector<std::string> string_columns(table->num_columns());

        struct ArrayColumn
        {
            arrow::Type::type element_type;
            std::vector<float> float_array;
            std::vector<double> double_array;
            std::vector<int> int_array;
            std::vector<int16_t> short_array;
            std::vector<uint64_t> ull_array;
            std::vector<int64_t> ll_array;
            std::vector<uint32_t> uint_array;
            std::vector<uint16_t> ushort_array;
            std::vector<char> bool_array;
            std::vector<std::string> string_array;
            // for decimal arrays such as decimal(21,10), stored in ROOT as doubles
            int32_t decimal_scale = 0;
            int32_t decimal_precision = 0;
        };

        std::map<int, ArrayColumn> array_columns;

        for (int col = 0; col < table->num_columns(); ++col)
        {
            std::string col_name = table->schema()->field(col)->name();
            auto col_type = column_types.at(col);

            if (col_type == arrow::Type::LIST)
            {
                auto list_type = std::static_pointer_cast<arrow::ListType>(table->column(col)->type());
                auto value_type = list_type->value_type();
                auto element_type = value_type->id();

                ArrayColumn arr_col;
                arr_col.element_type = element_type;

                switch (element_type)
                {
                case arrow::Type::FLOAT:
                    tree.Branch(col_name.c_str(), &arr_col.float_array);
                    break;
                case arrow::Type::DOUBLE:
                    tree.Branch(col_name.c_str(), &arr_col.double_array);
                    break;
                case arrow::Type::INT32:
                    tree.Branch(col_name.c_str(), &arr_col.int_array);
                    break;
                case arrow::Type::INT16:
                    tree.Branch(col_name.c_str(), &arr_col.short_array);
                    break;
                case arrow::Type::UINT64:
                    tree.Branch(col_name.c_str(), &arr_col.ull_array);
                    break;
                case arrow::Type::INT64:
                    tree.Branch(col_name.c_str(), &arr_col.ll_array);
                    break;
                case arrow::Type::UINT32:
                    tree.Branch(col_name.c_str(), &arr_col.uint_array);
                    break;
                case arrow::Type::UINT16:
                    tree.Branch(col_name.c_str(), &arr_col.ushort_array);
                    break;
                case arrow::Type::BOOL:
                    tree.Branch(col_name.c_str(), &arr_col.bool_array);
                    break;
                case arrow::Type::STRING:
                    tree.Branch(col_name.c_str(), &arr_col.string_array);
                    break;
                case arrow::Type::DECIMAL128:
                {
                    auto dec_type = std::static_pointer_cast<arrow::Decimal128Type>(value_type);
                    arr_col.decimal_scale = dec_type->scale();
                    arr_col.decimal_precision = dec_type->precision();

                    // store decimal array in ROOT as vector<double>
                    tree.Branch(col_name.c_str(), &arr_col.double_array);
                    break;
                }
                default:
                    std::cerr << "Unsupported list element type for column "
                              << col_name << " : " << value_type->ToString() << std::endl;
                    break;
                }

                array_columns[col] = std::move(arr_col);
            }
            else
            {
                switch (col_type)
                {
                case arrow::Type::FLOAT:
                    tree.Branch(col_name.c_str(), &float_columns[col]);
                    break;
                case arrow::Type::DOUBLE:
                    tree.Branch(col_name.c_str(), &double_columns[col]);
                    break;
                case arrow::Type::INT32:
                    tree.Branch(col_name.c_str(), &int_columns[col]);
                    break;
                case arrow::Type::INT16:
                    tree.Branch(col_name.c_str(), &short_columns[col]);
                    break;
                case arrow::Type::UINT64:
                    tree.Branch(col_name.c_str(), &ull_columns[col]);
                    break;
                case arrow::Type::INT64:
                    tree.Branch(col_name.c_str(), &ll_columns[col]);
                    break;
                case arrow::Type::UINT32:
                    tree.Branch(col_name.c_str(), &uint_columns[col]);
                    break;
                case arrow::Type::UINT16:
                    tree.Branch(col_name.c_str(), &ushort_columns[col]);
                    break;
                case arrow::Type::BOOL:
                    tree.Branch(col_name.c_str(), &bool_columns[col]);
                    break;
                case arrow::Type::STRING:
                    tree.Branch(col_name.c_str(), &string_columns[col]);
                    break;
                default:
                    std::cerr << "Unsupported scalar type for column "
                              << col_name << " : "
                              << table->column(col)->type()->ToString() << std::endl;
                    break;
                }
            }
        }

        for (int64_t row = 0; row < table->num_rows(); ++row)
        {
            for (int col = 0; col < table->num_columns(); ++col)
            {
                auto col_type = column_types.at(col);

                if (col_type == arrow::Type::LIST)
                {
                    auto list_array = std::static_pointer_cast<arrow::ListArray>(table->column(col)->chunk(0));
                    auto &arr_col = array_columns[col];

                    arr_col.float_array.clear();
                    arr_col.double_array.clear();
                    arr_col.int_array.clear();
                    arr_col.short_array.clear();
                    arr_col.ull_array.clear();
                    arr_col.ll_array.clear();
                    arr_col.uint_array.clear();
                    arr_col.ushort_array.clear();
                    arr_col.bool_array.clear();
                    arr_col.string_array.clear();

                    if (list_array->IsNull(row))
                        continue;

                    auto values = list_array->values();
                    int64_t start = list_array->value_offset(row);
                    int64_t end = list_array->value_offset(row + 1);

                    switch (arr_col.element_type)
                    {
                    case arrow::Type::FLOAT:
                    {
                        auto arr = std::static_pointer_cast<arrow::FloatArray>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.float_array.push_back(arr->Value(i));
                        break;
                    }
                    case arrow::Type::DOUBLE:
                    {
                        auto arr = std::static_pointer_cast<arrow::DoubleArray>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.double_array.push_back(arr->Value(i));
                        break;
                    }
                    case arrow::Type::INT32:
                    {
                        auto arr = std::static_pointer_cast<arrow::Int32Array>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.int_array.push_back(arr->Value(i));
                        break;
                    }
                    case arrow::Type::INT16:
                    {
                        auto arr = std::static_pointer_cast<arrow::Int16Array>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.short_array.push_back(arr->Value(i));
                        break;
                    }
                    case arrow::Type::UINT64:
                    {
                        auto arr = std::static_pointer_cast<arrow::UInt64Array>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.ull_array.push_back(arr->Value(i));
                        break;
                    }
                    case arrow::Type::INT64:
                    {
                        auto arr = std::static_pointer_cast<arrow::Int64Array>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.ll_array.push_back(arr->Value(i));
                        break;
                    }
                    case arrow::Type::UINT32:
                    {
                        auto arr = std::static_pointer_cast<arrow::UInt32Array>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.uint_array.push_back(arr->Value(i));
                        break;
                    }
                    case arrow::Type::UINT16:
                    {
                        auto arr = std::static_pointer_cast<arrow::UInt16Array>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.ushort_array.push_back(arr->Value(i));
                        break;
                    }
                    case arrow::Type::BOOL:
                    {
                        auto arr = std::static_pointer_cast<arrow::BooleanArray>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.bool_array.push_back(arr->Value(i) ? 1 : 0);
                        break;
                    }
                    case arrow::Type::STRING:
                    {
                        auto arr = std::static_pointer_cast<arrow::StringArray>(values);
                        for (int64_t i = start; i < end; ++i)
                            arr_col.string_array.push_back(arr->IsNull(i) ? std::string() : arr->GetString(i));
                        break;
                    }
                    case arrow::Type::DECIMAL128:
                    {
                        auto arr = std::static_pointer_cast<arrow::Decimal128Array>(values);

                        for (int64_t i = start; i < end; ++i)
                        {
                            if (arr->IsNull(i))
                            {
                                arr_col.double_array.push_back(0.0);
                            }
                            else
                            {
                                std::string s = arr->FormatValue(i);
                                arr_col.double_array.push_back(std::stod(s));
                            }
                        }
                        break;
                    }
                    default:
                        break;
                    }
                }
                else
                {
                    switch (col_type)
                    {
                    case arrow::Type::FLOAT:
                    {
                        auto arr = std::static_pointer_cast<arrow::FloatArray>(table->column(col)->chunk(0));
                        float_columns[col] = arr->Value(row);
                        break;
                    }
                    case arrow::Type::DOUBLE:
                    {
                        auto arr = std::static_pointer_cast<arrow::DoubleArray>(table->column(col)->chunk(0));
                        double_columns[col] = arr->Value(row);
                        break;
                    }
                    case arrow::Type::INT32:
                    {
                        auto arr = std::static_pointer_cast<arrow::Int32Array>(table->column(col)->chunk(0));
                        int_columns[col] = arr->Value(row);
                        break;
                    }
                    case arrow::Type::INT16:
                    {
                        auto arr = std::static_pointer_cast<arrow::Int16Array>(table->column(col)->chunk(0));
                        short_columns[col] = arr->Value(row);
                        break;
                    }
                    case arrow::Type::UINT64:
                    {
                        auto arr = std::static_pointer_cast<arrow::UInt64Array>(table->column(col)->chunk(0));
                        ull_columns[col] = arr->Value(row);
                        break;
                    }
                    case arrow::Type::INT64:
                    {
                        auto arr = std::static_pointer_cast<arrow::Int64Array>(table->column(col)->chunk(0));
                        ll_columns[col] = arr->Value(row);
                        break;
                    }
                    case arrow::Type::UINT32:
                    {
                        auto arr = std::static_pointer_cast<arrow::UInt32Array>(table->column(col)->chunk(0));
                        uint_columns[col] = arr->Value(row);
                        break;
                    }
                    case arrow::Type::UINT16:
                    {
                        auto arr = std::static_pointer_cast<arrow::UInt16Array>(table->column(col)->chunk(0));
                        ushort_columns[col] = arr->Value(row);
                        break;
                    }
                    case arrow::Type::BOOL:
                    {
                        auto arr = std::static_pointer_cast<arrow::BooleanArray>(table->column(col)->chunk(0));
                        bool_columns[col] = arr->Value(row) ? 1 : 0;
                        break;
                    }
                    case arrow::Type::STRING:
                    {
                        auto arr = std::static_pointer_cast<arrow::StringArray>(table->column(col)->chunk(0));
                        string_columns[col] = arr->IsNull(row) ? std::string() : arr->GetString(row);
                        break;
                    }
                    case arrow::Type::DECIMAL128:
                    {
                        auto arr = std::static_pointer_cast<arrow::Decimal128Array>(table->column(col)->chunk(0));
                        double_columns[col] = arr->IsNull(row) ? 0.0 : std::stod(arr->FormatValue(row));
                        break;
                    }
                    default:
                        break;
                    }
                }
            }

            tree.Fill();
        }

        tree.Write();
        root_file.Close();

        std::cout << "  Conversion complete: " << root_filename << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error writing ROOT file " << root_filename << ": " << e.what() << std::endl;
    }
}

void ConvertSingleParquetToRoot(const std::string &parquet_filename, const std::string &root_filename)
{
    try
    {
        std::cout << "Reading: " << parquet_filename << std::endl;

        ParquetData parquet_data = ReadParquetFile(parquet_filename);

        if (!parquet_data.table)
        {
            std::cerr << "Skipping file due to read failure: " << parquet_filename << std::endl;
            return;
        }

        std::cout << "  Read " << parquet_data.table->num_rows()
                  << " rows, " << parquet_data.table->num_columns()
                  << " columns" << std::endl;

        WriteRootFile(root_filename, parquet_data);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error processing " << parquet_filename << ": " << e.what() << std::endl;
    }
}

void usage(char *argv0)
{
    std::cout << "[parquet2root]: Usage:\n"
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

    if (!std::filesystem::exists(output_dir))
    {
        std::filesystem::create_directories(output_dir);
        std::cout << "Created output directory: " << output_dir << std::endl;
    }

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

    ROOT::EnableThreadSafety();

    ThreadPool pool(num_threads);

    for (const auto &parquet_file : parquet_files)
    {
        std::string filename = std::filesystem::path(parquet_file).stem().string();
        std::string root_file = std::filesystem::path(output_dir) / (filename + ".root");

        pool.enqueue([parquet_file, root_file]()
                     { ConvertSingleParquetToRoot(parquet_file, root_file); });
    }

    std::cout << "Processing " << parquet_files.size() << " files." << std::endl;
    pool.wait_for_completion();

    std::cout << "All conversions completed. Output files are in: " << output_dir << std::endl;
    return 0;
}
