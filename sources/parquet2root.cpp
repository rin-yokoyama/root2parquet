#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <TFile.h>
#include <TTree.h>
#include <iostream>

// Function to read the Parquet file and convert it to ROOT TTree
void ConvertParquetToRoot(const std::string &parquet_filename, const std::string &root_filename)
{
    // Open ROOT file and create TTree
    TFile root_file(root_filename.c_str(), "RECREATE");
    TTree tree("tree", "Converted Parquet Data");

    arrow::MemoryPool *pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(parquet_filename));

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

    // Loop over columns in the Arrow Table and prepare branches in TTree
    std::vector<float> float_columns(table->num_columns());
    for (int i = 0; i < table->num_columns(); ++i)
    {
        auto column = table->column(i);
        auto column_name = table->schema()->field(i)->name(); // Get column name from schema
        if (column->type()->id() == arrow::Type::FLOAT)
        {
            tree.Branch(column_name.c_str(), &float_columns[i]);
        }
    }

    // Fill the TTree with data
    for (int row = 0; row < table->num_rows(); ++row)
    {
        for (int col = 0; col < table->num_columns(); ++col)
        {
            if (table->column(col)->type()->id() == arrow::Type::FLOAT)
            {
                // Retrieve data from the chunked array
                auto float_array = std::static_pointer_cast<arrow::FloatArray>(table->column(col)->chunk(0));
                float_columns[col] = float_array->Value(row);
            }
        }
        tree.Fill();
    }

    // Write and close ROOT file
    root_file.Write();
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
    if (argc != 3)
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
        std::cout << "output_file_name = " << output_file_name << std::endl;
    }
    ConvertParquetToRoot(input_file_name, output_file_name);
    return 0;
}
