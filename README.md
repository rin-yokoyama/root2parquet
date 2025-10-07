# root2parquet
- root2parquet: 
Converts a simple ROOT TTree to a Apache parquet file.
- parquet2root:
Converts a simple parquet to ROOT TTree

## Requirements
- Cern ROOT
- Apache Arrow, Parquet


For Ubuntu 22.04
```
sudo apt update
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev
sudo apt install -y -V libparquet-dev
```
Check [https://arrow.apache.org/install/] for details

## Installation
```
mkdir build
cd build
cmake ..
make
```
Requires the `$ROOTSYS` environment

## Usage
```
root2parquet -i [input_root_file_name]
-t [input_tree_name] (default: tree) -o [output_file_name] (default: [input_root_file_name].parquet)
parquet2root -i [input_parquet_file_name]
-o [output_file_name] (default: [input_parquet_file_name].root)
```
parquet2root assumes a directory for the input. If you have a single parquet file, put it in a directory ends with .parquet and provide it as an input.

## Supported Data Types
- `Double_t`
- `Float_t`
- `Int_t`
- `Long64_t`
- `ULong64_t`
- `Short_t`
- `UShort_t`
- `Bool_t`
- `UInt_t`
- `Char_t`
- `UChar_t`
- `ROOT::VecOps::RVec`, `std::vector`, or 1d arrays (`[]`), of types above

Add else if statements to the root2parquet.cpp to support more data types
```
builders[lName] = std::make_shared<arrow::ArrayBuilder>(pool); : Create ArrayBuilder for the specific type
fields.emlace_back(arrow::field(column_name, type)); : Append arrow::field object to the FieldVector for generating schema
valueReaders[lName] = new TTreeReaderValue<type>(reader, lName.c_str()); : Create TTreeReaderValue for the branch
function[lName] =[](const std::string &name) {}; : Define a function to fill a branch entry to the arrow builder
```
