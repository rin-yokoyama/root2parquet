# root2parquet
Converts a simple ROOT TTree to a Apache parquet file.

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

## Usage
```
root2parquet -i [input_root_file_name]
-t [input_tree_name] (default: tree) -o [output_file_name] (default: [input_ridf_file_name].parquet)
```


## Supported Data Types
- `Double_t`
- `Float_t`
- `Int_t`
- `Long64_t`
- `ULong64_t`
- `ROOT::VecOps::RVec<int>`, `vector<int>`
- `ROOT::VecOps::RVec<float>`, `vector<float>`
- `ROOT::VecOps::RVec<double>`, `vector<double>`

Add else if statements to the root2parquet.cpp to support more data types
```
builders[lName] = std::make_shared<arrow::ArrayBuilder>(pool); : Create ArrayBuilder for the specific type
fields.emlace_back(arrow::field(column_name, type)); : Append arrow::field object to the FieldVector for generating schema
valueReaders[lName] = new TTreeReaderValue<type>(reader, lName.c_str()); : Create TTreeReaderValue for the branch
function[lName] =[](const std::string &name) {}; : Define a function to fill a branch entry to the arrow builder
```
