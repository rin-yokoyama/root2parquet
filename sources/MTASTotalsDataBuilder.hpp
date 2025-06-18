#ifndef __MTASTOTALS_DATA_BUILDER_HPP__
#define __MTASTOTALS_DATA_BUILDER_HPP__

#include <arrow/api.h>
#include "PaassRootStruct.hpp"

class MTASTotalsDataBuilder
{
public:
    MTASTotalsDataBuilder()
    {
        Total = std::make_shared<arrow::DoubleBuilder>();
        Center = std::make_shared<arrow::DoubleBuilder>();
        Middle = std::make_shared<arrow::DoubleBuilder>();
        Outer = std::make_shared<arrow::DoubleBuilder>();
    }

    virtual ~MTASTotalsDataBuilder() {}

    int FillArrow(const std::vector<processor_struct::MTASTOTALS> &input)
    {
        if (!input.empty())
        {
            PARQUET_THROW_NOT_OK(Total->Append(input.at(0).Total));
            PARQUET_THROW_NOT_OK(Center->Append(input.at(0).Center));
            PARQUET_THROW_NOT_OK(Middle->Append(input.at(0).Middle));
            PARQUET_THROW_NOT_OK(Outer->Append(input.at(0).Outer));
            return 1;
        }
        return 0;
    }

    void Finalize(arrow::FieldVector &fvec, arrow::ArrayVector &avec)
    {
        std::shared_ptr<arrow::Array> TotalArray;
        std::shared_ptr<arrow::Array> CenterArray;
        std::shared_ptr<arrow::Array> MiddleArray;
        std::shared_ptr<arrow::Array> OuterArray;
        PARQUET_THROW_NOT_OK(Total->Finish(&TotalArray));
        PARQUET_THROW_NOT_OK(Center->Finish(&CenterArray));
        PARQUET_THROW_NOT_OK(Middle->Finish(&MiddleArray));
        PARQUET_THROW_NOT_OK(Outer->Finish(&OuterArray));
        avec.emplace_back(TotalArray);
        avec.emplace_back(CenterArray);
        avec.emplace_back(MiddleArray);
        avec.emplace_back(OuterArray);
        fvec.emplace_back(arrow::field("mtastotals_Total", arrow::float64()));
        fvec.emplace_back(arrow::field("mtastotals_Center", arrow::float64()));
        fvec.emplace_back(arrow::field("mtastotals_Middle", arrow::float64()));
        fvec.emplace_back(arrow::field("mtastotals_Outer", arrow::float64()));
    }

protected:
    std::shared_ptr<arrow::DoubleBuilder> Total;
    std::shared_ptr<arrow::DoubleBuilder> Center;
    std::shared_ptr<arrow::DoubleBuilder> Middle;
    std::shared_ptr<arrow::DoubleBuilder> Outer;
};
#endif // __MTASTOTALS_DATA_BUILDER_HPP__
