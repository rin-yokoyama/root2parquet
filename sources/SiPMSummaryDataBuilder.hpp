#ifndef __SIPMSUMMARY_DATA_BUILDER_HPP__
#define __SIPMSUMMARY_DATA_BUILDER_HPP__

#include <arrow/api.h>
#include "PaassRootStruct.hpp"

class SiPMSummaryDataBuilder
{
public:
    SiPMSummaryDataBuilder()
    {
        posXlow = std::make_shared<arrow::DoubleBuilder>();
        posYlow = std::make_shared<arrow::DoubleBuilder>();
        posXlowQDC = std::make_shared<arrow::DoubleBuilder>();
        posYlowQDC = std::make_shared<arrow::DoubleBuilder>();
        dynEnergylow = std::make_shared<arrow::DoubleBuilder>();
        dynQdclow = std::make_shared<arrow::DoubleBuilder>();
        dynTmaxlow = std::make_shared<arrow::DoubleBuilder>();
        ansumQdclow = std::make_shared<arrow::DoubleBuilder>();
        ansumEnergylow = std::make_shared<arrow::DoubleBuilder>();
        timelow = std::make_shared<arrow::DoubleBuilder>();
        validPoslow = std::make_shared<arrow::BooleanBuilder>();

        posXhigh = std::make_shared<arrow::DoubleBuilder>();
        posYhigh = std::make_shared<arrow::DoubleBuilder>();
        posXhighQDC = std::make_shared<arrow::DoubleBuilder>();
        posYhighQDC = std::make_shared<arrow::DoubleBuilder>();
        dynEnergyhigh = std::make_shared<arrow::DoubleBuilder>();
        dynQdchigh = std::make_shared<arrow::DoubleBuilder>();
        dynTmaxhigh = std::make_shared<arrow::DoubleBuilder>();
        ansumQdchigh = std::make_shared<arrow::DoubleBuilder>();
        ansumEnergyhigh = std::make_shared<arrow::DoubleBuilder>();
        timehigh = std::make_shared<arrow::DoubleBuilder>();
        validPoshigh = std::make_shared<arrow::BooleanBuilder>();

        fitEnergy = std::make_shared<arrow::DoubleBuilder>();
        fitTime = std::make_shared<arrow::DoubleBuilder>();
        ritEnergy = std::make_shared<arrow::DoubleBuilder>();
        ritTime = std::make_shared<arrow::DoubleBuilder>();
    }

    virtual ~SiPMSummaryDataBuilder() {}

    int FillArrow(const std::vector<processor_struct::SIPMSUMMARY> &input)
    {
        if (!input.empty())
        {
            PARQUET_THROW_NOT_OK(posXlow->Append(input.at(0).posXlow));
            PARQUET_THROW_NOT_OK(posYlow->Append(input.at(0).posYlow));
            PARQUET_THROW_NOT_OK(posXlowQDC->Append(input.at(0).posXlowQDC));
            PARQUET_THROW_NOT_OK(posYlowQDC->Append(input.at(0).posYlowQDC));
            PARQUET_THROW_NOT_OK(dynEnergylow->Append(input.at(0).dynEnergylow));
            PARQUET_THROW_NOT_OK(dynQdclow->Append(input.at(0).dynQdclow));
            PARQUET_THROW_NOT_OK(dynTmaxlow->Append(input.at(0).dynTmaxlow));
            PARQUET_THROW_NOT_OK(ansumQdclow->Append(input.at(0).ansumQdclow));
            PARQUET_THROW_NOT_OK(ansumEnergylow->Append(input.at(0).ansumEnergylow));
            PARQUET_THROW_NOT_OK(timelow->Append(input.at(0).timelow));
            PARQUET_THROW_NOT_OK(validPoslow->Append(input.at(0).validPoslow));

            PARQUET_THROW_NOT_OK(posXhigh->Append(input.at(0).posXhigh));
            PARQUET_THROW_NOT_OK(posYhigh->Append(input.at(0).posYhigh));
            PARQUET_THROW_NOT_OK(posXhighQDC->Append(input.at(0).posXhighQDC));
            PARQUET_THROW_NOT_OK(posYhighQDC->Append(input.at(0).posYhighQDC));
            PARQUET_THROW_NOT_OK(dynEnergyhigh->Append(input.at(0).dynEnergyhigh));
            PARQUET_THROW_NOT_OK(dynQdchigh->Append(input.at(0).dynQdchigh));
            PARQUET_THROW_NOT_OK(dynTmaxhigh->Append(input.at(0).dynTmaxhigh));
            PARQUET_THROW_NOT_OK(ansumQdchigh->Append(input.at(0).ansumQdchigh));
            PARQUET_THROW_NOT_OK(ansumEnergyhigh->Append(input.at(0).ansumEnergyhigh));
            PARQUET_THROW_NOT_OK(timehigh->Append(input.at(0).timehigh));
            PARQUET_THROW_NOT_OK(validPoshigh->Append(input.at(0).validPoshigh));

            PARQUET_THROW_NOT_OK(fitEnergy->Append(input.at(0).fitEnergy));
            PARQUET_THROW_NOT_OK(fitTime->Append(input.at(0).fitEnergy));
            PARQUET_THROW_NOT_OK(ritEnergy->Append(input.at(0).ritEnergy));
            PARQUET_THROW_NOT_OK(ritTime->Append(input.at(0).ritTime));
            return 1;
        }
        return 0;
    }

    void Finalize(arrow::FieldVector &fvec, arrow::ArrayVector &avec)
    {
        std::shared_ptr<arrow::Array> posXlowArray;
        std::shared_ptr<arrow::Array> posYlowArray;
        std::shared_ptr<arrow::Array> posXlowQDCArray;
        std::shared_ptr<arrow::Array> posYlowQDCArray;
        std::shared_ptr<arrow::Array> dynEnergylowArray;
        std::shared_ptr<arrow::Array> dynQdclowArray;
        std::shared_ptr<arrow::Array> dynTmaxlowArray;
        std::shared_ptr<arrow::Array> ansumQdclowArray;
        std::shared_ptr<arrow::Array> ansumEnergylowArray;
        std::shared_ptr<arrow::Array> timelowArray;
        std::shared_ptr<arrow::Array> validPoslowArray;

        std::shared_ptr<arrow::Array> posXhighArray;
        std::shared_ptr<arrow::Array> posYhighArray;
        std::shared_ptr<arrow::Array> posXhighQDCArray;
        std::shared_ptr<arrow::Array> posYhighQDCArray;
        std::shared_ptr<arrow::Array> dynEnergyhighArray;
        std::shared_ptr<arrow::Array> dynQdchighArray;
        std::shared_ptr<arrow::Array> dynTmaxhighArray;
        std::shared_ptr<arrow::Array> ansumQdchighArray;
        std::shared_ptr<arrow::Array> ansumEnergyhighArray;
        std::shared_ptr<arrow::Array> timehighArray;
        std::shared_ptr<arrow::Array> validPoshighArray;

        std::shared_ptr<arrow::Array> fitEnergyArray;
        std::shared_ptr<arrow::Array> fitTimeArray;
        std::shared_ptr<arrow::Array> ritEnergyArray;
        std::shared_ptr<arrow::Array> ritTimeArray;

        PARQUET_THROW_NOT_OK(posXlow->Finish(&posXlowArray));
        PARQUET_THROW_NOT_OK(posYlow->Finish(&posYlowArray));
        PARQUET_THROW_NOT_OK(posXlowQDC->Finish(&posXlowQDCArray));
        PARQUET_THROW_NOT_OK(posYlowQDC->Finish(&posYlowQDCArray));
        PARQUET_THROW_NOT_OK(dynEnergylow->Finish(&dynEnergylowArray));
        PARQUET_THROW_NOT_OK(dynQdclow->Finish(&dynQdclowArray));
        PARQUET_THROW_NOT_OK(dynTmaxlow->Finish(&dynTmaxlowArray));
        PARQUET_THROW_NOT_OK(ansumQdclow->Finish(&ansumQdclowArray));
        PARQUET_THROW_NOT_OK(ansumEnergylow->Finish(&ansumEnergylowArray));
        PARQUET_THROW_NOT_OK(timelow->Finish(&timelowArray));
        PARQUET_THROW_NOT_OK(validPoslow->Finish(&validPoslowArray));

        PARQUET_THROW_NOT_OK(posXhigh->Finish(&posXhighArray));
        PARQUET_THROW_NOT_OK(posYhigh->Finish(&posYhighArray));
        PARQUET_THROW_NOT_OK(posXhighQDC->Finish(&posXhighQDCArray));
        PARQUET_THROW_NOT_OK(posYhighQDC->Finish(&posYhighQDCArray));
        PARQUET_THROW_NOT_OK(dynEnergyhigh->Finish(&dynEnergyhighArray));
        PARQUET_THROW_NOT_OK(dynQdchigh->Finish(&dynQdchighArray));
        PARQUET_THROW_NOT_OK(dynTmaxhigh->Finish(&dynTmaxhighArray));
        PARQUET_THROW_NOT_OK(ansumQdchigh->Finish(&ansumQdchighArray));
        PARQUET_THROW_NOT_OK(ansumEnergyhigh->Finish(&ansumEnergyhighArray));
        PARQUET_THROW_NOT_OK(timehigh->Finish(&timehighArray));
        PARQUET_THROW_NOT_OK(validPoshigh->Finish(&validPoshighArray));

        PARQUET_THROW_NOT_OK(fitEnergy->Finish(&fitEnergyArray));
        PARQUET_THROW_NOT_OK(fitTime->Finish(&fitTimeArray));
        PARQUET_THROW_NOT_OK(ritEnergy->Finish(&ritEnergyArray));
        PARQUET_THROW_NOT_OK(ritTime->Finish(&ritTimeArray));

        avec.emplace_back(posXlowArray);
        avec.emplace_back(posYlowArray);
        avec.emplace_back(posXlowQDCArray);
        avec.emplace_back(posYlowQDCArray);
        avec.emplace_back(dynEnergylowArray);
        avec.emplace_back(dynQdclowArray);
        avec.emplace_back(dynTmaxlowArray);
        avec.emplace_back(ansumQdclowArray);
        avec.emplace_back(ansumEnergylowArray);
        avec.emplace_back(timelowArray);
        avec.emplace_back(validPoslowArray);

        avec.emplace_back(posXhighArray);
        avec.emplace_back(posYhighArray);
        avec.emplace_back(posXhighQDCArray);
        avec.emplace_back(posYhighQDCArray);
        avec.emplace_back(dynEnergyhighArray);
        avec.emplace_back(dynQdchighArray);
        avec.emplace_back(dynTmaxhighArray);
        avec.emplace_back(ansumQdchighArray);
        avec.emplace_back(ansumEnergyhighArray);
        avec.emplace_back(timehighArray);
        avec.emplace_back(validPoshighArray);

        avec.emplace_back(fitEnergyArray);
        avec.emplace_back(fitTimeArray);
        avec.emplace_back(ritEnergyArray);
        avec.emplace_back(ritTimeArray);

        fvec.emplace_back(arrow::field("sipmsum_posXlow", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_posYlow", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_posXlowQDC", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_posYlowQDC", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_dynEnergylow", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_dynQdclow", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_dynTmaxlow", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_ansumQdclow", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_ansumEnergylow", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_timelow", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_validPoslow", arrow::boolean()));

        fvec.emplace_back(arrow::field("sipmsum_posXhigh", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_posYhigh", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_posXhighQDC", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_posYhighQDC", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_dynEnergyhigh", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_dynQdchigh", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_dynTmaxhigh", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_ansumQdchigh", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_ansumEnergyhigh", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_timehigh", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_validPoshigh", arrow::boolean()));

        fvec.emplace_back(arrow::field("sipmsum_fitEnergy", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_fitTime", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_ritEnergy", arrow::float64()));
        fvec.emplace_back(arrow::field("sipmsum_ritTime", arrow::float64()));
    }

protected:
    std::shared_ptr<arrow::DoubleBuilder> posXlow;
    std::shared_ptr<arrow::DoubleBuilder> posYlow;
    std::shared_ptr<arrow::DoubleBuilder> posXlowQDC;
    std::shared_ptr<arrow::DoubleBuilder> posYlowQDC;
    std::shared_ptr<arrow::DoubleBuilder> dynEnergylow;
    std::shared_ptr<arrow::DoubleBuilder> dynQdclow;
    std::shared_ptr<arrow::DoubleBuilder> dynTmaxlow;
    std::shared_ptr<arrow::DoubleBuilder> ansumQdclow;
    std::shared_ptr<arrow::DoubleBuilder> ansumEnergylow;
    std::shared_ptr<arrow::DoubleBuilder> timelow;
    std::shared_ptr<arrow::BooleanBuilder> validPoslow;

    std::shared_ptr<arrow::DoubleBuilder> posXhigh;
    std::shared_ptr<arrow::DoubleBuilder> posYhigh;
    std::shared_ptr<arrow::DoubleBuilder> posXhighQDC;
    std::shared_ptr<arrow::DoubleBuilder> posYhighQDC;
    std::shared_ptr<arrow::DoubleBuilder> dynEnergyhigh;
    std::shared_ptr<arrow::DoubleBuilder> dynQdchigh;
    std::shared_ptr<arrow::DoubleBuilder> dynTmaxhigh;
    std::shared_ptr<arrow::DoubleBuilder> ansumQdchigh;
    std::shared_ptr<arrow::DoubleBuilder> ansumEnergyhigh;
    std::shared_ptr<arrow::DoubleBuilder> timehigh;
    std::shared_ptr<arrow::BooleanBuilder> validPoshigh;

    std::shared_ptr<arrow::DoubleBuilder> fitEnergy;
    std::shared_ptr<arrow::DoubleBuilder> fitTime;
    std::shared_ptr<arrow::DoubleBuilder> ritEnergy;
    std::shared_ptr<arrow::DoubleBuilder> ritTime;
};
#endif // __SIPMSUMMARY_DATA_BUILDER_HPP__
