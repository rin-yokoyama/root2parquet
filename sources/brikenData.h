#ifndef __BRIKENTreeData__
#define __BRIKENTreeData__
#include <string>
#include <vector>

#ifndef kMaxBeamInfo
#define kMaxBeamInfo 6
#endif

typedef u_int64_t TSTime_t;

class BrikenTreeData
{
public:
    BrikenTreeData() {};
    ~BrikenTreeData() {};
    //    std::vector<double> CorrE;
    //    std::vector<double> CorrT;
    //    std::vector<uint64_t> CorrTS;
    //    std::vector<uint16_t> CorrId;
    //    std::vector<uint16_t> CorrType;
    double E;
    TSTime_t T;
    u_int16_t Id;
    u_int16_t type;
    u_int16_t Index1;
    u_int16_t Index2;
    u_int16_t InfoFlag;
    u_int64_t EventId;
    u_int64_t RunId;
    std::string Name;
    std::vector<u_int16_t> Samples;

    void clear()
    {
        Name.clear();
        E = 0;
        T = 0;
        type = 0;
        Id = 0;
        Index1 = 0;
        Index2 = 0;
        InfoFlag = 0;
        EventId = 0;
        RunId = 0;
        Samples.clear();
    }
};

#endif