#pragma once

#include <vector>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <iostream>
#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <algorithm>
#include "utils.h"
#include <unordered_map>
#include "QueryInfo.h"
#include "SubqManager.h"

using namespace std;

struct RenameCorr;
struct QueryInfo;

class IsoChecker {
private:
    const QueryInfo& q1_;
    const QueryInfo& q2_;

    std::vector<int> rel_mapping_;
    std::unordered_map<int,int> attr_mapping_;

    std::vector<bool> q2_rel_used_;
    std::unordered_map<int,bool> q2_attr_used_;

    std::vector<std::vector<int>> q1_rel_neighbors_;
    std::vector<std::vector<int>> q2_rel_neighbors_;

    int q1_attr_num = -1;
    int q2_attr_num = -1;

public:
    IsoChecker(const QueryInfo& q1, const QueryInfo& q2) ;

    bool areIsomorphic() ;

    RenameCorr getRenameCorr() const ;
private:
    bool checkBasicSizes() const ;

    void buildNeighborhoodInfo() ;

    bool matchRelations(int q1_rel_index) ;

    std::vector<int> findRelationCandidates(int q1_rel_index) const ;

    bool checkPartialAttributeMapping(const std::vector<int>& q1_attrs,
                                     const std::vector<int>& q2_attrs) const ;

    bool satisfiesLookAheadConstraints(int q1_rel, int q2_rel) const ;

    bool tryRelationMapping(int q1_rel, int q2_rel) ;

    void undoRelationMapping(int q1_rel, int q2_rel) ;

    bool areAttributesCompatible(int q1_attr, int q2_attr) const ;

    bool checkRemainingConstraints() ;

    bool checkRelCopySet() const ;

    bool checkOutputAttributes() const ;
    bool checkRoot() const;

};

bool CheckIsomorphic(const QueryInfo& q1, const QueryInfo& q2);

RenameCorr GetCorr(const QueryInfo &q1, const QueryInfo &q2);
