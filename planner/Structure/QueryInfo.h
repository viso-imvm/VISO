#pragma once

#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <algorithm>
#include "utils.h"
#include <unordered_map>
#include <cassert>
#include <map>

using namespace std;

struct RenameCorr
{
    unordered_map<int,int> rel_corr;
    unordered_map<int,int> attr_corr;
};

struct RelationCopy
{
    int label_id = -1;
    vector<int> attributes;
    RelationCopy(int label, const vector<int>& attrs);
    RelationCopy() = default;
    void Print(ostream& out) const ;
};

class PlanRoot
{
    int rel_id = -1;
    vector<int> general_attrs;
public:
    PlanRoot(int r_id = -1);
    PlanRoot(const vector<int>& attrs);
    PlanRoot(const PlanRoot& other, const RenameCorr& corr);
    bool CheckIn(const vector<int>& s) const ;
    bool IsGeneralized() const;
    vector<int> GetGeneralizedAttrs() const;
    int GetRelId() const;

    double GetRootCost() const;

    string ToStringIso() const ;
    string ToString() const;
    bool operator==(const PlanRoot &other) const;
    bool CheckNotDecide() const ;
    void Print(ostream& out) const ;
    vector<int> ConvertToAttrIdxs(const vector<int> &attrs) const;
    int GetAttrIdx(int attr_id) const;

    void PrintWithLabel(ostream &out) const;

    vector<int> GetAllAttrs() const;;

};

struct QueryInfo
{
    PlanRoot plan_root;

    vector<int> rel_copy_set;
    vector<int> y;

    void Print() const;

    QueryInfo() = default;
    QueryInfo(const QueryInfo& query, const RenameCorr& corr);
    vector<int> GetAllAttr() const;
    unordered_map<int,int> GetAllAttrWithCnt() const ;
    void SetY(const vector<int>& output_attrs);
    QueryInfo(const QueryInfo& parent_query, vector<int> & rel_set, int root_rel_id = -2) ;
    vector<int> AttrIntersection(const QueryInfo& other_query) const;
    bool FreeConnexCheck() const;

    bool operator==(const QueryInfo& other) const;

    string GenIdentifierIso() const;

    size_t ComputeHash() const;

    bool CheckValid() const;

    bool IsConnected() const;
    double GetUpdFreq() const;

};
