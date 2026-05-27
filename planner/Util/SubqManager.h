#pragma once

#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <algorithm>
#include "utils.h"
#include <unordered_map>
#include "Query.h"
#include <cassert>
#include <functional>
#include "IsoCheck.h"
using namespace std;

struct QueryHandler;
struct Query;
struct RenameCorr;

struct QueryInfoComparedByIso
{
    QueryInfo info;
    bool operator==(const QueryInfoComparedByIso& other) const;
    size_t ComputeHash() const;
};

struct QueryInfoIsoHash {
    size_t operator()(const QueryInfoComparedByIso& key) const ;
};

class SubqManager
{
    unordered_map<QueryInfoComparedByIso, shared_ptr<QueryHandler>, QueryInfoIsoHash> subq_map;

public:
    pair<shared_ptr<QueryHandler>,RenameCorr> GetSubqHandler(const QueryInfo& key);

    void GenPlanSpace4All();
    int Size() const;

    void TestSubqSame() const;

    vector<shared_ptr<Query>> GetAllSubqs() const;

};
