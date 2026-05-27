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
#include <set>
#include "SubqManager.h"
#include "QueryInfo.h"
#include "FinalNode.h"
#include "PlanNaive.h"
#include <cfloat>

using namespace std;

struct Query;
struct QueryInfo;

struct Plan
{
    PlanRoot plan_root;
    vector<shared_ptr<Query>> children;
    vector<RenameCorr> child_corrs;
    vector<shared_ptr<Query>> parents;

    double min_cost = DBL_MAX;
    void Print() const;
    double CalcCost();
    Plan(int rel_id);
    Plan() = default;
    void UpdRefCnt(int change);
    vector<shared_ptr<Query>> GetParents() const;
    void UpdSharedCost();
};

struct Query:public enable_shared_from_this<Query>
{
    bool shared = false;
    set<int> possible_qids;
    int plan_num_after_prune = 0;
    QueryInfo info;

    vector<shared_ptr<Query>> subqueries;
    vector<RenameCorr> subq_corrs;
    vector<shared_ptr<Plan>> plans;
    double min_cost = DBL_MAX;
    int best_plan_idx = -1;
    int old_opt_child = -1;

    shared_ptr<FinalNode> aod = nullptr;
    int plan_num = 0;
    vector<shared_ptr<PlanNaive>> naive_plans;

    int actual_ref_count = 0;
    vector<shared_ptr<Plan>> parents;

    void AddPlan(shared_ptr<Plan> plan);
    void AddPlans(vector<shared_ptr<Plan>> ps);
    vector<shared_ptr<Plan>> GetParents() const;
    void UpdRefCnt(int change);
    double GetUpdFreq();
    double GetMinCost();
    int GetRefCnt() const;
    void SetShared();
    void GenSubqSpace();
    void GenValidPlan();
    bool GenValidSubqueries();
    Query(const QueryInfo& qk);
    QueryInfo GetQueryKeyCopy() const;

    void SelectPlan(int depth);
    double GetCost(int depth);
    void ConstructAOD(int query_id, shared_ptr<FinalNode> parent);

    void ConstructPlanNaive();

    void CalcAllPlanNum();
    void TestSubqSame() const;

    void SpreadQid(int qid);

    bool ReSelectOptChild();
};

struct QueryHandler
{
    bool tested_subq = false;
    bool have_valid_subq = false;
    bool tested_plan = false;
    bool have_valid_plan = false;

    shared_ptr<Query> query_ptr = nullptr;
    QueryInfo qkey;

    QueryHandler(const QueryInfo& key);
    QueryHandler() = delete;
    QueryHandler(const QueryInfo& key, shared_ptr<Query> ptr);

    void TestSubq();
    void TestPlan();
};

struct QueryInfoHash {
    size_t operator()(const QueryInfo& key) const ;
};
