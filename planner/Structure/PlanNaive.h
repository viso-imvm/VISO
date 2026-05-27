#pragma once
#include "Query.h"
#include "QueryInfo.h"
class PlanNaive
{
    PlanRoot plan_root;
    vector<shared_ptr<PlanNaive>> children;
public:
    PlanNaive(const PlanRoot& pr):plan_root(pr) {};
    void AddChild(shared_ptr<PlanNaive> child)
    {
        children.push_back(child);
    }
    int GetChildNum() const
    {
        return children.size();
    }

};