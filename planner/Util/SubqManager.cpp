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
#include "SubqManager.h"
using namespace std;

pair<shared_ptr<QueryHandler>,RenameCorr> SubqManager::GetSubqHandler(const QueryInfo& key)
{
    assert(key.CheckValid());

    QueryInfoComparedByIso qkey_iso{key};

    if(!subq_map.count(qkey_iso))
    {
        subq_map.insert({qkey_iso,make_shared<QueryHandler>(key)});

    }

    auto corr = GetCorr(subq_map.at(qkey_iso)->qkey,key);

    return {subq_map[qkey_iso], corr};
}

void SubqManager::GenPlanSpace4All()
{
    for(auto& [qkey, qptr]: subq_map)
    {
        if(qptr->tested_subq && qptr->have_valid_subq && !qptr->tested_plan)
        {
            qptr->TestPlan();
        }
    }
}

void SubqManager::TestSubqSame() const
{
    for(auto& [qk, qptr]: subq_map)
    {
        assert(qptr!=nullptr);
        if(qptr->query_ptr == nullptr) continue;
        qptr->query_ptr->TestSubqSame();
    }
}

vector<shared_ptr<Query>> SubqManager::GetAllSubqs() const
{
    vector<shared_ptr<Query>> ans;
    cout<<"subq_map.size(): "<<subq_map.size()<<endl;
    for(auto& [k,v]: subq_map)
    {
        if(v->query_ptr != nullptr)
        {
            ans.push_back(v->query_ptr);
        }
    }
    return ans;

}
int SubqManager::Size() const { return subq_map.size(); }
size_t QueryInfoIsoHash::operator()(const QueryInfoComparedByIso& key) const {
    return key.ComputeHash();
}
