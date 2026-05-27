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
#include "SubqManager.h"
using namespace std;

QueryHandler::QueryHandler(const QueryInfo& key): qkey(key) {}
QueryHandler::QueryHandler(const QueryInfo& key, shared_ptr<Query> ptr): qkey(key),query_ptr(ptr) {
    tested_subq = true;
    have_valid_subq = (ptr != nullptr);
}

void QueryHandler::TestSubq()
{
    assert(!tested_subq && query_ptr == nullptr);
    tested_subq = true;
    query_ptr = make_shared<Query>(qkey);
    have_valid_subq = query_ptr->GenValidSubqueries();
    if(!have_valid_subq)
        query_ptr = nullptr;
}
void QueryHandler::TestPlan()
{
    assert(tested_subq && have_valid_subq && !tested_plan && query_ptr != nullptr);
    tested_plan = true;
    if(query_ptr->info.rel_copy_set.size() == 1)
    {

        have_valid_plan = true;
        auto p = make_shared<Plan>(query_ptr->info.rel_copy_set.at(0));
        query_ptr->AddPlan(p);

        return;
    }
    query_ptr->GenValidPlan();
    have_valid_plan = !query_ptr->plans.empty();
    if(!have_valid_plan)
        query_ptr = nullptr;
}
