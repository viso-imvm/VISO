#include <stdexcept>
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
#include <boost/unordered_map.hpp>
using namespace std;

int plan_naive_num = 0;
int plan_naive_edge_num = 0;

extern vector<RelationCopy> relation_copies;
extern SubqManager subq_manager;
extern vector<double> rel_label2upd_freq_est;
extern int stored_plan_num;

bool FreeConnexCheckWithGeneralizedNode(const QueryInfo& query, const vector<vector<int>>& gen_nodes)
{
    QueryInfo test_q = query;
    int ori_rel_num = relation_copies.size();
    for(auto& gnode: gen_nodes)
    {
        relation_copies.emplace_back(-1, gnode);
        test_q.rel_copy_set.push_back(relation_copies.size()-1);
    }

    bool ans = test_q.FreeConnexCheck();
    relation_copies.resize(ori_rel_num);
    return ans;

}

bool TryGenSubtree(QueryHandler& subquery_handler, vector<shared_ptr<Query>>& subqueries)
{
    if(!subquery_handler.tested_subq)
    {
        subquery_handler.TestSubq();
        if(subquery_handler.have_valid_subq)
        {
            subqueries.push_back(subquery_handler.query_ptr);
            return true;
        }
        return false;
    }
    else
    {
        if(subquery_handler.have_valid_subq)
        {
            subqueries.push_back(subquery_handler.query_ptr);
            return true;
        }
        return false;
    }
}

double Query::GetUpdFreq()
{
    return info.GetUpdFreq();

}

double Query::GetMinCost()
{
    if(min_cost!=DBL_MAX) return min_cost;

    if(plans.empty()) info.Print();
    assert(!plans.empty());

    for(int i = 0; i < plans.size(); i++)
    {
        auto& p = plans.at(i);
        double cost = p->CalcCost();
        if (cost < min_cost)
        {
            min_cost = cost;
            best_plan_idx = i;
        }
    }

    return min_cost;
}

int Query::GetRefCnt() const
{
    return actual_ref_count;
}

void Query::SetShared()
{
    shared = true;
    min_cost = 0;
}

void Query::GenSubqSpace() {
    assert(info.plan_root.CheckNotDecide());
    assert(info.FreeConnexCheck());

    for (int rel_id : info.rel_copy_set) {

        auto root_attrs = relation_copies.at(rel_id).attributes;
        if(Intersection(root_attrs, info.y).empty())
        {
            continue;
        }

        auto qkey = GetQueryKeyCopy();
        qkey.plan_root = {rel_id};
        auto [subquery_ptr,corr] = subq_manager.GetSubqHandler(qkey);

        auto success = TryGenSubtree(*subquery_ptr, subqueries);
        if(success) subq_corrs.push_back(corr);

    }
    for(int attr: info.y)
    {
        auto qkey = GetQueryKeyCopy();
        vector<int> root_attrs;
        root_attrs.emplace_back(attr);
        qkey.plan_root={root_attrs};
        auto [subquery_ptr,corr] = subq_manager.GetSubqHandler(qkey);

        auto success = TryGenSubtree(*subquery_ptr, subqueries);
        if(success) subq_corrs.push_back(corr);
    }

    if(subqueries.empty())
    {
        assert(0);
    }

}

void Query::AddPlans(vector<shared_ptr<Plan>> ps)
{
    for(auto& p: ps)
    {
        AddPlan(p);
    }
}

void Query::AddPlan(shared_ptr<Plan> plan)
{
    plans.push_back(plan);
    plan->parents.push_back(shared_from_this());
}

void Query::GenValidPlan()
{

    assert(plans.empty());

    assert(info.rel_copy_set.size() > 1);

    boost::unordered_map<pair<int,int>, vector<shared_ptr<Plan>>> memo;

    function<vector<shared_ptr<Plan>>(int, int)> dp = [&](int used_rels_mask, int start_idx) {
        if (used_rels_mask == (1 << info.rel_copy_set.size()) - 1) {
            auto plan = make_shared<Plan>();
            plan->plan_root = info.plan_root;
            return vector<shared_ptr<Plan>>{plan};
        }

        if (start_idx >= subqueries.size()) {
            return vector<shared_ptr<Plan>>();
        }

        if (memo.count({used_rels_mask, start_idx})) {
            return memo.at({used_rels_mask, start_idx});
        }

        vector<shared_ptr<Plan>> result;

        auto child_q = subqueries.at(start_idx);
        int child_mask = 0;

        QueryInfo info_renamed(child_q->info, subq_corrs.at(start_idx));
        auto child_rels = info_renamed.rel_copy_set;

        for (int rel_id : child_rels) {
            auto it = find(info.rel_copy_set.begin(), info.rel_copy_set.end(), rel_id);
            assert(it != info.rel_copy_set.end());
            int rel_idx = distance(info.rel_copy_set.begin(), it);
            child_mask |= (1 << rel_idx);
        }

        if ((used_rels_mask & child_mask) == 0) {
            int new_mask = used_rels_mask | child_mask;
            auto sub_plans = dp(new_mask, start_idx + 1);

            for (auto& sub_plan : sub_plans) {
                auto new_plan = make_shared<Plan>(*sub_plan);
                new_plan->children.push_back(child_q);
                child_q->parents.push_back(new_plan);
                new_plan->child_corrs.push_back(subq_corrs.at(start_idx));
                result.push_back(new_plan);
            }
        }

        auto skip_plans = dp(used_rels_mask, start_idx + 1);
        result.insert(result.end(), skip_plans.begin(), skip_plans.end());

        assert(!memo.count({used_rels_mask, start_idx}));
        memo.insert({{used_rels_mask, start_idx}, result});

        return result;
    };

    int initial_mask = 0;
    if(!info.plan_root.IsGeneralized())
    {
        int root_rel_idx = -1;
        for(int i = 0; i < info.rel_copy_set.size(); ++i)
        {
            if(info.rel_copy_set.at(i) == info.plan_root.GetRelId())
            {
                root_rel_idx = i;
                break;
            }
        }
        initial_mask |= 1 << root_rel_idx;
    }
    auto all_plans = dp(initial_mask,0);
    AddPlans(all_plans);

    if(plans.empty())
    {

        info.Print();
        cout<<endl;
        
        for(auto& sq: subqueries)
        {
            sq->info.Print();
            cout<<endl;
        }
        throw runtime_error("GenValidPlan: no valid plan found for query (plans.empty())");
    }

}

bool Query::GenValidSubqueries()
{

    assert(subqueries.empty());

    int n = info.rel_copy_set.size();
    if(n <= 1) return true;

    vector<QueryInfo> valid_subq_info;
    for (int i = 0; i < (1 << n); ++i) {

        if(i == 0) continue;
        if(i == (1 << n) - 1) continue;

        vector<int> s3;
        vector<int> not_s3;
        for (int j = 0; j < n; ++j) {
            if (i & (1 << j)) {
                s3.push_back(info.rel_copy_set.at(j));
            } else {
                not_s3.push_back(info.rel_copy_set.at(j));
            }
        }

        if(!info.plan_root.CheckIn(s3)) continue;

        QueryInfo s3_subq = {this->info, s3};
        QueryInfo not_s3_subq = {this->info, not_s3, -1};

        if(!s3_subq.IsConnected() || !not_s3_subq.IsConnected())
        {

            continue;
        }

        if(info.plan_root.IsGeneralized())
        {

            auto root_attrs = info.plan_root.GetGeneralizedAttrs();

            vector<vector<int>> gen_nodes;
            gen_nodes.emplace_back(root_attrs);
            if(!FreeConnexCheckWithGeneralizedNode(s3_subq, gen_nodes)) continue;

            if(!not_s3_subq.FreeConnexCheck()) continue;

            auto attr_intersect = s3_subq.AttrIntersection(not_s3_subq);
            for(int e2: not_s3)
            {

                auto attr_e2_s3_intersect = Intersection(relation_copies.at(e2).attributes, s3_subq.GetAllAttr());
                sort(attr_e2_s3_intersect.begin(), attr_e2_s3_intersect.end());
                sort(root_attrs.begin(), root_attrs.end());
                if(attr_e2_s3_intersect != root_attrs) continue;

                if(!SubseteqCheck(attr_intersect, relation_copies.at(e2).attributes)) continue;

                vector<int> s3_plus_e2 = s3; s3_plus_e2.push_back(e2);
                QueryInfo s3e2subq ={this->info, s3_plus_e2};
                if(!s3e2subq.FreeConnexCheck()) continue;

                bool cond6 = true;
                bool subset_cond_good = SubseteqCheck(attr_e2_s3_intersect, info.y);
                if(!subset_cond_good)
                {
                    SubseteqCheck(info.y, s3_subq.GetAllAttr()) ? cond6 = true : cond6 = false;
                }
                if(!cond6) continue;

                not_s3_subq.plan_root = {e2};
                assert(not_s3_subq.CheckValid());
                valid_subq_info.push_back(not_s3_subq);

            }

            auto candidate_attrs = Minus(info.GetAllAttr(), s3_subq.GetAllAttr());
            for(auto attr: candidate_attrs)
            {
                vector<int> e2_attrs = root_attrs;
                e2_attrs.push_back(attr);

                vector<vector<int>> gen_nodes;
                gen_nodes.push_back(e2_attrs);
                if(!FreeConnexCheckWithGeneralizedNode(not_s3_subq, gen_nodes)) continue;
                not_s3_subq.plan_root = {e2_attrs};
                if(!not_s3_subq.CheckValid()) continue;
                not_s3_subq.plan_root = {-1};

                if(!SubseteqCheck(attr_intersect, e2_attrs)) continue;

                gen_nodes.push_back(root_attrs);
                if(!FreeConnexCheckWithGeneralizedNode(s3_subq, gen_nodes)) continue;

                bool cond6 = true;
                bool subset_cond_good = SubseteqCheck(root_attrs, info.y);
                if(!subset_cond_good)
                {
                    SubseteqCheck(info.y, s3_subq.GetAllAttr()) ? cond6 = true : cond6 = false;
                }
                if(!cond6) continue;

                not_s3_subq.plan_root = {e2_attrs};
                assert(not_s3_subq.CheckValid());
                valid_subq_info.push_back(not_s3_subq);
            }

        }
        else
        {
            if(!s3_subq.FreeConnexCheck()) continue;
            if(!not_s3_subq.FreeConnexCheck()) continue;

            auto attr_intersect = s3_subq.AttrIntersection(not_s3_subq);
            for(int e2: not_s3)
            {
                if(!SubseteqCheck(attr_intersect, relation_copies.at(e2).attributes)) continue;

                vector<int> s3_plus_e2 = s3; s3_plus_e2.push_back(e2);
                QueryInfo s3e2subq ={this->info, s3_plus_e2};

                if(!s3e2subq.FreeConnexCheck()) continue;

                auto attr_e2_s3_intersect = Intersection(relation_copies.at(e2).attributes, s3_subq.GetAllAttr());
                auto root_attrs = relation_copies.at(info.plan_root.GetRelId()).attributes;
                if(!SubseteqCheck(attr_e2_s3_intersect, root_attrs)) continue;

                bool cond6 = true;
                bool subset_cond_good = SubseteqCheck(attr_e2_s3_intersect, info.y);
                if(!subset_cond_good)
                {
                    SubseteqCheck(info.y, s3_subq.GetAllAttr()) ? cond6 = true : cond6 = false;
                }
                if(!cond6) continue;

                not_s3_subq.plan_root = {e2};
                assert(not_s3_subq.CheckValid());
                valid_subq_info.push_back(not_s3_subq);

            }
        }

    }

    for(auto& qk: valid_subq_info)
    {
        auto [subquery_ptr,corr] = subq_manager.GetSubqHandler(qk);

        auto success = TryGenSubtree(*subquery_ptr, subqueries);
        if(success) subq_corrs.push_back(corr);

    }

    return !subqueries.empty();
}

Query::Query(const QueryInfo& qk):info(qk){}

QueryInfo Query::GetQueryKeyCopy() const {
    return info;
}

RelationCopy::RelationCopy(int label, const vector<int> &attrs)
{
    label_id = label;
    attributes = attrs;
}
void Plan::Print() const
{
    cout<<"plan root: ";
    plan_root.Print(cout);cout<<endl;
    cout<<", children: ";
    for(auto& c: children)
    {
        c->info.Print();
        cout<<"; ";
    }
    cout<<endl;
}

double Plan::CalcCost()
{
    if(min_cost!=DBL_MAX)return min_cost;
    min_cost = 0;
    for(auto& c: children)
    {
        auto child_cost = c->GetMinCost();

        child_cost += plan_root.IsGeneralized()? 0: c->GetUpdFreq();
        min_cost += child_cost;
    }
    min_cost+=plan_root.GetRootCost();

    return min_cost;
}
void Plan::UpdRefCnt(int change) {
    for (auto& child : children) {
        child->UpdRefCnt(change);
    }
}
vector<shared_ptr<Query>> Plan::GetParents() const {
    return parents;
}
void Plan::UpdSharedCost()
{
    min_cost = 0;
    for(auto& c: children)
    {
        auto child_cost = c->GetMinCost();

        child_cost += plan_root.IsGeneralized()? 0: c->GetUpdFreq();
        min_cost += child_cost;
    }
    min_cost+=plan_root.GetRootCost();
}

Plan::Plan(int rel_id)
{
    plan_root = {rel_id};
}

vector<shared_ptr<Plan>> Query::GetParents() const {
    return parents;
}
void Query::UpdRefCnt(int change) {
    actual_ref_count += change;
    assert(actual_ref_count >=0);
    assert(best_plan_idx!=-1);
    auto the_child = plans[best_plan_idx];
    the_child->UpdRefCnt(change);
}

bool Query::ReSelectOptChild() {

    old_opt_child = best_plan_idx;

    min_cost = numeric_limits<double>::max();
    int new_best = -1;

    int idx = 0;
    for (auto plan : plans) {

        double current_plan_cost = plan->CalcCost();

        if (current_plan_cost < min_cost) {
            min_cost = current_plan_cost;
            new_best = idx;
        }
        idx++;
    }

    if (new_best != old_opt_child) {
        best_plan_idx = new_best;
        return true;
    }
    old_opt_child = -1;
    return false;
}

void Query::ConstructAOD(int query_id, shared_ptr<FinalNode> parent)
{

    aod = make_shared<FinalNode>(info.plan_root);

    aod->y = info.y;

    auto& plan = plans.at(best_plan_idx);
    if(info.plan_root.CheckNotDecide())
    {
        aod->root = plan->plan_root;
    }
    else assert(aod->root == plan->plan_root);

    auto root_attrs = plan->plan_root.GetAllAttrs();
    int idx = 0;
    for(auto& child: plan->children)
    {
        bool child_exists = child->aod != nullptr;
        if(!child_exists)
            child->ConstructAOD(query_id,aod);

        vector<pair<int, int>> common_attr;
        auto child_attrs = child->info.plan_root.GetAllAttrs();
        auto corr = plan->child_corrs.at(idx).attr_corr;
        for (size_t i = 0; i < root_attrs.size(); ++i) {
            for (size_t j = 0; j < child_attrs.size(); ++j) {
                if (root_attrs.at(i) == corr.at(child_attrs.at(j))) {
                    common_attr.emplace_back(i, j);
                }
            }
        }
        aod->AddEdge(child->aod, aod,common_attr);
        if(!child_exists)
            child->aod->SetRelevantQid(query_id,child->aod->parents.size()-1);
        else
            child->aod->SpreadQueryId(query_id,child->aod->parents.size()-1);

        idx++;
    }

    aod->child_corrs = plan->child_corrs;
    assert(aod->child_corrs.size() == aod->children.size());

    if(parent == nullptr)
    {
        aod->relevant_qids.emplace_back();
        aod->relevant_qids[0].q_place_ids.emplace_back(query_id,0);
        aod->q2place_num.insert({query_id,1});
    }

}

void Query::ConstructPlanNaive()
{

    if(!naive_plans.empty())return;

    if(info.plan_root.CheckNotDecide())
    {
        for(auto& subq: subqueries)
        {
            subq->ConstructPlanNaive();
            cout<<"subquery naive plans: "<<subq->naive_plans.size()<<endl;
        }
        cout<<"plan naive num = "<<plan_naive_num<<endl;
        cout<<"plan naive edge num = "<<plan_naive_edge_num<<endl;
        return;
    }

    if(info.rel_copy_set.size() <= 1)
    {
        assert(info.rel_copy_set.size() == 1);
        naive_plans.emplace_back(make_shared<PlanNaive>(info.plan_root));
        plan_naive_num += 1;

        return;
    }
    for(auto& p: plans)
    {
        if(p->children.empty()) assert(0);

        for(auto& c: p->children)
        {
            c->ConstructPlanNaive();
        }

        vector<vector<shared_ptr<PlanNaive>>> child_naive_plans;
        for(auto& c: p->children)
        {
            child_naive_plans.emplace_back(c->naive_plans);
        }
        vector<shared_ptr<PlanNaive>> this_plan_naive_list;

        function<void(int, shared_ptr<PlanNaive>)> backtrack = [&](int idx, shared_ptr<PlanNaive> current_plan) {
            if (idx == child_naive_plans.size()) {
                this_plan_naive_list.push_back(current_plan);
                return;
            }
            for (const auto& child_plan : child_naive_plans[idx]) {

                auto new_plan = make_shared<PlanNaive>(*current_plan);
                new_plan->AddChild(child_plan);
                backtrack(idx + 1, new_plan);

            }
        };
        auto initial_plan = make_shared<PlanNaive>(p->plan_root);
        backtrack(0, initial_plan);
        naive_plans.insert(naive_plans.end(), this_plan_naive_list.begin(), this_plan_naive_list.end());
        plan_naive_num += this_plan_naive_list.size();
        for(auto& np: this_plan_naive_list)
        {
            plan_naive_edge_num += np->GetChildNum();
        }
    }

}

void Query::CalcAllPlanNum()
{

    if(plan_num != 0) return;

    if(info.plan_root.CheckNotDecide())
    {
        for(auto& subq: subqueries)
        {
            subq->CalcAllPlanNum();
            plan_num+= subq->plan_num;
        }
        return;
    }
    stored_plan_num += plans.size();

    if(info.rel_copy_set.size() <= 1)
    {
        assert(info.rel_copy_set.size() == 1);
        plan_num = 1;
        return;
    }
    for(auto& p: plans)
    {
        if(p->children.empty()) continue;
        int this_plan_num = 1;
        for(auto& c: p->children)
        {
            c->CalcAllPlanNum();
            this_plan_num *= c->plan_num;

        }

        plan_num += this_plan_num;
    }

}

void Query::TestSubqSame() const
{
    unordered_set<Query*> subq_set;
    for(auto sq: subqueries)
    {
        if(subq_set.count(sq.get()))
        {
            assert(0);
        }
        subq_set.insert(sq.get());
    }
}

void Query::SpreadQid(int qid)
{
    if(!possible_qids.count(qid))
    {
        possible_qids.insert(qid);
        for(auto& sq: subqueries)
        {
            sq->SpreadQid(qid);
        }
    }

}
