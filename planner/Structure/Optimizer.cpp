#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <algorithm>
#include "utils.h"
#include <unordered_map>
#include "Query.h"
#include "SubqManager.h"
#include <fstream>
#include "FinalNode.h"
#include <queue>
#include <sstream>
#include "SingleOut.h"
#include "MQOOut.h"

using namespace std;

unordered_set<shared_ptr<Query>> shared_set;
extern bool single_query_mode;

void PropagateRefCountUpdate(shared_ptr<Query> q) {

    if (q->old_opt_child !=-1) {
        auto old_child = q->plans[q->old_opt_child];
        old_child->UpdRefCnt(-(q->actual_ref_count));
        q->old_opt_child = -1;

        auto new_child = q->plans[q->best_plan_idx];
        new_child->UpdRefCnt(q->actual_ref_count);
    }

}

void InitRefCount(shared_ptr<Query> q) {
    q->UpdRefCnt(1);
}

double evaluateCost(const vector<shared_ptr<Query>>& all_qs) {

    double opt_cost = 0.0;

    for (const auto& q : all_qs) {

        double cost = q->GetMinCost();

        if (shared_set.count(q)) {
            opt_cost += cost;
        } else {
            opt_cost += cost * q->GetRefCnt();
        }
    }

    return opt_cost;
}

double calculateBenefit(shared_ptr<Query> q) {

    if (shared_set.count(q)) return 0.0;

    int refs = q->GetRefCnt();
    if (refs <= 1) return 0.0;

    double oh = q->GetMinCost();
    return (refs - 1) * oh;
}

void optimize(const vector<shared_ptr<Query>>& all_qs, vector<shared_ptr<Query>>& user_qs,shared_ptr<FinalNode> dummy_root) {
    cout<<"optimization start"<<endl;
    double initial_cost = evaluateCost(all_qs);

    for(auto& q: user_qs)
    {
        q->GetMinCost();
        InitRefCount(q);
    }
    if(single_query_mode){
        int qid = 0;
        for(auto& q: user_qs)
        {
            try {
            q->ConstructAOD(qid,nullptr);
            } catch(const exception& e) { cerr << "[ERROR] ConstructAOD(qid=" << qid << ") failed: " << e.what() << endl; throw; }
            try {
            dummy_root->AddEdge(q->aod, dummy_root,{});
            } catch(const exception& e) { cerr << "[ERROR] AddEdge failed: " << e.what() << endl; throw; }
            qid++;
        }
        if(dummy_root->children.size() == 0) {cout<<"no free-connex queries, return. "<<endl; return;}
        return;
    }

    while (true) {
        shared_ptr<Query> best_q = nullptr;
        double best_benefit = 0;

        for (const auto& q : all_qs) {
            double ben = calculateBenefit(q);
            if (ben > best_benefit) {
                best_benefit = ben;
                best_q = q;
            }
        }

        if (!best_q) break;

        shared_set.insert(best_q);
        cout<<"best q ref cnt: "<<best_q->GetRefCnt()<<' '<<best_q->GetMinCost()<<endl;

        best_q->SetShared();

        queue<shared_ptr<Query>> update_queue;
        unordered_set<shared_ptr<Query>> visited;

        for (const auto& plan_with_q : best_q->GetParents()) {
            plan_with_q->UpdSharedCost();
            for (const auto& parent_q : plan_with_q->GetParents()) {
                update_queue.push(parent_q);
                visited.insert(parent_q);
            }

        }

        while (!update_queue.empty()) {
            auto current_node = update_queue.front();
            update_queue.pop();

            bool opt_child_changed = current_node->ReSelectOptChild();

            if (opt_child_changed) {

                PropagateRefCountUpdate(current_node);

                for (const auto& parent_plan : current_node->GetParents()) {
                    parent_plan->UpdSharedCost();
                    for (const auto& parent_q : parent_plan->GetParents()) {
                        if(!visited.count(parent_q))
                        {
                            update_queue.push(parent_q);
                            visited.insert(parent_q);
                        }

                    }
                }
            }
        }

        double new_cost = evaluateCost(all_qs);

    }

    int qid = 0;
    for(auto& q: user_qs)
    {

        bool found_shared_aod = false;
        if(q->best_plan_idx != -1 && !q->plans.empty())
        {
            auto my_best_plan = q->plans.at(q->best_plan_idx);
            for(int i = 0; i < qid; i++)
            {
                auto& other_q = user_qs[i];
                if(other_q->aod != nullptr && other_q->best_plan_idx != -1 && !other_q->plans.empty())
                {
                    if(my_best_plan == other_q->plans.at(other_q->best_plan_idx))
                    {
                        q->aod = other_q->aod;

                        q->aod->SpreadQueryId(qid, q->aod->parents.size()-1);
                        found_shared_aod = true;
                        break;
                    }
                }
            }
        }

        if(!found_shared_aod)
        {
            q->ConstructAOD(qid,nullptr);
        }
        dummy_root->AddEdge(q->aod, dummy_root,{});
        qid++;
    }
    if(dummy_root->children.size() == 0) {cout<<"no free-connex queries, return. "<<endl; return;}

    cout << "Optimization Finished." << endl;
}
