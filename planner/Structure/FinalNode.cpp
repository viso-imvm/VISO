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
#include "FinalNode.h"
using namespace std;

extern int global_plan_node_id;
extern int generalized_node_num;
extern vector<RelationCopy> relation_copies;

void FinalNode::AddEdge(shared_ptr<FinalNode> child,shared_ptr<FinalNode> this_shared_ptr, vector<pair<int, int>> common_attr)
{
    children.push_back(child);
    qid_maps.emplace_back();
    child->parents.push_back(this_shared_ptr);
    child->relevant_qids.push_back({});
    child->in_connex.push_back(false);

    common_attr_children.push_back({common_attr});
    child->common_attr_parents.push_back({common_attr});
    if(debug)
    {
        cout<<"Add edge from "<<GetName()<<" to "<<child->GetName()<<", common_attr: "
            <<common_attr_children.back().ToString()<<endl;
    }
    int parent_idx = child->parents.size() -1;
    parent_idxs.push_back(parent_idx);
}

tuple<string,int,int> FinalNode::FindAttrInConnex(int parent_idx, int qid,int placeid, int out_attr)
{
    if(parent_idx!=-1 && in_connex[parent_idx] == false) return {"",-1,-1};
    if(find(y.begin(), y.end(), out_attr) == y.end()) return {"",-1,-1};
    auto root_attrs = root.GetAllAttrs();
    auto it = find(root_attrs.begin(), root_attrs.end(), out_attr);
    if(it != root_attrs.end()) return {GetName(), placeid, it-root_attrs.begin()};

     int child_idx = 0;
    for(auto c: children)
    {
        int pi = parent_idxs[child_idx];

        auto [_,child_place_id] = qid_maps[child_idx].parent2child_qid[{qid,placeid}];

        int child_out_attr = -1;
        auto corrs = child_corrs[child_idx];
        for(auto [c_attr, attr]: corrs.attr_corr)
        {
            if(attr == out_attr)
            {
                child_out_attr = c_attr;
                break;
            }
        }
        if(child_out_attr == -1)
        {
            child_idx++;
            continue;
        }

        auto [child_table, cp, c_idx] = c->FindAttrInConnex(pi, qid, child_place_id, child_out_attr);
        if(!child_table.empty()) return {child_table, cp, c_idx};
        child_idx++;
    }
    return {"",-1,-1};

}
vector<int> FinalNode::GetRelevantQueries() const
{
    vector<int> qids;
    for (const auto& rq : relevant_qids) {
        for(const auto& [qid, place_id] : rq.q_place_ids)
            qids.push_back(qid);
    }
    return qids;
}

string CommonAttrs::ToString() const
{
    string s;
    for(auto& [p_idx, c_idx]: attrs)
    {
        s += to_string(p_idx) + ":" + to_string(c_idx) + ",";
    }
    if(!s.empty()) s.pop_back();
    return s;
}

void FinalNode::SetInConnex(int parent_idx, const vector<int>& attr_need_output)
{

    if(parent_idx == -1)
    {
        assert(parents.empty());
        parent_idx =0;
    }
    in_connex[parent_idx] = true;

    auto to_output_vec = Minus(attr_need_output, root.GetAllAttrs());
    auto to_output_set = VectorToUnorderedSet(to_output_vec);

    int child_idx = 0;
    for(auto c: children)
    {
        vector<int> child_attr_need_output;
        for(auto child_attr: c->y)
        {
            auto my_attr = child_corrs[child_idx].attr_corr.at(child_attr);
            if(to_output_set.count(my_attr))
            {
                to_output_set.erase(my_attr);
                child_attr_need_output.emplace_back(child_attr);
            }
        }
        if(!child_attr_need_output.empty())
            c->SetInConnex(parent_idxs[child_idx], child_attr_need_output);
        child_idx ++;
    }
}

void FinalNode::SetRelevantQid(int qid, int parent_idx)
{

    assert(parent_idx != -1);
    int place_id = 0;
    if(q2place_num.count(qid))
    {
        place_id = q2place_num[qid];
        q2place_num[qid]+= 1;
    }
    else
    {
        q2place_num[qid] = 1;
    }

    relevant_qids[parent_idx].q_place_ids.emplace_back(qid,place_id);
}

void FinalNode::SpreadQueryId(int qid, int parent_idx)
{
    SetRelevantQid(qid, parent_idx);

    int child_idx = 0;
    for(auto& child: children)
    {
        child->SpreadQueryId(qid,parent_idxs[child_idx]);
        child_idx++;
    }
}
int FinalNode::GetId() {
    if(id == -1) {
        id = global_plan_node_id;
        global_plan_node_id++;
    }
    return id;
}

int FinalNode::GetLabelId()const
{
    assert(root.GetRelId() != -1);
    return relation_copies[root.GetRelId()].label_id;
}

void FinalNode::SetSameLabelId() {
    if(same_label_id != -1) return;

    if(root.IsGeneralized())
    {
        generalized_node_num++;
        same_label_id = generalized_node_num;
        return;
    }
    int label_id = GetLabelId();
    if(label_id >= label2node_num.size())
    {
        label2node_num.resize(label_id+1,0);
    }
    label2node_num[label_id]++;
    same_label_id = label2node_num[label_id];
}

string FinalNode::GetName() const {
    string name;
    if(root.CheckNotDecide())
        return "dummy_root";
    if(root.IsGeneralized())
    {
        name =  "generalized";
        name += same_label_id > 0 ? to_string(same_label_id) : "";
        return name;
    }
    name = label2name[GetLabelId()];
    name += GetSameLabelId() > 0 ? to_string(GetSameLabelId()) : "";
    return name;

}

vector<int> FinalNode::GetRootOutputAttrs() const
{
    auto attrs = root.GetAllAttrs();
    auto out_attrs = Intersection(y,attrs);

    return root.ConvertToAttrIdxs(out_attrs);
}

int FinalNode::GetSameLabelId() const { return same_label_id; }
