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
#include "QueryInfo.h"
using namespace std;

extern vector<double> rel_label2upd_freq_est;

extern vector<RelationCopy> relation_copies;

void QueryInfo::Print() const
{
    cout << "Plan root: ";
    plan_root.Print(cout);cout<<endl;

    cout << "Relations: ";
    for(int rel_id: rel_copy_set)
    {
        cout << rel_id << " ";
    }
    cout << endl;

    cout << "Output attributes (y): ";
    for(int attr: y)
    {
        cout << attr << " ";
    }
    cout << endl;
}

vector<int> QueryInfo::GetAllAttr() const
{

    vector<int> attrs;
    for (int rel_id : rel_copy_set) {
        auto& rel = relation_copies.at(rel_id);
        attrs.insert(attrs.end(), rel.attributes.begin(), rel.attributes.end());
    }

    sort(attrs.begin(), attrs.end());
    attrs.erase(unique(attrs.begin(), attrs.end()), attrs.end());
    return attrs;
}

unordered_map<int,int> QueryInfo::GetAllAttrWithCnt() const
{

    unordered_map<int,int> ans;
    for (int rel_id : rel_copy_set) {
        auto& rel = relation_copies.at(rel_id);
        for(int attr: rel.attributes)
        {
            if(ans.count(attr) == 0)
                ans.insert({attr,0});
            ans.at(attr)++;
        }
    }

    return ans;
}
void QueryInfo::SetY(const vector<int>& output_attrs)
{

    auto all_attrs = GetAllAttr();
    y.clear();
    for (int attr : output_attrs) {
        if (find(all_attrs.begin(), all_attrs.end(), attr) != all_attrs.end()) {
            y.push_back(attr);
        }
    }
}

QueryInfo::QueryInfo(const QueryInfo& parent_query, vector<int> & rel_set, int root_rel_id)
{
    if(root_rel_id != -2)
        plan_root = {root_rel_id};
    else
        plan_root = parent_query.plan_root;

    rel_copy_set = rel_set;
    sort(rel_copy_set.begin(), rel_copy_set.end());
    SetY(parent_query.y);
}
vector<int> QueryInfo::AttrIntersection(const QueryInfo& other_query) const
{

    auto attrs1 = GetAllAttr();
    auto attrs2 = other_query.GetAllAttr();
    return Intersection(attrs1, attrs2);
}

bool QueryInfo::FreeConnexCheck() const
{

    unordered_map<int, unordered_set<int>> rel_2_shared_attr;
    auto all_attrs = GetAllAttrWithCnt();
    for(int rel_id: rel_copy_set)
    {
        rel_2_shared_attr.insert({rel_id,{}});

        auto& attrs = relation_copies.at(rel_id).attributes;
        for(int attr: attrs)
        {
            if(all_attrs.at(attr) > 1)
            {
                rel_2_shared_attr.at(rel_id).insert(attr);
            }
        }
    }

    while(true)
    {
        unordered_set<int> ears;
        for(auto& [rel_id, shared_attrs]: rel_2_shared_attr)
        {
            for(auto& [other_rel_id, other_shared_attrs]: rel_2_shared_attr)
            {
                if(rel_id == other_rel_id) continue;

                if(SubseteqCheck(shared_attrs, other_shared_attrs))
                {
                    ears.insert(rel_id);

                    break;
                }
            }
        }
        if(ears.empty()) break;
        for(int ear: ears)
        {

            vector<int> attr_no_longer_shared;
            for(int attr: rel_2_shared_attr.at(ear))
            {

                all_attrs.at(attr)--;
                if(all_attrs.at(attr) <= 1)
                {
                    attr_no_longer_shared.push_back(attr);
                }
            }
            for(auto& [other_rel_id, other_shared_attrs]: rel_2_shared_attr)
            {
                if(other_rel_id == ear) continue;
                for(int attr: attr_no_longer_shared)
                {
                    other_shared_attrs.erase(attr);
                }
            }

            rel_2_shared_attr.erase(ear);

        }

    }

    if(rel_2_shared_attr.size() <=1) return true;
    else {

        return false;
    }
}

string QueryInfo::GenIdentifierIso() const
{
    string s = plan_root.ToStringIso();

    map<int,int> rel_cnt;
    for(int copy_id: rel_copy_set)
    {
        int label_id = relation_copies.at(copy_id).label_id;
        if(rel_cnt.count(label_id))
            rel_cnt.at(label_id)++;
        else
            rel_cnt.insert({label_id,1});
    }

    vector<int> ans_vec;
    for(auto& [label_id, cnt]: rel_cnt)
    {
        ans_vec.push_back(label_id);
        ans_vec.push_back(cnt);
    }
    ans_vec.push_back(-1);
    ans_vec.push_back(y.size());
    s += {(char*)(&ans_vec.at(0)), ans_vec.size() * sizeof(int)};
    s += '\0';

    return s;
}

size_t QueryInfoComparedByIso::ComputeHash() const {

    auto s = info.GenIdentifierIso();

    return hash<string>()(s);
}

size_t QueryInfo::ComputeHash() const {

    string s = plan_root.ToString();

    assert(is_sorted(rel_copy_set.begin(), rel_copy_set.end()));
    s += {(char*)(&rel_copy_set.at(0)), rel_copy_set.size() * sizeof(int)};
    s += '\0';

    assert(is_sorted(y.begin(), y.end()));
    s += {(char*)(&y.at(0)), y.size() * sizeof(int)};
    s += '\0';

    return hash<string>()(s);
}

bool QueryInfo::CheckValid() const
{
    if(plan_root.CheckNotDecide()) return true;
    if(plan_root.IsGeneralized())
    {
        auto attrs = plan_root.GetGeneralizedAttrs();
        auto all_attrs = GetAllAttr();
        for(int attr: attrs)
        {
            if(find(all_attrs.begin(), all_attrs.end(), attr) == all_attrs.end())
                return false;
        }
        return true;
    }
    else
    {
        int rel_id = plan_root.GetRelId();
        if(find(rel_copy_set.begin(), rel_copy_set.end(), rel_id) == rel_copy_set.end())
            return false;
        else
            return true;
    }
    return false;
}

bool QueryInfo::IsConnected() const
{

    if(rel_copy_set.size() <= 1) return true;
    unordered_map<int, unordered_set<int>> adj;
    for(int rel_id: rel_copy_set)
    {
        adj.insert({rel_id,{}});
    }
    for(int i = 0; i < (int)rel_copy_set.size(); i++)
    {
        for(int j = i + 1; j < (int)rel_copy_set.size(); j++)
        {
            int rel_id1 = rel_copy_set.at(i);
            int rel_id2 = rel_copy_set.at(j);
            auto& attrs1 = relation_copies.at(rel_id1).attributes;
            auto& attrs2 = relation_copies.at(rel_id2).attributes;
            if(!Intersection(attrs1, attrs2).empty())
            {
                adj.at(rel_id1).insert(rel_id2);
                adj.at(rel_id2).insert(rel_id1);
            }
        }
    }

    unordered_set<int> visited;
    vector<int> stack;
    stack.push_back(rel_copy_set.at(0));
    while(!stack.empty())
    {
        int rel_id = stack.back();
        stack.pop_back();
        if(visited.count(rel_id)) continue;
        visited.insert(rel_id);
        for(int neighbor: adj.at(rel_id))
        {
            if(!visited.count(neighbor))
            {
                stack.push_back(neighbor);
            }
        }
    }
    if(visited.size() == rel_copy_set.size())
    {

        return true;
    }
    else return false;
}
double QueryInfo::GetUpdFreq() const
{
    double freq = 0;
    for(int rel_id: rel_copy_set)
    {
        int label_id = relation_copies.at(rel_id).label_id;
        if(label_id >= 0 && label_id < rel_label2upd_freq_est.size())
        {
            freq += rel_label2upd_freq_est[label_id];
        }
        else
        {
            cerr<<"Error: label_id = "<<label_id<<", rel_id = "<<rel_id<<endl;
            assert(false);
        }
    }
    return freq;
}
bool QueryInfoComparedByIso::operator==(const QueryInfoComparedByIso &other) const
{

    if(info.GenIdentifierIso() != other.info.GenIdentifierIso())
    {

        return false;
    }

    return CheckIsomorphic(info, other.info);
}

bool QueryInfo::operator==(const QueryInfo& other) const {
    assert(is_sorted(rel_copy_set.begin(), rel_copy_set.end()));
    assert(is_sorted(other.rel_copy_set.begin(), other.rel_copy_set.end()));
    assert(is_sorted(y.begin(), y.end()));
    assert(is_sorted(other.y.begin(), other.y.end()));
    return plan_root == other.plan_root &&
           rel_copy_set == other.rel_copy_set &&
           y == other.y;
}

size_t QueryInfoHash::operator()(const QueryInfo& key) const {
    return key.ComputeHash();
}

QueryInfo::QueryInfo(const QueryInfo& query, const RenameCorr& corr):plan_root(query.plan_root,corr)
{

    rel_copy_set.clear();
    for(int rel_id: query.rel_copy_set)
    {
        if(corr.rel_corr.count(rel_id))
            rel_copy_set.push_back(corr.rel_corr.at(rel_id));
        else
            rel_copy_set.push_back(rel_id);
    }
    sort(rel_copy_set.begin(), rel_copy_set.end());

    y.clear();
    for(int attr: query.y)
    {
        if(corr.attr_corr.count(attr))
            y.push_back(corr.attr_corr.at(attr));
        else
            y.push_back(attr);
    }
    sort(y.begin(), y.end());
}

PlanRoot::PlanRoot(const PlanRoot& other, const RenameCorr& corr)
{
    if(other.IsGeneralized())
    {
        auto attrs = other.GetGeneralizedAttrs();
        vector<int> new_attrs;
        for(int attr: attrs)
        {
            if(corr.attr_corr.count(attr))
                new_attrs.push_back(corr.attr_corr.at(attr));
            else
                new_attrs.push_back(attr);
        }
        sort(new_attrs.begin(), new_attrs.end());
        rel_id = -1;
        general_attrs = new_attrs;
    }
    else
    {
        if(corr.rel_corr.count(other.rel_id))
            rel_id = corr.rel_corr.at(other.rel_id);
        else
            rel_id = other.rel_id;
        general_attrs.clear();
    }
}

void RelationCopy::Print(ostream& out) const
{
    out << label_id << ", .at(";
    for(size_t i = 0; i < attributes.size(); i++)
    {
        out << attributes.at(i);
        if(i != attributes.size() - 1) out << ", ";
    }
    out << ")";
}
