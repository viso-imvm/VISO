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
#include "QueryInfo.h"
#include <fstream>
using namespace std;

extern vector<RelationCopy> relation_copies;
extern vector<double> rel_label2upd_freq_est;

PlanRoot::PlanRoot(int r_id): rel_id(r_id) {}
PlanRoot::PlanRoot(const vector<int>& attrs): rel_id(-1), general_attrs(attrs) {
    assert(!attrs.empty());
    sort(general_attrs.begin(), general_attrs.end());
}
bool PlanRoot::CheckIn(const vector<int>& s) const
{
    if(rel_id != -1)
    {
        return find(s.begin(), s.end(), rel_id) != s.end();
    }
    else
    {
        assert(!general_attrs.empty());

        unordered_set<int> s_attrs;
        for(int rel_id: s)
        {
            auto& attrs = relation_copies[rel_id].attributes;
            for(int attr: attrs)
            {
                s_attrs.insert(attr);
            }
        }

        for(int attr: general_attrs)
        {
            if(!s_attrs.count(attr))
                return false;
        }
        return true;
    }
}
bool PlanRoot::IsGeneralized() const
{
    return rel_id == -1 && !general_attrs.empty();
}
vector<int> PlanRoot::GetGeneralizedAttrs() const
{
    assert(IsGeneralized());
    return general_attrs;
}
int PlanRoot::GetRelId() const
{
    return rel_id;
}

double PlanRoot::GetRootCost() const
{
    if(IsGeneralized())
    {
        return 0;
    }
    assert(rel_id!=-1);
    int label_id = relation_copies[rel_id].label_id;
    if(!(label_id >= 0 && label_id < rel_label2upd_freq_est.size()))
    {
        cerr<<"Error: label_id = "<<label_id<<", rel_id = "<<rel_id<<endl;
        assert(false);
    }
    assert(label_id >= 0 && label_id < rel_label2upd_freq_est.size());
    return rel_label2upd_freq_est[label_id];
}
string PlanRoot::ToStringIso() const {
    string ans;
    if(rel_id != -1)
    {
        ans += 'R';
        int label_rel_id = relation_copies.at(rel_id).label_id;
        ans +={(char*)(&label_rel_id), sizeof(label_rel_id)};
    }
    else
    {
        ans += 'G';
        int general_attr_num = general_attrs.size();
        ans += {(char*)(&general_attr_num), sizeof(general_attr_num)};

    }

    ans += '\0';

    return ans;
}
string PlanRoot::ToString() const
{
    string s;
    if(IsGeneralized())
    {
        s += "G";
        s += {(char*)(&general_attrs.at(0)), general_attrs.size() * sizeof(int)};
        s += '\0';
    }
    else
    {
        s += "R";
        s += {(char*)(&rel_id), sizeof(rel_id)};
        s += '\0';
    }
    return s;
}
bool PlanRoot::operator==(const PlanRoot& other) const {
    return rel_id == other.rel_id && general_attrs == other.general_attrs;
}
bool PlanRoot::CheckNotDecide() const
{
    return rel_id == -1 && general_attrs.empty();
}

void PlanRoot::Print(ostream& out) const
{
    if(IsGeneralized())
    {
        out << "Generalized(";
        auto attrs = GetGeneralizedAttrs();
        for(int attr: attrs)
        {
            out << attr << " ";
        }
        out << ")";
    }
    else
    {
        out << GetRelId();
    }
}

vector<int> PlanRoot::ConvertToAttrIdxs(const vector<int>& attrs) const
{
    vector<int> attr_idxs;
    for(int attr: attrs)
    {
        int idx = GetAttrIdx(attr);
        if(idx != -1)
        {
            attr_idxs.push_back(idx);
        }
        else
        {
            assert(0);
        }
    }
    return attr_idxs;
}
int PlanRoot::GetAttrIdx(int attr_id) const
{
    if(IsGeneralized())
    {
        auto it = find(general_attrs.begin(), general_attrs.end(), attr_id);
        if(it != general_attrs.end())
        {
            return distance(general_attrs.begin(), it);
        }
        else
        {
            return -1;
        }
    }
    else
    {
        auto& attrs = relation_copies[rel_id].attributes;
        auto it =  find(attrs.begin(), attrs.end(), attr_id);
        if(it != attrs.end())
        {
            return distance(attrs.begin(), it);
        }
        else
        {
            return -1;
        }
    }
}
void PlanRoot::PrintWithLabel(ostream &out) const
{
    if(IsGeneralized())
    {
        out << "Generalized(";
        auto attrs = GetGeneralizedAttrs();
        for(int attr: attrs)
        {
            out<<attr<<" ";
        }
        out << ")";
    }
    else
    {
        if(rel_id == -1)
        {
            out << -1;
            return;
        }
        out<<GetRelId()<<":";
        relation_copies[GetRelId()].Print(out);
    }
}

vector<int> PlanRoot::GetAllAttrs() const {
    if(IsGeneralized()) return general_attrs;
    else {
        return relation_copies.at(rel_id).attributes;
    }
}
