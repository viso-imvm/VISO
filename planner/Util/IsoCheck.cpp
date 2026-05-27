#include <vector>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <iostream>
#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <algorithm>
#include "utils.h"
#include <unordered_map>
#include "Query.h"
#include "IsoCheck.h"
#include "SubqManager.h"
#include <cassert>
using namespace std;

extern vector<RelationCopy> relation_copies;

IsoChecker::IsoChecker(const QueryInfo& q1, const QueryInfo& q2)
    : q1_(q1), q2_(q2) {

    auto q1_attr_copies = q1_.GetAllAttr();
    auto q2_attr_copies = q2_.GetAllAttr();
    q1_attr_num = q1_attr_copies.size();
    q2_attr_num = q2_attr_copies.size();

    rel_mapping_.resize(q1.rel_copy_set.size(), -1);
    q2_rel_used_.resize(q2.rel_copy_set.size(), false);
    for(int attr: q1_attr_copies)
        attr_mapping_.insert({attr,-1});
    for(int attr: q2_attr_copies)
        q2_attr_used_.insert({attr,false});

    buildNeighborhoodInfo();
}

bool IsoChecker::areIsomorphic() {

    if (!checkBasicSizes()) return false;

    return matchRelations(0);
}

bool IsoChecker::checkBasicSizes() const {
    return q1_.rel_copy_set.size() == q2_.rel_copy_set.size() &&
            q1_.y.size() == q2_.y.size() &&
            q1_attr_num == q2_attr_num &&
            q1_.rel_copy_set.size() == q2_.rel_copy_set.size();
}

void IsoChecker::buildNeighborhoodInfo() {

    q1_rel_neighbors_.resize(q1_.rel_copy_set.size());
    q2_rel_neighbors_.resize(q2_.rel_copy_set.size());

    for (size_t i = 0; i < q1_.rel_copy_set.size(); ++i) {
        for(int j = 0; j< q1_.rel_copy_set.size();j++)
        {
            if(i == j) continue;
            auto inter = Intersection(relation_copies.at(q1_.rel_copy_set.at(i)).attributes, relation_copies.at(q1_.rel_copy_set.at(j)).attributes);
            if(inter.empty()) continue;
            q1_rel_neighbors_.at(i).emplace_back(j);
        }
    }
    for (size_t i = 0; i < q2_.rel_copy_set.size(); ++i) {
        for(int j = 0; j< q2_.rel_copy_set.size();j++)
        {
            if(i == j) continue;
            auto inter = Intersection(relation_copies.at(q2_.rel_copy_set.at(i)).attributes, relation_copies.at(q2_.rel_copy_set.at(j)).attributes);
            if(inter.empty()) continue;
            q2_rel_neighbors_.at(i).emplace_back(j);
        }
    }
}

bool IsoChecker::matchRelations(int q1_rel_index) {

    if (q1_rel_index >= q1_.rel_copy_set.size()) {

        return checkRemainingConstraints();
    }

    std::vector<int> candidates = findRelationCandidates(q1_rel_index);

    for (int q2_candidate : candidates) {
        if (tryRelationMapping(q1_rel_index, q2_candidate)) {

            if (matchRelations(q1_rel_index + 1)) {
                return true;
            }

            undoRelationMapping(q1_rel_index, q2_candidate);
        }
    }

    return false;
}

std::vector<int> IsoChecker::findRelationCandidates(int q1_rel_index) const {
    const RelationCopy& q1_rel = relation_copies.at(q1_.rel_copy_set.at(q1_rel_index));
    std::vector<int> candidates;

    for (size_t i = 0; i < q2_.rel_copy_set.size(); ++i) {
        if (q2_rel_used_.at(i)) continue;

        const RelationCopy& q2_rel = relation_copies.at(q2_.rel_copy_set.at(i));

        if (q1_rel.label_id != q2_rel.label_id) continue;

        if (!checkPartialAttributeMapping(q1_rel.attributes, q2_rel.attributes)) continue;

        if (!satisfiesLookAheadConstraints(q1_rel_index, i)) continue;

        candidates.push_back(i);
    }

    return candidates;
}

bool IsoChecker::checkPartialAttributeMapping(const std::vector<int>& q1_attrs,
                                    const std::vector<int>& q2_attrs) const {

    for (size_t i = 0; i < q1_attrs.size(); ++i) {
        int q1_attr = q1_attrs.at(i);
        int q2_attr = q2_attrs.at(i);

        if (attr_mapping_.at(q1_attr) != -1) {
            if (attr_mapping_.at(q1_attr) != q2_attr) {
                return false;
            }
        } else {

            if (q2_attr_used_.at(q2_attr)) {
                return false;
            }
        }
    }
    return true;
}

bool IsoChecker::satisfiesLookAheadConstraints(int q1_rel, int q2_rel) const {

    int q1_unmapped_neighbors = 0;
    int q2_unmapped_neighbors = 0;

    for (int rel_idx : q1_rel_neighbors_.at(q1_rel)) {
        if (rel_mapping_.at(rel_idx) == -1) q1_unmapped_neighbors++;
    }
    for (int rel_idx : q2_rel_neighbors_.at(q2_rel)) {
        if (!q2_rel_used_.at(rel_idx)) q2_unmapped_neighbors++;
    }

    return q1_unmapped_neighbors == q2_unmapped_neighbors;
}

bool IsoChecker::tryRelationMapping(int q1_rel, int q2_rel) {

    rel_mapping_.at(q1_rel) = q2_rel;
    q2_rel_used_.at(q2_rel) = true;

    const auto& q1_attrs = relation_copies.at(q1_.rel_copy_set.at(q1_rel)).attributes;
    const auto& q2_attrs = relation_copies.at(q2_.rel_copy_set.at(q2_rel)).attributes;

    for (size_t i = 0; i < q1_attrs.size(); ++i) {
        int q1_attr = q1_attrs.at(i);
        int q2_attr = q2_attrs.at(i);

        if (attr_mapping_.at(q1_attr) == -1) {
            attr_mapping_.at(q1_attr) = q2_attr;
            q2_attr_used_.at(q2_attr) = true;
        }
    }

    return true;
}

void IsoChecker::undoRelationMapping(int q1_rel, int q2_rel) {
    rel_mapping_.at(q1_rel) = -1;
    q2_rel_used_.at(q2_rel) = false;

    const auto& q1_attrs = relation_copies.at(q1_.rel_copy_set.at(q1_rel)).attributes;
    const auto& q2_attrs = relation_copies.at(q2_.rel_copy_set.at(q2_rel)).attributes;

    for (size_t i = 0; i < q1_attrs.size(); ++i) {
        int q1_attr = q1_attrs.at(i);
        int q2_attr = q2_attrs.at(i);

        if (attr_mapping_.at(q1_attr) == q2_attr) {
            attr_mapping_.at(q1_attr) = -1;
            q2_attr_used_.at(q2_attr) = false;
        }
    }
}

bool IsoChecker::checkRemainingConstraints() {

    if (!checkRelCopySet()) return false;

    if (!checkOutputAttributes()) return false;

    if (!checkRoot()) return false;

    return true;
}

bool IsoChecker::checkRelCopySet() const {

    std::vector<int> q1_mapped;
    for (int rel_idx =0;rel_idx< q1_.rel_copy_set.size();rel_idx++) {
        if (rel_idx < 0 || rel_idx >= rel_mapping_.size() || rel_mapping_.at(rel_idx) == -1) {

            return false;
        }
        q1_mapped.push_back(q2_.rel_copy_set.at(rel_mapping_.at(rel_idx)));
    }

    std::vector<int> q2_sorted = q2_.rel_copy_set;
    std::sort(q1_mapped.begin(), q1_mapped.end());
    std::sort(q2_sorted.begin(), q2_sorted.end());

    return q1_mapped == q2_sorted;
}

bool IsoChecker::checkOutputAttributes() const {
    if (q1_.y.size() != q2_.y.size()) return false;

    for (size_t i = 0; i < q1_.y.size(); ++i) {
        int q1_attr = q1_.y.at(i);
        int q2_attr = q2_.y.at(i);

        if (q1_attr < 0 || !attr_mapping_.count(q1_attr) || attr_mapping_.at(q1_attr) != q2_attr) {

            return false;
        }
    }

    return true;
}

bool IsoChecker::checkRoot() const {
    if(q1_.plan_root.IsGeneralized())
    {
        if(!q2_.plan_root.IsGeneralized()) return false;

        auto q1_root_attrs = q1_.plan_root.GetGeneralizedAttrs();
        auto q2_root_attrs = q2_.plan_root.GetGeneralizedAttrs();
        if(q1_root_attrs.size() != q2_root_attrs.size()) return false;

        for(int i = 0; i< q1_root_attrs.size();i++)
        {
            int q1_attr = q1_root_attrs.at(i);
            int q2_attr = q2_root_attrs.at(i);
            if(attr_mapping_.at(q1_attr) != q2_attr) {

                return false;
            }
        }

        return true;
    }
    else
    {
        if(q2_.plan_root.IsGeneralized()) return false;

        auto q1_rel_copy_id = q1_.plan_root.GetRelId();

        int q1_rel_idx = -1;
        for(int i = 0; i< q1_.rel_copy_set.size();i++)
        {
            if(q1_.rel_copy_set.at(i) == q1_rel_copy_id)
            {
                q1_rel_idx = i;
                break;
            }
        }

        int q2_rel_idx = rel_mapping_.at(q1_rel_idx);
        return q2_.rel_copy_set.at(q2_rel_idx) == q2_.plan_root.GetRelId();

    }

}

RenameCorr IsoChecker::getRenameCorr() const {
    RenameCorr corr;
    for(int i = 0; i < rel_mapping_.size(); ++i)
    {
        if(rel_mapping_.at(i) != -1)
        {
            auto q1_rel = q1_.rel_copy_set.at(i);
            auto q2_rel = q2_.rel_copy_set.at(rel_mapping_.at(i));
            corr.rel_corr.insert({q1_rel, q2_rel});
        }
    }
    for(auto& [q1_attr, q2_attr]: attr_mapping_)
    {
        if(q2_attr != -1)
        {
            corr.attr_corr.insert({q1_attr,q2_attr});
        }
    }
    return corr;
}

bool CheckIsomorphic(const QueryInfo& q1, const QueryInfo& q2) {
    IsoChecker checker(q1, q2);

    bool ans = checker.areIsomorphic();

    return ans;
}

RenameCorr GetCorr(const QueryInfo &q1, const QueryInfo &q2)
{
    IsoChecker checker(q1, q2);
    assert(checker.areIsomorphic());
    return checker.getRenameCorr();
}
