#pragma once
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
#include <boost/functional/hash.hpp>
#include "QueryInfo.h"
using namespace std;

extern vector<int> label2node_num;
extern vector<string> label2name;
class PlanRoot;
struct CommonAttrs
{
    vector<pair<int, int>> attrs;
    string ToString() const ;
};

struct RelevantQids
{
    vector<pair<int,int>> q_place_ids;
};

struct QidMap
{
    unordered_map<pair<int,int>,pair<int,int>,boost::hash<pair<int,int>>> parent2child_qid;
};
class FinalNode: public std::enable_shared_from_this<FinalNode>
{
private:
    int id = -1;
    int same_label_id = -1;
public:

    PlanRoot root;
    vector<int> y;
    vector<shared_ptr<FinalNode>> children;
    vector<shared_ptr<FinalNode>> parents;
    vector<int> parent_idxs;

    unordered_map<int,int> q2place_num;
    vector<RelevantQids> relevant_qids;

    vector<QidMap> qid_maps;

    vector<bool> in_connex;

    boost::unordered_map<vector<int>, pair<bool, vector<int>>> output_infos;
    boost::unordered_map<vector<int>,string> key2rc_name;

    vector<RenameCorr> child_corrs;

    vector<CommonAttrs> common_attr_children;
    vector<CommonAttrs> common_attr_parents;

public:
    tuple<string,int,int> FindAttrInConnex(int parent_idx, int qid,int placeid, int out_attr);
    vector<int> GetRelevantQueries() const;

    void AddEdge(shared_ptr<FinalNode> child, shared_ptr<FinalNode> this_shared_ptr, vector<pair<int, int>> common_attr);
    void SetInConnex(int parent_idx, const vector<int> &attr_need_output);

    void SetRelevantQid(int qid, int parent_idx);

    FinalNode(const PlanRoot &r) : root(r) {}
    FinalNode()=default;
    void SpreadQueryId(int qid,int parent_idx);
    int GetId();
    int GetLabelId()const ;
    void SetSameLabelId();
    int GetSameLabelId() const;
    string GetName() const;
    vector<int> GetRootOutputAttrs() const;
};
