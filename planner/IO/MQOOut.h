#pragma once
#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <memory>
#include "Query.h"
#include "FinalNode.h"
#include <queue>
#include <fstream>

using namespace std;
extern unordered_map<int,int> input_rl_id2rl_id;

class MultiQueryOutput
{
    struct RelationConfig
    {
        string name;
        string rel_type;
        int num_children = 1;
        vector<int> output_attr;
    };
    struct ConnectionConfig{
        string from_rel;
        string to_rel;
        vector<pair<int,int>> relevant_qs;
        vector<int> join_keys_parent;
        vector<int> join_keys_child;
        bool child_in_connex;
        unordered_map<pair<int,int>,pair<int,int>,boost::hash<pair<int,int>>> parent2child_qid;
    };

public:
    string ToJson(shared_ptr<FinalNode> root) ;

    string ToSchemaFile(shared_ptr<FinalNode> root);

};
