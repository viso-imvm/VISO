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

int global_plan_node_id = 0;
vector<int> label2node_num;
int generalized_node_num = 0;

vector<vector<tuple<shared_ptr<FinalNode>,vector<int>,int>>> sq_output_schema;

void SetOutputAttrSQ(shared_ptr<FinalNode> root, int query_id) {

    unordered_set<int> attrs_to_output;

    queue<tuple<shared_ptr<FinalNode>, vector<int>, unordered_map<int,int>>> q;
    unordered_map<int,int> corr_with_y_root;
    vector<int> root_output_attrs;

    unordered_map<int,int> y_attr2idx;
    for(size_t i = 0; i < root->y.size(); ++i)
    {
        y_attr2idx[root->y.at(i)] = i;
    }
    vector<tuple<shared_ptr<FinalNode>,vector<int>,int>> output_schema; output_schema.resize(root->y.size());

    vector<int> root_key;
    root_key.emplace_back(query_id);

    for(auto attr: root->y)
    {
        corr_with_y_root.insert({attr,attr});

        auto attr_idx = root->root.GetAttrIdx(attr);
        if(attr_idx!=-1)
        {
            root_output_attrs.emplace_back(attr_idx);
            output_schema.at(y_attr2idx[attr]) = {root,root_key,(attr_idx)};
        }
        else
        {
            attrs_to_output.insert(attr);
        }
    }

    root->output_infos.insert({root_key, {true, root_output_attrs}});

    q.push({root,{},corr_with_y_root});
    int idx = 0;
    while (!q.empty()) {

        auto [node, visit_path, corr_with_y] = q.front(); q.pop();
        auto node_root_attrs = node->root.GetAllAttrs();
        node->SetSameLabelId();

        for (size_t i = 0; i < node->children.size(); ++i) {
            auto& child = node->children.at(i);
            vector<int> child_visit_path;
            if(child->parents.size() > 1)
            {
                child_visit_path.push_back(node->GetId());
                child_visit_path.insert(child_visit_path.end(), visit_path.begin(), visit_path.end());
            }
            else
            {
                child_visit_path = visit_path;
            }

            auto c_corr_with_y = corr_with_y;
            for(auto [c_attr, p_attr]: node->child_corrs.at(i).attr_corr)
            {
                if(child->y.end() == find(child->y.begin(), child->y.end(), c_attr)) continue;
                c_corr_with_y.at(c_attr) = corr_with_y.at(p_attr);
            }

            q.push({child, child_visit_path,c_corr_with_y});

            idx++;

            vector<int> key;
            key.emplace_back(query_id);
            key.insert(key.end(), child_visit_path.begin(), child_visit_path.end());

            if(attrs_to_output.empty())
            {
                child->output_infos.insert({key, {false, {}}});
                continue;
            }

            bool in_connex_subtree = true;
            for(auto [p_idx, c_idx]: node->common_attr_children.at(i).attrs)
            {
                auto attr_in_key = node_root_attrs.at(p_idx);

                if(find(node->y.begin(), node->y.end(), attr_in_key) == node->y.end())
                {
                    in_connex_subtree = false;
                    child->output_infos.insert({key, {in_connex_subtree, {}}});
                    break;
                }
            }

            if(in_connex_subtree == false) continue;

            vector<int> output_attrs;
            for(auto [child_attr, y_attr]: c_corr_with_y)
            {
                auto attr_idx = child->root.GetAttrIdx(child_attr);
                if(attr_idx == -1) continue;
                if(attrs_to_output.count(y_attr))
                {
                    output_attrs.emplace_back(attr_idx);
                    attrs_to_output.erase(y_attr);

                    output_schema[y_attr2idx[y_attr]] = {child,key, attr_idx};
                }
            }
            child->output_infos.insert({key, {in_connex_subtree, output_attrs}});

        }
    }

    sq_output_schema.emplace_back(output_schema);

}

void SetOutputAttrMQO(shared_ptr<FinalNode> root, int query_id) {

    unordered_set<int> attrs_to_output;

    queue<shared_ptr<FinalNode>> q;

    q.push(root);
    int idx = 0;
    while (!q.empty()) {

        auto node = q.front(); q.pop();
        auto node_root_attrs = node->root.GetAllAttrs();
        node->SetSameLabelId();

        for (size_t i = 0; i < node->children.size(); ++i) {
            auto& child = node->children.at(i);

            q.push(child);

            idx++;

        }
    }

}
