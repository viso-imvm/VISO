#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <memory>
#include "Query.h"
#include "FinalNode.h"
#include <queue>
#include <fstream>
#include "SingleOut.h"

using namespace std;
extern vector<RelationCopy> relation_copies;
extern unordered_map<int,int> input_rl_id2rl_id;
extern string prefix_path;
extern vector<vector<tuple<shared_ptr<FinalNode>,vector<int>,int>>> sq_output_schema;

string SingleQueryOutput::ToJson(shared_ptr<FinalNode> root) {

    extern vector<string> label2name;

    vector<shared_ptr<FinalNode>> nodes;
    vector<string> relations_json;
    vector<ConnectionConfig> connections;
    vector<string> connections_json;
    unordered_map<string, vector<RelationConfig>> relation_configs;
    vector<string> node_idx2name;

    queue<pair<int,vector<int>>> q;
    nodes.emplace_back(root);
    q.push({nodes.size()-1,{}});

    while (!q.empty()) {
        auto [nodes_idx, visit_path] = q.front(); q.pop();
        RelationConfig rc;

        rc.name = "node_" + to_string(nodes_idx);
        auto node = nodes.at(nodes_idx);
        rc.node_ptr = node;

        if(!node->root.IsGeneralized() && !node->root.CheckNotDecide())
        {

            int label_id = node->GetLabelId();
            rc.name = label2name[label_id];

        }

        if(node->root.CheckNotDecide())
            rc.rel_type = "dummy_root";
        else if(node->root.IsGeneralized())
            rc.rel_type = "generalized";
        else
            rc.rel_type = (node->children.empty()) ? "leaf" : "middle";

        auto key = vector<int>();
        if(!node->parents.empty())
        {
            int parent_idx = 0;
            if(node->parents.size() != 1)
            {
                int pid = visit_path[0];
                for(size_t i = 0; i < node->parents.size(); ++i)
                {
                    if(node->parents.at(i)->GetId() == pid)
                    {
                        parent_idx = i;
                        break;
                    }
                }
            }
            for(auto [p_idx, c_idx]: node->common_attr_parents.at(parent_idx).attrs)
            {
                rc.keys.emplace_back(to_string(c_idx));
            }
            rc.num_children = (node->children.size());

            key.emplace_back(node->relevant_qids.at(0).q_place_ids.at(0).first);
            key.insert(key.end(), visit_path.begin(), visit_path.end());

            auto [b,v] = node->output_infos.at(key);
            rc.in_connex_subtree = b;
            rc.output_attr = convert_int_vector_to_string(v);
            rc.output_info_key = key;
        }

        if(rc.keys.empty() && rc.name!= "node_0")
        {
            int attr_size = node->root.GetAllAttrs().size();
            for(int i = 0;i<attr_size;++i)
            {
                rc.keys.push_back(to_string(i));
            }
        }

        if(relation_configs.count(rc.name) == 0)
        {
            vector<RelationConfig> temp;
            temp.push_back(rc);
            relation_configs.insert({rc.name,temp});

        }
        else
        {
            auto& same_label_nodes = relation_configs[rc.name];
            if(same_label_nodes.size() == 1)
            {
                rc.name += to_string(same_label_nodes.size()+1);
                same_label_nodes.push_back(rc);
                same_label_nodes[0].name += "1";
                same_label_nodes[0].node_ptr->key2rc_name[same_label_nodes[0].output_info_key] = same_label_nodes[0].name;
            }
            else
            {
                rc.name += to_string(same_label_nodes.size()+1);
                same_label_nodes.push_back(rc);
            }
        }
        rc.node_ptr->key2rc_name.insert({key, rc.name});

        if(node_idx2name.size() <= nodes_idx)
            node_idx2name.resize(nodes_idx+1);
        node_idx2name[nodes_idx] = rc.name;

        for (size_t i = 0; i < node->children.size(); ++i) {
            auto& child = node->children.at(i);
            nodes.emplace_back(child);

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

            q.push({nodes.size()-1,child_visit_path});

            ConnectionConfig cc;
            cc.from_rel = rc.name;
            cc.to_rel = "node_" + to_string(nodes.size()-1);

            auto common_attrs = node->common_attr_children.at(i);
            for(auto [p_idx, c_idx]: common_attrs.attrs)
            {
                cc.join_keys_parent.emplace_back(to_string(p_idx));
            }

            connections.push_back(cc);

        }
    }

    for(auto& cc: connections)
    {
        int to_rel_node_idx = stoi(cc.to_rel.substr(5));
        auto to_node = nodes[to_rel_node_idx];
        if(to_node->root.IsGeneralized())
        {

        }
        else
        {
            cc.to_rel = node_idx2name[to_rel_node_idx];
        }

        if(cc.from_rel.back()>='0' && cc.from_rel.back()<='9')
        {

        }
        else
        {
            cc.from_rel = relation_configs.at(cc.from_rel).at(0).name;
        }

        if(cc.to_rel.back()>='0' && cc.to_rel.back()<='9')
        {

        }
        else
        {
            cc.to_rel = relation_configs.at(cc.to_rel).at(0).name;
        }

        if(cc.from_rel!="node_0")
            connections_json.push_back(
                "    { \"fromRelation\": \"" + cc.from_rel + "\", "
                "\"toRelation\": \"" + cc.to_rel + "\", "
                "\"joinKeys\": " + to_json_array(cc.join_keys_parent) + " }"
            );
    }

    for (auto& [name, rc_vec] : relation_configs) {
        for(auto& rc: rc_vec)
        {
            if(rc.rel_type != "dummy_root")
            {

                relations_json.push_back(
                    "    { \"name\": \"" + rc.name + "\", "
                    "\"relationType\": \"" + rc.rel_type + "\", "
                    "\"keys\": " + to_json_array(rc.keys) + ", "
                    "\"numChildren\": " + to_string(rc.num_children) + ", "
                    "\"in_connex_subtree\": " + (rc.in_connex_subtree ? "true" : "false") + ", "
                    "\"output_attr\": " + to_json_array(rc.output_attr) + " }"
                );
            }

        }

    }

    stringstream json;
    json << "{\n";
    json << "  \"relations\": [\n";
    for (size_t i = 0; i < relations_json.size(); ++i) {
        json << relations_json.at(i);
        if (i < relations_json.size() - 1) json << ",";
        json << "\n";
    }
    json << "  ],\n";
    json << "  \"connections\": [\n";
    for (size_t i = 0; i < connections_json.size(); ++i) {
        json << connections_json.at(i);
        if (i < connections_json.size() - 1) json << ",";
        json << "\n";
    }
    json << "  ]\n";
    json << "}";

    return json.str();
}

string SingleQueryOutput::ToSchemaFile()
{
    string ans;
    ans += to_string(sq_output_schema.size()) + "\n";
    int idx = 0;
    for(auto& v: sq_output_schema)
    {
        ans += to_string(idx) + "\n";

        for(auto& [node_ptr, key, attr_idx]: v)
        {
            string name = node_ptr->key2rc_name.at(key);

            ans += name + "," + to_string(attr_idx) + "\n";
        }

        idx++;
    }
    return ans;
}
