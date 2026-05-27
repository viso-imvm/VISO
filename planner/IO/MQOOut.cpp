#include <vector>
#include <string>
#include <sstream>
#include <unordered_map>
#include <memory>
#include "Query.h"
#include "FinalNode.h"
#include <queue>
#include <fstream>
#include "MQOOut.h"

using namespace std;

vector<string> label2name;

extern string prefix_path;

string MultiQueryOutput::ToSchemaFile(shared_ptr<FinalNode> root) {

    stringstream ss;
    int qid = 0;
    ss << root->children.size()<<endl;

    for(auto q_root: root->children)
    {
        ss<<qid<<endl;
        for(auto out_attr: q_root->y)
        {
            auto [name,place_id,attr_idx] = q_root->FindAttrInConnex(-1, qid, 0, out_attr);
            assert(name != "");
            ss << name<<"."<<place_id<<","<<attr_idx<<endl;

        }

        qid++;
    }

    return ss.str();
}

string MultiQueryOutput::ToJson(shared_ptr<FinalNode> root) {

    vector<string> relations_json;
    vector<ConnectionConfig> connections;
    vector<string> connections_json;
    unordered_map<string, vector<RelationConfig>> relation_configs;

    unordered_set<shared_ptr<FinalNode>> visited;
    queue<shared_ptr<FinalNode>> q;
    q.push(root);

    while (!q.empty()) {
        auto node = q.front(); q.pop();
        if (visited.count(node)) continue;
        visited.insert(node);

        RelationConfig rc;

        rc.name = node->GetName();

        if(node->root.CheckNotDecide())
            rc.rel_type = "dummy_root";
        else if(node->root.IsGeneralized())
            rc.rel_type = "generalized";
        else
            rc.rel_type = (node->children.empty()) ? "leaf" : "middle";

        rc.num_children = (node->children.size());

        if(rc.rel_type != "dummy_root")
        {

            rc.output_attr = node->GetRootOutputAttrs();

            relation_configs[rc.name].push_back(rc);
        }

        int idx = 0;
        for (auto& child : node->children) {
            string root_str;
            string child_str;

            ConnectionConfig cc;

            cc.from_rel = rc.name;
            cc.to_rel = child->GetName();

            auto common_attrs = node->common_attr_children.at(idx);
            for(auto [p_idx, c_idx]: common_attrs.attrs)
            {
                cc.join_keys_parent.emplace_back((p_idx));
                cc.join_keys_child.emplace_back((c_idx));
            }

            if(cc.join_keys_parent.empty())
            {
                cc.join_keys_child = child->GetRootOutputAttrs();
            }

            int parent_idx = node->parent_idxs[idx];
            cc.relevant_qs = child->relevant_qids[parent_idx].q_place_ids;

            cc.child_in_connex = child->in_connex[parent_idx];

            for(const auto& [qid,place_id_num]: node->q2place_num)
            {
                int place_id = 0;

                for(const auto& [cqid, cplace_id]: cc.relevant_qs)
                {
                    if(cqid == qid)
                    {
                        node->qid_maps[idx].parent2child_qid[{qid, place_id}] = {qid, cplace_id};
                        place_id ++;
                    }
                }
                if(place_id != place_id_num)
                {
                    cout<<"Error in ToJson: place_id_num mismatch. "<<endl;
                    cout<<"  qid: "<<qid<<", place_id_num: "<<place_id_num<<", found: "<<place_id<<endl;
                    cout<<node->GetName()<<" to "<<child->GetName()<<endl;
                    assert(0);
                }
            }

            cc.parent2child_qid = node->qid_maps[idx].parent2child_qid;
            connections.push_back(cc);

            q.push(child);

            idx++;
        }
    }

    for(auto& cc: connections)
    {

        connections_json.push_back(
            "    { \"fromRelation\": \"" + cc.from_rel + "\", "
            "\"toRelation\": \"" + cc.to_rel + "\", "
            "\"joinKeys\": " + to_json_array(cc.join_keys_parent) + ", "
            "\"child_upward_join_keys\": " + to_json_array(cc.join_keys_child) + ", "
            "\"related_qs\": " + to_json_query_id_array(cc.relevant_qs) + ", "
            "\"child_in_connex\": " + (cc.child_in_connex ? "true" : "false") + ", "
            "\"parent2child_qids\": " + to_json_parent2child_qids(cc.parent2child_qid) + " }"
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
                    "\"numChildren\": " + to_string(rc.num_children) + ", "
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
