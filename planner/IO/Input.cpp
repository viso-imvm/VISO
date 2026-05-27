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

extern unordered_map<int,int> input_rc_id2rc_id;
extern unordered_map<int,int> input_ac_id2ac_id;
extern unordered_map<int,int> input_rl_id2rl_id;

extern vector<RelationCopy> relation_copies;
extern vector<QueryInfo> query_infos;
extern vector<Query> queries;
extern SubqManager subq_manager;
extern vector<double> rel_label2upd_freq_est;
extern vector<vector<FinalNode>> rel_label2aod;
extern shared_ptr<FinalNode> dummy_root;

void TransInputIds(int& input_id, string type)
{
    unordered_map<int,int>& input_map = (type == "rc" ? input_rc_id2rc_id : (type == "ac" ? input_ac_id2ac_id : input_rl_id2rl_id));
    int ori_id = input_id;
    if(!input_map.count(input_id))
    {
        int new_id = input_map.size();
        input_map[input_id] = new_id;
    }
    input_id = input_map[input_id];
    if(debug) cout<<ori_id<<' '<<type<<" mapped to "<<input_id<<endl;
}

void ReadQueryFromFile(const string& filename) {
    ifstream fin(filename);

    cout<<"input file name: "<<filename<<endl;
    int query_num;
    fin >> query_num;
    for (int q = 0; q < query_num; q++) {
        QueryInfo qinfo;
        int rel_num;
        fin >> rel_num;
        assert(rel_num<20);
        qinfo.rel_copy_set.resize(rel_num);
        for (int i = 0; i < rel_num; i++) {
            int rel_copy_id, label_id, attr_num;
            fin >> rel_copy_id >> label_id >> attr_num;
            TransInputIds(rel_copy_id, "rc");
            TransInputIds(label_id, "rl");

            if(rel_label2upd_freq_est.size() <= label_id)
                rel_label2upd_freq_est.resize(label_id + 1, 1.0);
            qinfo.rel_copy_set[i] = rel_copy_id;
            vector<int> attrs(attr_num);
            for (int j = 0; j < attr_num; j++) {
                fin >> attrs[j];
                TransInputIds(attrs[j], "ac");
            }

            if (rel_copy_id >= relation_copies.size()) {
                relation_copies.resize(rel_copy_id + 1);
            }
            relation_copies[rel_copy_id] = {label_id, attrs};
            if(debug)
            {
                cout<<"read relation copy id "<<rel_copy_id<<" with label "<<label_id<<", attrs: ";
                for(auto a: attrs) cout<<a<<" ";
                cout<<endl;
            }

        }
        int output_attr_num;
        fin >> output_attr_num;
        qinfo.y.resize(output_attr_num);
        for (int i = 0; i < output_attr_num; i++) {
            fin >> qinfo.y[i];
            TransInputIds(qinfo.y[i], "ac");
        }
        sort(qinfo.y.begin(), qinfo.y.end());
        sort(qinfo.rel_copy_set.begin(), qinfo.rel_copy_set.end());

        query_infos.push_back(qinfo);
    }
    fin.close();
    cout << "read " << query_infos.size() << " queries" << endl;

}

void ToDotFormat(shared_ptr<FinalNode> root,ostream& out=cout) {
    unordered_set<shared_ptr<FinalNode>> visited;
    queue<shared_ptr<FinalNode>> q;
    q.push(root);

    out << "digraph G {" << endl;

    while (!q.empty()) {
        auto node = q.front(); q.pop();
        if (visited.count(node)) continue;
        visited.insert(node);
        int idx = 0;
        for (auto& child : node->children) {
            string root_str;
            string child_str;

            root_str += "Q[";
            for(auto qid: node->GetRelevantQueries()) root_str += to_string(qid)+",";
            if(!node->GetRelevantQueries().empty()) root_str.pop_back();
            root_str += "] ";

            root_str += "y: ";
            for(auto attr: node->y)
            {
                root_str += to_string(attr)+" ";
            }
            root_str += ". ";

            child_str += "Q[";
            for(auto& qid: child->GetRelevantQueries()) child_str += to_string(qid)+",";
            if(!child->GetRelevantQueries().empty()) child_str.pop_back();
            child_str += "] ";
            child_str += "y: ";
            for(auto attr: child->y)
            {
                child_str += to_string(attr)+" ";
            }
            child_str += ". ";

            stringstream ss;
            node->root.PrintWithLabel(ss);
            root_str += ss.str();

            ss.str(""); ss.clear();
            child->root.PrintWithLabel(ss);
            child_str += ss.str();

            string edge_label;
            edge_label = node->common_attr_children[idx].ToString();

            out << "  \"" << root_str << "\" -> \"" << child_str << "\"";
            if (!edge_label.empty()) {
                out << " [label=\"" << edge_label << "\"]";
            }
            out << ";" << endl;

            q.push(child);

            idx++;
        }
    }
    out << "}" << endl;
}
