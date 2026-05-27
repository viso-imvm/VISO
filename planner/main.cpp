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
#include <chrono>
using namespace std;

vector<RelationCopy> relation_copies;
vector<QueryInfo> query_infos;
vector<shared_ptr<Query>> queries;
SubqManager subq_manager;
vector<double> rel_label2upd_freq_est;
vector<vector<FinalNode>> rel_label2aod;
shared_ptr<FinalNode> dummy_root;

unordered_map<int,int> input_rc_id2rc_id;
unordered_map<int,int> input_ac_id2ac_id;
unordered_map<int,int> input_rl_id2rl_id;
string prefix_path = "./";
string project_root = "./";
bool single_query_mode = false;

int stored_plan_num = 0;

void optimize(const vector<shared_ptr<Query>>& all_qs, vector<shared_ptr<Query>>& user_qs,shared_ptr<FinalNode> dummy_root);
void SetOutputAttrSQ(shared_ptr<FinalNode> root, int query_id);
void SetOutputAttrMQO(shared_ptr<FinalNode> root, int query_id);
void ReadQueryFromFile(const string& filename);
void ToDotFormat(shared_ptr<FinalNode> root,ostream& out=cout) ;

int main(int argc, char* argv[]) {

    string input_name = "workload/input.txt";
    string output_dir = "output/";
    string benchmark_type = "SNB";
    int input_qid = -1;
    bool custom_input = false;

    for(int i = 1; i < argc; i++)
    {
        string arg(argv[i]);
        if(arg == "--input" && i + 1 < argc)
        {
            input_name = argv[++i];
            custom_input = true;
            single_query_mode = false;
        }
        else if(arg == "--output-dir" && i + 1 < argc)
        {
            output_dir = argv[++i];
            if(output_dir.back() != '/') output_dir += "/";
        }
        else if(arg == "JOB" || arg == "SNB")
        {
            benchmark_type = arg;
            if(i + 1 < argc && argv[i+1][0] != '-')
                input_qid = atoi(argv[++i]);
        }
        else if(arg[0] != '-' && !custom_input)
        {

            input_qid = atoi(arg.c_str());
        }
    }

    prefix_path = output_dir;

    {
        string out = output_dir;
        size_t pos = out.find("output/");
        if (pos != string::npos) {
            project_root = out.substr(0, pos);
        } else {
            project_root = "./";
        }
        if (project_root.empty()) project_root = "./";
    }

    if(input_qid != -1)
    {
        string folder = (benchmark_type == "JOB") ? "separate_q_JOB" : "separate_q";
        input_name = "workload/" + folder + "/query_"+to_string(input_qid)+".txt";
        cout << "Running " << benchmark_type << " query " << input_qid << " from: " << input_name << endl;
        single_query_mode = true;
    }

    ReadQueryFromFile(input_name);

    if(custom_input)
    {
        if((int)query_infos.size() <= 1)
            single_query_mode = true;
        else
            single_query_mode = false;
        cout << "[Auto] mode=" << (single_query_mode ? "single" : "multi")
             << " (" << query_infos.size() << " queries)\n";

        string mkdir_cmd = "mkdir -p " + prefix_path;
        system(mkdir_cmd.c_str());
    }

    extern vector<string> label2name;
    {
        string input_lower = input_name;
        transform(input_lower.begin(), input_lower.end(), input_lower.begin(), ::tolower);
        string label_name_file;
        bool is_job_path = (input_lower.find("separate_q_job") != string::npos ||
                            input_lower.find("/job/") != string::npos);
        bool is_snb_path = (input_lower.find("separate_q_mutated") != string::npos ||
                            input_lower.find("separate_q/") != string::npos ||
                            input_lower.find("/snb/") != string::npos);
        if (is_job_path && !is_snb_path) {
            label_name_file = project_root + "workload/label_name.txt";
        } else if (is_snb_path) {
            label_name_file = project_root + "workload/label_name_snb_gq.txt";
        } else {

            label_name_file = project_root + "workload/label_name.txt";
        }
        label2name = ReadLabelNameFromFile(label_name_file);
        cout << "[LabelName] Loaded from: " << label_name_file << " (" << label2name.size() << " entries)" << endl;
    }

    try {

    auto t_plan_start = chrono::high_resolution_clock::now();

    int idx = 0;
    for(auto& qinfo: query_infos)
    {
        if(!qinfo.FreeConnexCheck())
        {
            cout << "query "<<idx <<" is not free-connex, skip it. "<<endl;
            idx++;
            continue;
        }

        auto q = make_shared<Query>(qinfo);
        queries.emplace_back(q);
        queries.back()->GenSubqSpace();

        idx++;
    }
    cout<<subq_manager.Size()<<" subqueries in total."<<endl;

    int qid = 0;
    for(auto& q: queries)
    {
        q->SpreadQid(qid);
        qid++;
    }

    subq_manager.GenPlanSpace4All();

    for(auto& q: queries)
    {
        for(auto& subq: q->subqueries)
        {

            q->AddPlans(subq->plans);

        }

    }

    for(auto& q: queries)
    {
        stored_plan_num = 0;
        q->CalcAllPlanNum();

        cout<<"query with rels: ";
        for(auto r: q->info.rel_copy_set) cout<<r<<" ";
        cout<<" has "<<q->plan_num<<" plans."<<endl;
        cout<<"stored plan num (not counted by previous queries): "<<stored_plan_num<<endl;
    }

    auto all_qs = subq_manager.GetAllSubqs();
    cout<<"all_qs.size(): "<<all_qs.size()<<endl;
    auto t_plan_space_end = chrono::high_resolution_clock::now();
    double plan_space_ms = chrono::duration<double, milli>(t_plan_space_end - t_plan_start).count();
    cout << "[Time] Plan space generation: " << plan_space_ms << " ms" << endl;

    dummy_root = make_shared<FinalNode>();
    auto t_opt_start = chrono::high_resolution_clock::now();
    try { optimize(all_qs,queries,dummy_root); }
    catch(const exception& e) { cerr << "[ERROR] optimize() failed: " << e.what() << endl; throw; }
    auto t_opt_end = chrono::high_resolution_clock::now();
    double opt_ms = chrono::duration<double, milli>(t_opt_end - t_opt_start).count();
    cout << "[Time] Optimization: " << opt_ms << " ms" << endl;

    try {
    if(!single_query_mode)
    {
        int child_idx = 0;
        for(auto& q: queries)
        {
            int dummy_parent_idx = dummy_root->parent_idxs[child_idx];
            q->aod->SetInConnex(dummy_parent_idx, q->info.y);

            child_idx++;
        }

    }

    qid = 0;
    for(auto c: dummy_root->children)
    {
        if(single_query_mode)
            SetOutputAttrSQ(c, qid);
        else SetOutputAttrMQO(c, qid);
        qid++;
    }
    } catch(const exception& e) { cerr << "[ERROR] SetOutputAttr/SetInConnex failed: " << e.what() << endl; throw; }

    string dot_name = (custom_input) ? (prefix_path + "aod.dot") : (prefix_path + "output/aod.dot");
    ofstream fout(dot_name);

    string json_str;
    try {
    if(single_query_mode)
    {
        SingleQueryOutput sqo;
        json_str = sqo.ToJson(dummy_root);

        string schema_str = sqo.ToSchemaFile();
        string schema_name;
        if(custom_input)
            schema_name = prefix_path + "single_query_schema.txt";
        else {
            schema_name = "output/single_query_schema.txt";
            if (input_qid != -1)
                schema_name = "output/query_schema_" + to_string(input_qid) + ".txt";
            schema_name = prefix_path + schema_name;
        }
        ofstream schema_out(schema_name);

        schema_out.close();

    }
    else
    {
        MultiQueryOutput mqo;
        json_str = mqo.ToJson(dummy_root);

        string schema_str = mqo.ToSchemaFile(dummy_root);
        string schema_name;
        if(custom_input)
            schema_name = prefix_path + "multi_query_schema.txt";
        else {
            schema_name = "output/multi_query_schema.txt";
            if (input_qid != -1)
                schema_name = "output/query_schema_" + to_string(input_qid) + ".txt";
            schema_name = prefix_path + schema_name;
        }
        ofstream schema_out(schema_name);

        schema_out.close();

    }
    } catch(const exception& e) { cerr << "[ERROR] ToJson/ToSchemaFile failed: " << e.what() << endl; throw; }

    ofstream json_out;
    string output_name;

    if(custom_input)
    {

        if(single_query_mode)
            output_name = prefix_path + "single_query_plan.json";
        else
            output_name = prefix_path + "multi_query_plan.json";
    }
    else
    {

        output_name = "output/";
        if(single_query_mode)
            output_name += "single_";
        else
            output_name += "multi_";

        output_name += "query_plan.json";
        if(input_qid != -1)
        {
            output_name = "output/query_plan_" + to_string(input_qid) + ".json";
        }
        output_name = prefix_path + output_name;
    }

    json_out.open(output_name);

    json_out.close();

    auto t_plan_end = chrono::high_resolution_clock::now();
    double total_plan_ms = chrono::duration<double, milli>(t_plan_end - t_plan_start).count();
    cout << "[Time] Total plan generation: " << total_plan_ms << " ms" << endl;

    } catch(const exception& e) {
        cerr << "[ERROR] " << e.what() << endl;
        cerr << "[ERROR] Failed to generate plan for input: " << input_name << endl;
        return 1;
    }

    return 0;

}
