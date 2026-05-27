#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <cassert>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/functional/hash.hpp>
using namespace std;

extern unordered_map<int,int> input_rl_id2rl_id;
bool debug = false;

vector<int> Intersection(const vector<int>& v1, const vector<int>& v2)
{
    vector<int> intersection;
    for (int i : v1) {
        if (find(v2.begin(), v2.end(), i) != v2.end()) {
            intersection.push_back(i);
        }
    }
    return intersection;
}

bool SubseteqCheck(const vector<int>& left, const vector<int>& right)
{

    for(int i: left)
    {
        if(find(right.begin(), right.end(), i) == right.end())
            return false;
    }
    return true;
}

bool SubseteqCheck(const unordered_set<int>& left, const unordered_set<int>& right)
{
    for(int i: left)
    {
        if(right.find(i) == right.end())
            return false;
    }
    return true;
}

unordered_set<int> VectorToUnorderedSet(const vector<int>& vec)
{
    unordered_set<int> uset;
    for(int i: vec)
    {
        uset.insert(i);
    }
    return uset;
}

vector<int> Minus(const vector<int>& v1, const vector<int>& v2)
{
    vector<int> result;
    for(int i: v1)
    {
        if(find(v2.begin(), v2.end(), i) == v2.end())
            result.push_back(i);
    }
    return result;
}

vector<string> convert_int_vector_to_string(const vector<int>& int_vec) {
    vector<string> string_vec;
    for (const auto& num : int_vec) {
        string_vec.push_back(to_string(num));
    }
    return string_vec;
}

string to_json_array(const vector<int>& attrs) {
    stringstream ss;
    ss << "[";
    for (size_t i = 0; i < attrs.size(); ++i) {
        ss << "\"" << attrs.at(i) << "\"";
        if (i < attrs.size() - 1) ss << ", ";
    }
    ss << "]";
    return ss.str();
}

string to_json_array(const vector<string>& attrs) {
    stringstream ss;
    ss << "[";
    for (size_t i = 0; i < attrs.size(); ++i) {
        ss << "\"" << attrs.at(i) << "\"";
        if (i < attrs.size() - 1) ss << ", ";
    }
    ss << "]";
    return ss.str();
}

vector<string> ReadLabelNameFromFile(const string& filename)
{
    vector<string> label2name;
    label2name.resize(input_rl_id2rl_id.size());

    ifstream fin(filename);
    string name;
    int label_id;
    while(fin>>label_id>>name)
    {
        if(input_rl_id2rl_id.count(label_id) == 0)
        {
            continue;
        }
        label_id = input_rl_id2rl_id[label_id];
        label2name[label_id] = name;
    }
    fin.close();
    return label2name;
}

string to_json_query_id_array(const vector<pair<int, int>>& relevant_qs) {
    vector<string> query_id_objects;
    for (const auto& qs_pair : relevant_qs) {
        query_id_objects.push_back(
            "{\"qid\": " + to_string(qs_pair.first) +
            ", \"place_id\": " + to_string(qs_pair.second) + "}"
        );
    }
    return "[" + boost::algorithm::join(query_id_objects, ", ") + "]";
}

std::string join_strings(const std::vector<std::string>& strings, const std::string& delimiter) {
    std::string result;
    for (size_t i = 0; i < strings.size(); ++i) {
        if (i > 0) {
            result += delimiter;
        }
        result += strings[i];
    }
    return result;
}

std::string to_json_parent2child_qids(const std::unordered_map<std::pair<int,int>, std::pair<int,int>, boost::hash<std::pair<int,int>>>& parent2child_qid) {
    std::vector<std::string> entries;
    for (const auto& entry : parent2child_qid) {
        const auto& parent_qid = entry.first;
        const auto& child_qid = entry.second;

        std::string parent_str = "{ \"qid\": " + std::to_string(parent_qid.first) +
                               ", \"place_id\": " + std::to_string(parent_qid.second) + " }";

        std::string child_str = "{ \"qid\": " + std::to_string(child_qid.first) +
                              ", \"place_id\": " + std::to_string(child_qid.second) + " }";

        entries.push_back("[ " + parent_str + ", " + child_str + " ]");
    }

    return "[ " + join_strings(entries, ", ") + " ]";
}
