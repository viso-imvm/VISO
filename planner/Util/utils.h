#pragma once

#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <cassert>
#include <unordered_map>
#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <cassert>
#include <unordered_map>
#include "QueryInfo.h"
#include <fstream>
#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/functional/hash.hpp>
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

extern bool debug;

vector<int> Intersection(const vector<int>& v1, const vector<int>& v2) ;
bool SubseteqCheck(const vector<int>& left, const vector<int>& right);
bool SubseteqCheck(const unordered_set<int>& left, const unordered_set<int>& right);
vector<int> Minus(const vector<int>& v1, const vector<int>& v2);
unordered_set<int> VectorToUnorderedSet(const vector<int>& vec);

vector<string> ReadLabelNameFromFile(const string& filename);
string to_json_array(const vector<string>& attrs);
string to_json_array(const vector<int>& attrs);
vector<string> convert_int_vector_to_string(const vector<int>& int_vec);
string to_json_query_id_array(const vector<pair<int, int>>& relevant_qs);
string to_json_parent2child_qids(const unordered_map<pair<int,int>, std::pair<int,int>, boost::hash<std::pair<int,int>>>& parent2child_qid) ;
