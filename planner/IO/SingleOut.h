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
extern vector<RelationCopy> relation_copies;
extern unordered_map<int,int> input_rl_id2rl_id;

class SingleQueryOutput
{
    struct RelationConfig{
        string name;
        string rel_type;
        vector<string> keys;
        int num_children = 1;
        bool in_connex_subtree = true;
        vector<string> output_attr;

        shared_ptr<FinalNode> node_ptr;
        vector<int> output_info_key;
    };
    struct ConnectionConfig{
        string from_rel;
        string to_rel;
        vector<string> join_keys_parent;
    };

public:

    string ToJson(shared_ptr<FinalNode> root);

    string ToSchemaFile();
};
