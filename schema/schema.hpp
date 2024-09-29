#ifndef SCHEMA_HPP
#define SCHEMA_HPP
#include <map>
#include <iostream>
#include <fstream>
#include "../lib/json.hpp"

using json = nlohmann::json;
using namespace std;

ostream& operator<<(ostream& os, vector<string>& v) {
    for (auto& elem : v) {
        os << elem << ' ' << endl;
    }
    return os;
}

ostream& operator<<(ostream& os, map<string, vector<string>>& m) {
    for (auto& pair : m) {
        os << pair.first << ": " << endl;
        os << pair.second << endl;
    }
    return os;
}

struct Schema {
    string name;
    int tuplesLimit;
    map<string, vector<string>> structure;
};

Schema readSchema(string filename) {
    Schema* sc = new Schema();
    string name;
    int tuplesLimit;
    map<string, vector<string>> structure;

    ifstream file(filename);

    json jsonSchema;
    file>>jsonSchema;

    sc->name = jsonSchema["name"];
    sc->tuplesLimit = jsonSchema["tuples_limit"];
    sc->structure = jsonSchema["structure"];

    return *sc;
}

#endif