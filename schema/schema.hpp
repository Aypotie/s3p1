#ifndef SCHEMA_HPP
#define SCHEMA_HPP
#include <map>
#include <iostream>
#include <fstream>
#include "../lib/json.hpp"
#include "../data_structures/map.hpp"
#include "../data_structures/vector.hpp"

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
    Map<Vector<string>> structure;
};

Schema readSchema(string filename) {
    Schema* sc = new Schema();

    ifstream file(filename);

    json jsonSchema;
    file>>jsonSchema;

    sc->name = jsonSchema["name"];
    sc->tuplesLimit = jsonSchema["tuples_limit"];
    auto structure = jsonSchema["structure"];
    for (auto it = structure.begin(); it != structure.end(); ++it) {
        Vector<string> columns;
        for (string column : it.value()) { // Доступ к значению правильно
            columns.pushBack(column);
        }
        sc->structure.put(it.key(), columns); // Доступ к ключу правильно
    }

    return *sc;
}

#endif