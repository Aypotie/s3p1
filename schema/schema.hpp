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
    auto structure = jsonSchema["structure"]; // разные типы
    for (auto it = structure.begin(); it != structure.end(); it++) {
        Vector<string> columns;
        for (string column : it.value()) { 
            columns.pushBack(column);
        }
        sc->structure.put(it.key(), columns); 
    }

    return *sc;
}

#endif