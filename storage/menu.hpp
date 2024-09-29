#ifndef MENU_H
#define MENU_H

#include <iostream>
#include <regex>
#include <string>
#include "../schema/schema.hpp"
#include "./storage.hpp"
#include "./utils.hpp"

using namespace std;

// Создаем регулярные выражения с флагом игнорирования регистра
regex insertRegex("^INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s+\\((.+)\\)\\s*;?$", regex_constants::icase);
regex selectRegex("^SELECT\\s+([\\w\\d\\.,\\s]+)\\s+FROM\\s+([\\w\\d,\\s]+)$", regex_constants::icase); // без where
regex deleteRegex("^DELETE\\s+FROM\\s+([\\w\\d,\\s]+)\\s*;?$", regex_constants::icase); // без where
regex deleteWhereRegex("^DELETE\\s+FROM\\s+([\\w\\d,\\s]+)\\s*WHERE\\s+(.+?)?\\s*;?$", regex_constants::icase);
regex selectWhereRegex("^SELECT\\s+([\\w\\d\\.,\\s]+)\\s+FROM\\s+([\\w\\d,\\s]+)\\s+\\s*WHERE\\s+(.+?)?\\s*;?$", regex_constants::icase);

void menu(string command, Storage storage) {
    // Проверяем команду INSERT INTO
    smatch match;
    if (regex_match(command, match, insertRegex)) {
        string tableName = match[1].str();
        string valuesStr = match[2].str();

        if (storage.schema.structure.find(tableName) == storage.schema.structure.end()) {
            cerr << "Table was not found" << endl;
            return;
        }

        vector<string> cols = storage.schema.structure[tableName];

        // Парсим значения из VALUES(...)
        vector<string> values = split(valuesStr, ",");

        if (values.size() != cols.size()) {
            cerr << "Incorrect count of columns" << endl;
            return;
        }

        // Убираем первые и последние кавычки
        for (auto& value : values) {
            if (value.front() == '\'' && value.back() == '\'') {
                value = trim(value, '\'');
            } else {
                cerr << "Error: All values must be enclosed in quotes." << endl;
                return;
            }
        }

        // Вставляем данные в таблицу
        storage.insert(storage.schema.name, tableName, values);
    } else if (regex_match(command, match, selectRegex)) {
        string columnsStr = match[1].str();
        string tablesStr = match[2].str();
        vector<string> tables = split(tablesStr, ",");
        vector<string> columns = split(columnsStr, ",");

        storage.select(columns, tables, "");
        
    } else if (regex_match(command, match, selectWhereRegex)) {
        string columnsStr = match[1].str();
        string tablesStr = match[2].str();
        string condition = match[3].str();
        vector<string> tables = split(tablesStr, ",");
        vector<string> columns = split(columnsStr, ",");

        storage.select(columns, tables, condition);
    } else if (regex_match(command, match, deleteRegex)) {
        string tablesStr = match[1].str();
        vector<string> tables = split(tablesStr, ",");

        storage.fDelete(tables, "");
    } else if (regex_match(command, match, deleteWhereRegex)) {
        string tablesStr = match[1].str();
        string condition = match[2].str();
        vector<string> tables = split(tablesStr, ",");

        storage.fDelete(tables, condition);
    } else {
        cout << "Unknown command." << endl;
    }
}

#endif
