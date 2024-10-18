#ifndef MENU_H
#define MENU_H

#include <iostream>
#include <regex>
#include <string>
#include "../data_structures/vector.hpp"
#include "../schema/schema.hpp"
#include "./storage.hpp"
#include "./lock.hpp"
#include "./utils.hpp"

using namespace std;

// Создаем регулярные выражения с флагом игнорирования регистра
regex insertRegex("^INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s+\\((.+)\\)\\s*;?$", regex_constants::icase);
regex selectRegex("^SELECT\\s+([\\w\\d\\.,\\s]+)\\s+FROM\\s+([\\w\\d,\\s]+)$", regex_constants::icase); // без where
regex deleteRegex("^DELETE\\s+FROM\\s+([\\w\\d,\\s]+)\\s*;?$", regex_constants::icase); // без where
regex deleteWhereRegex("^DELETE\\s+FROM\\s+([\\w\\d,\\s]+)\\s*WHERE\\s+(.+?)?\\s*;?$", regex_constants::icase);
regex selectWhereRegex("^SELECT\\s+([\\w\\d\\.,\\s]+)\\s+FROM\\s+([\\w\\d,\\s]+)\\s+\\s*WHERE\\s+(.+?)?\\s*;?$", regex_constants::icase);

void menu(string command, Storage storage) {
    smatch match;
    if (regex_match(command, match, insertRegex)) {
        string tableName = match[1].str();
        string valuesStr = match[2].str();

        if (!storage.schema.structure.contains(tableName)) {
            cerr << "Table was not found" << endl;
            return;
        }

        Vector<string> cols = storage.schema.structure.get(tableName);

        // Парсим значения из VALUES(...)
        Vector<string> values = split(valuesStr, ",");

        if (values.size() != cols.size()) {
            cerr << "Incorrect count of columns" << endl;
            return;
        }

        // Убираем первые и последние кавычки
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i).front() == '\'' && values.get(i).back() == '\'') {
                values.set(i, trim(values.get(i), '\''));
            } else {
                cerr << "Error: All values must be enclosed in quotes." << endl;
                return;
            }
        }

        // Вставляем данные в таблицу
        string lockPath = storage.schema.name + "/" + tableName + "/" + tableName + "_lock";
        try {
            lock(lockPath);
        } catch (runtime_error& e) {
            cerr << "Table " + tableName + " blocked" << endl;
            return;
        }
        storage.insert(storage.schema.name, tableName, values);
        unlock(lockPath);
    } else if (regex_match(command, match, selectRegex)) {
        string columnsStr = match[1].str();
        string tablesStr = match[2].str();
        Vector<string> tables = split(tablesStr, ",");
        Vector<string> columns = split(columnsStr, ",");

        storage.select(columns, tables, "");
    } else if (regex_match(command, match, selectWhereRegex)) {
        string columnsStr = match[1].str();
        string tablesStr = match[2].str();
        string condition = match[3].str();
        Vector<string> tables = split(tablesStr, ",");
        Vector<string> columns = split(columnsStr, ",");

        storage.select(columns, tables, condition);
    } else if (regex_match(command, match, deleteRegex)) {
        string tablesStr = match[1].str();
        Vector<string> tables = split(tablesStr, ",");

        try {
            lockTables(storage.schema.name, tables);
        } catch (runtime_error& e) {
            cerr << e.what() << endl;
            return;
        }
        storage.fDelete(tables, "");
        unlockTables(storage.schema.name, tables);
    } else if (regex_match(command, match, deleteWhereRegex)) {
        string tablesStr = match[1].str();
        string condition = match[2].str();
        Vector<string> tables = split(tablesStr, ",");

        try {
            lockTables(storage.schema.name, tables);
        } catch (runtime_error& e) {
            cerr << e.what() << endl;
            return;
        }
        storage.fDelete(tables, condition);
        unlockTables(storage.schema.name, tables);
    } else {
        cout << "Unknown command." << endl;
    }
}

#endif
