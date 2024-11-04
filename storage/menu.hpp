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
#include <sys/socket.h>

using namespace std;

// Создаем регулярные выражения с флагом игнорирования регистра
regex insertRegex("^INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s+\\((.+)\\)\\s*;?$", regex_constants::icase);
regex selectRegex("^SELECT\\s+([\\w\\d\\.,\\s]+)\\s+FROM\\s+([\\w\\d,\\s]+)$", regex_constants::icase); // без where
regex deleteRegex("^DELETE\\s+FROM\\s+([\\w\\d,\\s]+)\\s*;?$", regex_constants::icase); // без where
regex deleteWhereRegex("^DELETE\\s+FROM\\s+([\\w\\d,\\s]+)\\s*WHERE\\s+(.+?)?\\s*;?$", regex_constants::icase);
regex selectWhereRegex("^SELECT\\s+([\\w\\d\\.,\\s]+)\\s+FROM\\s+([\\w\\d,\\s]+)\\s+\\s*WHERE\\s+(.+?)?\\s*;?$", regex_constants::icase);

void menu(int clientSocket, string command, Storage* storage) {
    smatch match;
    if (regex_match(command, match, insertRegex)) {
        string tableName = match[1].str();
        string valuesStr = match[2].str();

        if (!storage->schema.structure.contains(tableName)) {
            string output = "Table was not found\n";
            send(clientSocket, output.c_str(), output.length(), 0);
            return;
        }

        Vector<string> cols = storage->schema.structure.get(tableName);

        // Парсим значения из VALUES(...)
        Vector<string> values = split(valuesStr, ",");

        if (values.size() != cols.size()) {
            string output = "Incorrect count of columns\n";
            send(clientSocket, output.c_str(), output.length(), 0);
            return;
        }

        // Убираем первые и последние кавычки
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i).front() == '\'' && values.get(i).back() == '\'') {
                values.set(i, trim(values.get(i), '\''));
            } else {
                string output = "Error: All values must be enclosed in quotes.\n";
                send(clientSocket, output.c_str(), output.length(), 0);
                return;
            }
        }

        storage->insert(storage->schema.name, tableName, values);
    } else if (regex_match(command, match, selectRegex)) {
        string columnsStr = match[1].str();
        string tablesStr = match[2].str();

        Vector<string> tables = split(tablesStr, ",");
        Vector<string> columns = split(columnsStr, ",");

        // Проверка существования таблиц
        for (int i = 0; i < tables.size(); i++) {
            string tableName = tables.get(i);
            if (!storage->schema.structure.contains(tableName)) {
                string output = "Table '" + tableName + "' does not exist.\n";
                send(clientSocket, output.c_str(), output.size(), 0);
                return;
            }
        }

        storage->select(clientSocket, columns, tables, "");
    } else if (regex_match(command, match, selectWhereRegex)) {
        string columnsStr = match[1].str();
        string tablesStr = match[2].str();
        string condition = match[3].str();
        Vector<string> tables = split(tablesStr, ",");
        Vector<string> columns = split(columnsStr, ",");

        storage->select(clientSocket, columns, tables, condition);
    } else if (regex_match(command, match, deleteRegex)) {
        string tablesStr = match[1].str();
        Vector<string> tables = split(tablesStr, ",");

        storage->fDelete(tables, "");
    } else if (regex_match(command, match, deleteWhereRegex)) {
        string tablesStr = match[1].str();
        string condition = match[2].str();
        Vector<string> tables = split(tablesStr, ",");

        storage->fDelete(tables, condition);
    } else {
        string output = "Unknown command\n";
        send(clientSocket, output.c_str(), output.length(), 0);
        return;
    }
    string output = "Command executed successfully\n";
    send(clientSocket, output.c_str(), output.length(), 0);
}

#endif
