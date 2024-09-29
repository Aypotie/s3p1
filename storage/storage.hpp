#ifndef STORAGE_H
#define STORAGE_H

#include <iostream>
#include <fstream>
#include <cstdio>
#include <filesystem>
#include <regex>
#include "../schema/schema.hpp"
#include "./file.hpp"
#include "./condition.hpp"

using namespace std;

namespace fs = filesystem;

struct Storage {
    Schema schema;

    Storage(Schema schema) : schema(schema) {
        this->schema = schema;
        createStructure();
    }

    void writeList(string path, string tableName, vector<string> cols) {
        ofstream file(path);

        file << tableName << "_pk,";
        for (int i = 0; i < cols.size(); i++) {
            if (i == cols.size() - 1) {
                file << cols[i];
            } else {
                file << cols[i] << ",";
            }
        }
        file << endl;

        file.close();
    }

    void createStructure() {
        if (!fs::exists(schema.name)) {
            fs::create_directory(schema.name);
        } else {
            cerr << "Storage directory " << schema.name << " already exists" << endl; 
        }
        for (auto pair : schema.structure) {
            string tablePath = schema.name + "/" + pair.first;
            if (!fs::exists(tablePath)) {
                fs::create_directory(tablePath);
                
            } else {
                cerr << "Table directory " << tablePath << " already exists" << endl; 
            }

            string pagePath = tablePath + "/1.csv";
            if (!fs::exists(pagePath)) {
                writeList(pagePath, pair.first, pair.second);

                string pkSequencePath = tablePath + "/" + pair.first + "_pk_sequence"; 
                ofstream file1(pkSequencePath);
                file1 << 0;
                file1.close();
            } else {
                cerr << "Page " << pagePath << " already exists" << endl;
            }
        }
    }

    void insert(string baseDir, string tableName, vector<string> values) {
        string tablePath = baseDir + "/" + tableName;
        smatch match;
        vector<string> pages = getCSVFromDir(tablePath);

        for (string pagePath : pages) {
            if (countStringsInFile(pagePath) - 1 < schema.tuplesLimit) {
                AddRowInCSV(pagePath, tablePath, tableName, values);
                return;
            }
        }
        string newPagePath = tablePath + "/" + to_string(pages.size()+1) + ".csv";
        writeList(newPagePath, tableName, schema.structure[tableName]);
        AddRowInCSV(newPagePath, tablePath, tableName, values);
    }

    void fDelete(vector<string> tables, string condition) {
        if (condition == "") {
            for (string tableName : tables) {
                vector<string> pages = getCSVFromDir(schema.name + "/" + tableName);
                for (string pagePath : pages) {
                    fs::remove(pagePath);
                }
                string pagePath = schema.name + "/" + tableName + "/1.csv";
                writeList(pagePath, tableName, schema.structure[tableName]);
            }
        } else {
            Node* node = getConditionTree(condition);
            for (string tableName : tables) {
                vector<string> pages = getCSVFromDir(schema.name + "/" + tableName);
                for (string pagePath : pages) {
                    vector<vector<string>> page = readCSV(pagePath);
                    vector<string> header = page[0];
                    int pageLen = page.size();
                    for (size_t i = 1; i < pageLen; i++) {
                        if (isValidRow(node, page[i], header, tables, tableName)) {
                            page.erase(page.begin() + i);
                            pageLen--;
                            i--;
                        }
                    }
                    writeCSV(pagePath, page);
                }
            }
        }
    }

    void select(vector<string> columns, vector<string> tables, string condition) {
        // Проверяем, что таблица существует и это единственная таблица
        for (string tableName : tables) {
            if (schema.structure.find(tableName) == schema.structure.end()) {
                cerr << "Table '" << tableName << "' does not exist." << endl;
                return;
            }
        }

        if (condition == "") { // без WHERE
            if (tables.size() == 1) { // для одной таблицы
                string tableName = tables[0];

                // Проверяем, что все колонки относятся к таблице
                vector<string> availableColumns = schema.structure[tableName];
                vector<string> columnsToSelect;

                for (const string& fullColumn : columns) {
                    // Разделяем строку на таблицу и колонку (формат таблица.колонка)
                    size_t dotPos = fullColumn.find(".");
                    if (dotPos == string::npos) {
                        cerr << "Error: Column '" << fullColumn << "' is not in 'table.column' format." << endl;
                        return;
                    }

                    string requestedTable = fullColumn.substr(0, dotPos);
                    string requestedColumn = fullColumn.substr(dotPos + 1);

                    // Проверяем, что таблица совпадает с той, что указана в запросе
                    if (requestedTable != tableName) {
                        cerr << "Error: Column '" << fullColumn << "' does not belong to table '" << tableName << "'." << endl;
                        return;
                    }

                    // Проверяем, что колонка существует в таблице
                    if (find(availableColumns.begin(), availableColumns.end(), requestedColumn) == availableColumns.end()) {
                        cerr << "Error: Column '" << requestedColumn << "' does not exist in table '" << tableName << "'." << endl;
                        return;
                    }

                    // Добавляем колонку в список для вывода
                    columnsToSelect.push_back(requestedColumn);
                }

                // Теперь читаем CSV-файлы и выводим только запрошенные колонки
                vector<string> pages = getCSVFromDir(schema.name + "/" + tableName);
                for (const string& pagePath : pages) {
                    vector<vector<string>> page = readCSV(pagePath);
                    vector<string> header = page[0];

                    // Определяем индексы нужных колонок
                    vector<int> columnIndexes;
                    for (const string& col : columnsToSelect) {
                        auto it = find(header.begin(), header.end(), col);
                        if (it != header.end()) {
                            columnIndexes.push_back(distance(header.begin(), it));
                        }
                    }

                    for (vector<string> row : page) {
                        // Выводим только те колонки, которые были запрошены
                        for (int i = 0; i < columnIndexes.size(); ++i) {
                            cout << row[columnIndexes[i]];
                            if (i < columnIndexes.size() - 1) {
                                cout << ", ";
                            }
                        }
                        cout << endl;
                    }
                }
            } else { // для нескольких
                vector<vector<vector<string>>> tablesData;
                vector<vector<int>> columnIndexesList;

                // Собираем данные для всех таблиц
                for (const string& tableName : tables) {
                    vector<string> availableColumns = schema.structure[tableName];
                    vector<string> columnsToSelect;

                    // Определяем, какие колонки нужно выбирать из каждой таблицы
                    for (const string& fullColumn : columns) {
                        size_t dotPos = fullColumn.find(".");
                        if (dotPos == string::npos) {
                            cerr << "Error: Column '" << fullColumn << "' is not in 'table.column' format." << endl;
                            return;
                        }

                        string requestedTable = fullColumn.substr(0, dotPos);
                        string requestedColumn = fullColumn.substr(dotPos + 1);

                        if (requestedTable == tableName) {
                            if (find(availableColumns.begin(), availableColumns.end(), requestedColumn) == availableColumns.end()) {
                                cerr << "Error: Column '" << requestedColumn << "' does not exist in table '" << tableName << "'." << endl;
                                return;
                            }
                            columnsToSelect.push_back(requestedColumn);
                        }
                    }

                    // Читаем CSV-файлы таблицы и собираем строки
                    vector<vector<string>> tableRows;
                    vector<string> pages = getCSVFromDir(schema.name + "/" + tableName);
                    for (const string& pagePath : pages) {
                        vector<vector<string>> page = readCSV(pagePath);
                        vector<string> header = page[0];

                        // Определяем индексы нужных колонок
                        vector<int> columnIndexes;
                        for (const string& col : columnsToSelect) {
                            auto it = find(header.begin(), header.end(), col);
                            if (it != header.end()) {
                                columnIndexes.push_back(distance(header.begin(), it));
                            }
                        }
                        columnIndexesList.push_back(columnIndexes);

                        for (int i = 1; i < page.size(); i++) {
                            tableRows.push_back(page[i]);
                        }
                    }
                    tablesData.push_back(tableRows);
                }

                // Выполняем декартово произведение данных
                vector<vector<string>> crossProduct;
                vector<string> currentCombination;

                function<void(int)> generateCombinations = [&](int depth) {
                    if (depth == tables.size()) {
                        crossProduct.push_back(currentCombination);
                        return;
                    }

                    for (const vector<string>& row : tablesData[depth]) {
                        for (int idx : columnIndexesList[depth]) {
                            currentCombination.push_back(row[idx]);
                        }
                        generateCombinations(depth + 1);
                        currentCombination.resize(currentCombination.size() - columnIndexesList[depth].size());
                    }
                };

                generateCombinations(0);

                // Выводим результаты
                for (const auto& combination : crossProduct) {
                    for (int i = 0; i < combination.size(); ++i) {
                        cout << combination[i];
                        if (i < combination.size() - 1) {
                            cout << ", ";
                        }
                    }
                    cout << endl;
                }
            }
        } else { // с WHERE
            Node* node = getConditionTree(condition);
            if (tables.size() == 1) { // для одной таблицы
                string tableName = tables[0];

                // Проверяем, что все колонки относятся к таблице
                vector<string> availableColumns = schema.structure[tableName];
                vector<string> columnsToSelect;

                for (const string& fullColumn : columns) {
                    // Разделяем строку на таблицу и колонку (формат таблица.колонка)
                    size_t dotPos = fullColumn.find(".");
                    if (dotPos == string::npos) {
                        cerr << "Error: Column '" << fullColumn << "' is not in 'table.column' format." << endl;
                        return;
                    }

                    string requestedTable = fullColumn.substr(0, dotPos);
                    string requestedColumn = fullColumn.substr(dotPos + 1);

                    // Проверяем, что таблица совпадает с той, что указана в запросе
                    if (requestedTable != tableName) {
                        cerr << "Error: Column '" << fullColumn << "' does not belong to table '" << tableName << "'." << endl;
                        return;
                    }

                    // Проверяем, что колонка существует в таблице
                    if (find(availableColumns.begin(), availableColumns.end(), requestedColumn) == availableColumns.end()) {
                        cerr << "Error: Column '" << requestedColumn << "' does not exist in table '" << tableName << "'." << endl;
                        return;
                    }

                    // Добавляем колонку в список для вывода
                    columnsToSelect.push_back(requestedColumn);
                }

                // Теперь читаем CSV-файлы и выводим только запрошенные колонки
                vector<string> pages = getCSVFromDir(schema.name + "/" + tableName);
                for (const string& pagePath : pages) {
                    vector<vector<string>> page = readCSV(pagePath);
                    vector<string> header = page[0];

                    // Определяем индексы нужных колонок
                    vector<int> columnIndexes;
                    for (const string& col : columnsToSelect) {
                        auto it = find(header.begin(), header.end(), col);
                        if (it != header.end()) {
                            columnIndexes.push_back(distance(header.begin(), it));
                        }
                    }
                    for (vector<string> row : page) {
                        // Выводим только те колонки, которые были запрошены
                        if (isValidRow(node, row, header, tables, tableName)) {
                            for (int i = 0; i < columnIndexes.size(); ++i) {
                                cout << row[columnIndexes[i]];
                                if (i < columnIndexes.size() - 1) {
                                    cout << ", ";
                                }
                            }
                            cout << endl;
                        }
                    }
                }
            } else { // для нескольких
                vector<vector<vector<string>>> tablesData;
                vector<vector<int>> columnIndexesList;

                // Собираем данные для всех таблиц
                for (const string& tableName : tables) {
                    vector<string> availableColumns = schema.structure[tableName];
                    vector<string> columnsToSelect;

                    // Определяем, какие колонки нужно выбирать из каждой таблицы
                    for (const string& fullColumn : columns) {
                        size_t dotPos = fullColumn.find(".");
                        if (dotPos == string::npos) {
                            cerr << "Error: Column '" << fullColumn << "' is not in 'table.column' format." << endl;
                            return;
                        }

                        string requestedTable = fullColumn.substr(0, dotPos);
                        string requestedColumn = fullColumn.substr(dotPos + 1);

                        if (requestedTable == tableName) {
                            if (find(availableColumns.begin(), availableColumns.end(), requestedColumn) == availableColumns.end()) {
                                cerr << "Error: Column '" << requestedColumn << "' does not exist in table '" << tableName << "'." << endl;
                                return;
                            }
                            columnsToSelect.push_back(requestedColumn);
                        }
                    }

                    // Читаем CSV-файлы таблицы и собираем строки
                    vector<vector<string>> tableRows;
                    vector<string> pages = getCSVFromDir(schema.name + "/" + tableName);
                    for (const string& pagePath : pages) {
                        vector<vector<string>> page = readCSV(pagePath);
                        vector<string> header = page[0];

                        // Определяем индексы нужных колонок
                        vector<int> columnIndexes;
                        for (const string& col : columnsToSelect) {
                            auto it = find(header.begin(), header.end(), col);
                            if (it != header.end()) {
                                columnIndexes.push_back(distance(header.begin(), it));
                            }
                        }
                        columnIndexesList.push_back(columnIndexes);

                        for (int i = 1; i < page.size(); i++) {
                            if (isValidRow(node, page[i], header, tables, tableName)) {
                                tableRows.push_back(page[i]);
                            }
                        }
                    }
                    tablesData.push_back(tableRows);
                }

                // Выполняем декартово произведение данных
                vector<vector<string>> crossProduct;
                vector<string> currentCombination;

                function<void(int)> generateCombinations = [&](int depth) {
                    if (depth == tables.size()) {
                        crossProduct.push_back(currentCombination);
                        return;
                    }

                    for (const vector<string>& row : tablesData[depth]) {
                        for (int idx : columnIndexesList[depth]) {
                            currentCombination.push_back(row[idx]);
                        }
                        generateCombinations(depth + 1);
                        currentCombination.resize(currentCombination.size() - columnIndexesList[depth].size());
                    }
                };

                generateCombinations(0);

                // Выводим результаты
                for (const auto& combination : crossProduct) {
                    for (int i = 0; i < combination.size(); ++i) {
                        cout << combination[i];
                        if (i < combination.size() - 1) {
                            cout << ", ";
                        }
                    }
                    cout << endl;
                }
            }
        }
    }
};
#endif
