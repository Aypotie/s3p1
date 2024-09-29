#ifndef FILE_H
#define FILE_H

#include <iostream>
#include <fstream>
#include <algorithm>
#include <filesystem>
#include <regex>

using namespace std;

namespace fs = filesystem;

regex pageRegex(".+\\d+\\.csv$");

int countStringsInFile(string path) {
    ifstream inFile(path);
    int stringsCount = count(istreambuf_iterator<char>(inFile), istreambuf_iterator<char>(), '\n');
    inFile.close();
    return stringsCount;
}

vector<string> getCSVFromDir(string dirPath) {
    vector<string> csvFiles;
    smatch match;
    for (const auto & entry : fs::directory_iterator(dirPath)) {
            string filepath = entry.path();
            if (regex_match(filepath, match, pageRegex)) {
                csvFiles.push_back(entry.path());
            }
    }
    return csvFiles;
}

int incPk(string tablePath, string tableName) {
    string pkFilePath = tablePath + "/" + tableName + "_pk_sequence";

    ifstream file2(pkFilePath);
    int currentPk;
    file2 >> currentPk;
    file2.close();

    currentPk++;
    ofstream file3(pkFilePath);
    file3 << currentPk;
    file3.close();

    return currentPk;
}

void AddRowInCSV(string pagePath, string tablePath, string tableName, vector<string> values) {
    ofstream file(pagePath, ios_base::app);

    int pk = incPk(tablePath, tableName);
    file << pk << ",";

    for (int i = 0; i < values.size(); i++) {
        if (i == values.size() - 1) {
            file << values[i];
        } else {
            file << values[i] << ",";
        }
    }
    file << endl;

    file.close();
}

// Функция для чтения CSV-файла в двумерный вектор строк
vector<vector<string>> readCSV(string filename) {
    vector<vector<string>> data;
    ifstream file(filename);

    if (!file.is_open()) {
        cerr << "Error: Could not open file " << filename << endl;
        return data;
    }

    string line;
    while (getline(file, line)) {
        vector<string> row;
        stringstream lineStream(line);
        string cell;

        while (getline(lineStream, cell, ',')) {
            row.push_back(cell);
        }

        data.push_back(row);
    }

    file.close();
    return data;
}

void writeCSV(const string& filename, const vector<vector<string>>& data) {
    ofstream file(filename);

    if (!file.is_open()) {
        cerr << "Error: Could not open file " << filename << endl;
        return;
    }

    for (const auto& row : data) {
        for (size_t i = 0; i < row.size(); ++i) {
            file << row[i];
            // Добавляем запятую, кроме последнего элемента в строке
            if (i < row.size() - 1) {
                file << ",";
            }
        }
        file << "\n";  // Переход на новую строку после каждой строки
    }

    file.close();
}

#endif