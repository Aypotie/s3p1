#ifndef FILE_H
#define FILE_H

#include <iostream>
#include <fstream>
#include <algorithm>
#include <filesystem>
#include <regex>

#include "../data_structures/vector.hpp"

using namespace std;

namespace fs = filesystem;

regex pageRegex(".+\\d+\\.csv$");

int countStringsInFile(string path) {
    ifstream inFile(path);
    int stringsCount = count(istreambuf_iterator<char>(inFile), istreambuf_iterator<char>(), '\n');
    inFile.close();
    return stringsCount;
}

Vector<string> getCSVFromDir(string dirPath) {
    Vector<string> csvFiles;
    smatch match;
    for (const auto & entry : fs::directory_iterator(dirPath)) {
            string filepath = entry.path();
            if (regex_match(filepath, match, pageRegex)) {
                csvFiles.pushBack(entry.path());
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

void AddRowInCSV(string pagePath, string tablePath, string tableName, Vector<string> values) {
    ofstream file(pagePath, ios_base::app);

    int pk = incPk(tablePath, tableName);
    file << pk << ",";

    for (int i = 0; i < values.size(); i++) {
        if (i == values.size() - 1) {
            file << values.get(i);
        } else {
            file << values.get(i) << ",";
        }
    }
    file << endl;

    file.close();
}

// Функция для чтения CSV-файла в двумерный вектор строк
Vector<Vector<string>> readCSV(string filename) {
    Vector<Vector<string>> data;
    ifstream file(filename);

    if (!file.is_open()) {
        cerr << "Error: Could not open file " << filename << endl;
        return data;
    }

    string line;
    while (getline(file, line)) {
        Vector<string> row;
        stringstream lineStream(line);
        string cell;

        while (getline(lineStream, cell, ',')) {
            row.pushBack(cell);
        }

        data.pushBack(row);
    }

    file.close();
    return data;
}

void writeCSV(const string& filename, const Vector<Vector<string>>& data) {
    ofstream file(filename);

    if (!file.is_open()) {
        cerr << "Error: Could not open file " << filename << endl;
        return;
    }

    for (int i = 0; i < data.size(); i++) {
        Vector<string> row = data.get(i);
        for (size_t i = 0; i < row.size(); ++i) {
            file << row.get(i);
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