#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <vector>

using namespace std;

string trim(const string& str, char ch = ' ') {
    size_t first = str.find_first_not_of(ch);
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(ch);
    return str.substr(first, last - first + 1);
}

vector<string> split(string str, string delimiter) {
    vector<string> values;
    size_t pos = 0;
    while ((pos = str.find(delimiter)) != string::npos) {
        string value = str.substr(0, pos);
        str = trim(trim(str), '\t');
        values.push_back(value);
        str.erase(0, pos + 1);
    }
    str = trim(trim(str), '\t');
    values.push_back(str);  // Последнее значение

    return values;
}

string join(vector<string>& parts, size_t startIndex, string delimiter) {
    string result;
    for (size_t i = startIndex; i < parts.size(); ++i) {
        if (i != startIndex) result += delimiter;
        result += parts[i];
    }
    return result;
}

#endif