#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include "../data_structures/vector.hpp"

using namespace std;

string trim(const string& str, char ch = ' ') {
    size_t first = str.find_first_not_of(ch);
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(ch);
    return str.substr(first, last - first + 1);
}

Vector<string> split(string str, string delimiter) {
    Vector<string> values;
    size_t pos = 0;
    while ((pos = str.find(delimiter)) != string::npos) {
        string value = str.substr(0, pos);
        str = trim(trim(str), '\t');
        values.pushBack(value);
        str.erase(0, pos + 1);
    }
    str = trim(trim(str), '\t');
    values.pushBack(str);  // Последнее значение

    return values;
}

string join(Vector<string>& parts, size_t startIndex, string delimiter) {
    string result;
    for (size_t i = startIndex; i < parts.size(); ++i) {
        if (i != startIndex) result += delimiter;
        result += parts.get(i);
    }
    return result;
}

#endif