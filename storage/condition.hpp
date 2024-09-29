#ifndef CONDITION_H
#define CONDITION_H

#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <algorithm>
#include <unordered_map>
#include "./utils.hpp"

// Тип узла
enum class NodeType {
    ConditionNode,
    OrNode,
    AndNode
};

// Структура для узла дерева выражений
struct Node {
    NodeType nodeType;
    string value;
    Node* left;
    Node* right;

    Node(NodeType type, const string& val = "", Node* l = nullptr, Node* r = nullptr)
        : nodeType(type), value(val), left(l), right(r) {}
};

// Вспомогательная функция для разделения строки по оператору
vector<string> splitByOperator(const string& query, const string& op) {
    string operatorPattern = "\\s+" + op + "\\s+";
    regex re(operatorPattern, regex_constants::icase);
    sregex_token_iterator it(query.begin(), query.end(), re, -1);
    sregex_token_iterator end;
    return vector<string>(it, end);
}


Node* getConditionTree(const string& query) {
    auto orParts = splitByOperator(query, "OR");

    // OR
    if (orParts.size() > 1) {
        Node* root = new Node(NodeType::OrNode);
        root->left = getConditionTree(orParts[0]);
        root->right = getConditionTree(join(orParts, 1, "OR"));
        return root;
    }

    // AND
    auto andParts = splitByOperator(query, "AND");
    if (andParts.size() > 1) {
        Node* root = new Node(NodeType::AndNode);
        root->left = getConditionTree(andParts[0]);
        root->right = getConditionTree(join(andParts, 1, "AND"));
        return root;
    }

    // Simple condition
    return new Node(NodeType::ConditionNode, trim(query));
}

bool isValidRow(Node* node, const vector<string>& row, const vector<string>& header,
                const vector<string>& neededTables, const string& curTable) {
    if (!node) {
        return false;
    }

    switch (node->nodeType) {
    case NodeType::ConditionNode: {
        auto parts = split(node->value, "=");
        if (parts.size() != 2) {
            return false;
        }

        string part1 = trim(parts[0]);
        string value = trim(trim(parts[1]), '\'');

        auto part1Splitted = split(part1, ".");
        if (part1Splitted.size() != 2) {
            return false;
        }

        string table = part1Splitted[0];
        string column = part1Splitted[1];
        // проверяем, что таблица есть в запросе
        if (find(neededTables.begin(), neededTables.end(), table) == neededTables.end()) {
            return false;
        }

        auto it = find(header.begin(), header.end(), column); // ищем индекс нужной колонки
        if (it == row.end()) { // проверяем, что колонка с таким именем существует в таблица
            return false;
        }
        int columnIndex = distance(header.begin(), it);

        if (curTable == table && row[columnIndex] != value) {
            return false;
        }

        return true;
    }
    case NodeType::OrNode:
        return isValidRow(node->left, row, header, neededTables, curTable) ||
                isValidRow(node->right, row, header, neededTables, curTable);
    case NodeType::AndNode:
        return isValidRow(node->left, row, header, neededTables, curTable) &&
                isValidRow(node->right, row, header, neededTables, curTable);
    default:
        return false;
    }
}


#endif