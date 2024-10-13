#ifndef CONDITION_H
#define CONDITION_H

#include <iostream>
#include <string>
#include <regex>
#include <algorithm>
#include <unordered_map>
#include "./utils.hpp"

#include "../data_structures/vector.hpp"

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
Vector<string> splitByOperator(const string& query, const string& op) {
    string operatorPattern = "\\s+" + op + "\\s+";
    regex re(operatorPattern, regex_constants::icase);
    sregex_token_iterator it(query.begin(), query.end(), re, -1);
    sregex_token_iterator end;

    Vector<string> result;
    while (it != end) {
        result.pushBack(*it);
        ++it;
    }

    return result;
}


Node* getConditionTree(const string& query) {
    Vector<string> orParts = splitByOperator(query, "OR");

    // OR
    if (orParts.size() > 1) {
        Node* root = new Node(NodeType::OrNode);
        root->left = getConditionTree(orParts.get(0));
        root->right = getConditionTree(join(orParts, 1, "OR"));
        return root;
    }

    // AND
    Vector<string> andParts = splitByOperator(query, "AND");
    if (andParts.size() > 1) {
        Node* root = new Node(NodeType::AndNode);
        root->left = getConditionTree(andParts.get(0));
        root->right = getConditionTree(join(andParts, 1, "AND"));
        return root;
    }

    // Simple condition
    return new Node(NodeType::ConditionNode, trim(query));
}

bool isValidRow(Node* node, const Vector<string>& row, const Vector<string>& header,
                const Vector<string>& neededTables, const string& curTable) {
    if (!node) {
        return false;
    }

    switch (node->nodeType) {
    case NodeType::ConditionNode: {
        auto parts = split(node->value, "=");
        if (parts.size() != 2) {
            return false;
        }

        string part1 = trim(parts.get(0));
        string value = trim(trim(parts.get(1)), '\'');

        Vector<string> part1Splitted = split(part1, ".");
        if (part1Splitted.size() != 2) {
            return false;
        }

        string table = part1Splitted.get(0);
        string column = part1Splitted.get(1);
        // проверяем, что таблица есть в запросе
        if (neededTables.find(table) == -1) {
            return false;
        }

        int columnIndex = header.find(column); // ищем индекс нужной колонки
        if (columnIndex == -1) { // проверяем, что колонка с таким именем существует в таблица
            return false;
        }

        if (curTable == table && row.get(columnIndex) != value) {
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