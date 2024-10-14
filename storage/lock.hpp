#ifndef LOCK_H
#define LOCK_H

#include <string>
#include <fstream>
#include "../data_structures/vector.hpp"

using namespace std;

bool isLock(string lockPath) {
    int lockValue;
    ifstream lockFile(lockPath);
    lockFile >> lockValue;
    lockFile.close();

    if (lockValue == 1) {
        return true;
    }
    return false;
}

void lock(string lockPath) {
    if (isLock(lockPath)) {
        throw runtime_error("table already locked");
    }

    ofstream lockFile(lockPath);

    lockFile << 1;
    lockFile.close();
}

void unlock(string lockPath) {
    ofstream lockFile(lockPath);

    lockFile << 0;
    lockFile.close();
}

void lockTables(string dbName, const Vector<string>& tables) {
    for (int i = 0; i < tables.size(); i++) {
        string tableName = tables.get(i);
        string lockPath = dbName + "/" + tableName + "/" + tableName + "_lock";

        try {
            lock(lockPath);
        } catch (runtime_error& e) {
            throw runtime_error("Table " + tableName + " blocked");
        }
    }
}

void unlockTables(string dbName, const Vector<string>& tables) {
    for (int i = 0; i < tables.size(); i++) {
        string tableName = tables.get(i);
        string lockPath = dbName + "/" + tableName + "/" + tableName + "_lock";

        unlock(lockPath);
    }
}

#endif