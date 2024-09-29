#include <iostream>
#include "./storage/storage.hpp"
#include "./storage/menu.hpp"

using namespace std;

int main() {
    string command;
    Schema schema = readSchema("schema.json");
    Storage storage(schema);

    while (true) {
        cout << "Enter command: ";
        getline(cin, command);

        menu(command, storage);
    }

    return 0;
}
