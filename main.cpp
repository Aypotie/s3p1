#include <iostream>
#include <thread>
#include <mutex>
#include <string>
#include <netinet/in.h>
#include <unistd.h>
#include "./storage/storage.hpp"
#include "./storage/menu.hpp" // реализация командного меню

#define BUFLEN 1024

using namespace std;

const int PORT = 7432;

void nullBuffer(char* buf, int len) {
    for (int i = 0; i < len; i++) {
        buf[i] = 0;
    }
}

void handleClient(int clientSocket, Storage* storage) {
    char buffer[BUFLEN];
    nullBuffer(buffer, BUFLEN);

    while (true) {
        int valread = read(clientSocket, buffer, 1024);
        if (valread < 0) {
            cerr << "Error while reading query" << endl;
            return;
        }
        string command(buffer);
        command = trim(command, '\n');

        if (command == "exit") {
            break;
        }

        menu(clientSocket, command, storage); // Использование функции меню для обработки команд
        nullBuffer(buffer, BUFLEN);
    }
                    
    close(clientSocket);
}

void startServer(Storage& storage) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0); // создаём сервер
    if (server_fd == 0) {
        cerr << "Server creation error" << endl;
        return;
    }

    struct sockaddr_in address;

    address.sin_family = AF_INET; // используем ipv4
    address.sin_addr.s_addr = INADDR_ANY; // любой адрес
    address.sin_port = htons(PORT); // задаём порт

    // Привязываем сокет к порту
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        cerr << "Socket error" << endl;
        return;
    }

    // Слушаем подключения (до 3 штук)
    if (listen(server_fd, 3) < 0) {
        cerr << "Listen error" << endl;
        return;
    }

    cout << "Server listening on port " << PORT << "..." << endl;

    // Принимаем подключения клиентов
    while (true) {
        int newSocket;
        sockaddr_in clientAddress; // Структура для хранения адреса клиента
        socklen_t clientAddressLen = sizeof(clientAddress); // Размер структуры адреса клиента

        newSocket = accept(server_fd, (sockaddr*)&clientAddress, &clientAddressLen);
        if (newSocket < 0) {
            cerr << "Accept client error" << endl;
            return;
        }

        // Создаем новый поток для обработки каждого подключения
        thread clientThread(handleClient, newSocket, &storage);
        clientThread.detach(); // Отсоединяем поток для асинхронного выполнения
    }
}

int main(int argc, char const* argv[]) {
    Schema schema = readSchema("schema.json");
    Storage storage(schema);

    startServer(storage);

    return 0;
}
