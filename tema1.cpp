#include <iostream>
#include <fstream>
#include <pthread.h>
#include <stdlib.h>
#include <queue>
#include <unordered_set>
#include <cmath>

using namespace std;

struct argumentM {
    int ID;
    int R;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    queue<string> *fileNames;
    vector<vector<int>> *mapValue;
    bool *mappingDone;
};

struct argumentR {
    int ID;
    int R;
    int M;
    vector<vector<int>> *mapValues;
    bool *mappingDone;
};

void readInputFile(string inputFileName, int &num, queue<string> &files) {
    ifstream inputFile(inputFileName);

    inputFile >> num;
    
    for (int i = 0; i < num; i++) {
        string file;
        inputFile >> file;
        files.push(file);
    }

    inputFile.close();
}

bool checkNumber(long int n, long int power) {
    long int start = 2;
    long int end = ceil(sqrt(n));
    long int mid, num;

    while (start <= end)
    {
        mid = (start + end) / 2;
        num = pow(mid, power);
        if (n == num)
            return true;

        else if (n > num) {
            start = mid + 1;
        }
        else {
            end = mid - 1;
        }
    }
    
    return false;
}

void getPerfectPowers(vector<int> &exponents, int n, int R) {

    if (n <= 0) {
        return;
    }
    if (n == 1) {
        for (int i = 2; i <= R + 1; i++) {
            exponents.push_back(i);
        }
        return;
    }

    for (int i = 2; i <= R + 1; i++) {
        if (checkNumber(n, i)) {
            exponents.push_back(i);
        }
        else if (i == 5) {
        }
    }
}

void *map(void *arg) {
    argumentM args = (*(argumentM *) arg);

    for (int i = 0; i < args.R + 2; i++) {
        args.mapValue->push_back(vector<int>());
    }

    string fileName = "initial value";

    while (fileName != "") {
        pthread_mutex_lock(args.mutex);
        if (!args.fileNames->empty()) {
            fileName = args.fileNames->front();
            args.fileNames->pop();
        } else {
            fileName = "";
        }
        pthread_mutex_unlock(args.mutex);

        if (fileName != "") {
            ifstream inputFile(fileName);
            int n = 0;
            inputFile >> n;

            for (int i = 0; i < n; i++) {
                int num = 0;
                inputFile >> num;

                vector<int> exponents;
                getPerfectPowers(exponents, num, args.R);

                for (unsigned int i = 0; i < exponents.size(); i++) {
                    args.mapValue->at(exponents[i]).push_back(num);
                }

            }

            inputFile.close();
        }
    }

    pthread_barrier_wait(args.barrier);

    if (args.ID == 0) {
        *(args.mappingDone) = true;
    }

    return NULL;
}

void *reduce(void *arg) {
    argumentR args = (*(argumentR *) arg);

    while (!(*args.mappingDone)) {}

    vector<int> list;
    
    unsigned int size = 0;
    for (int i = 0; i < args.M; i++) {
        size += args.mapValues[i].at(args.ID).size();
    }

    list.reserve(size);

    for (int i = 0; i < args.M; i++) {
        list.insert(list.end(), args.mapValues[i].at(args.ID).begin(), args.mapValues[i].at(args.ID).end());
    }
    unsigned int uniqueCount = unordered_set<int>(list.begin(), list.end()).size();

    string outFileName = "out" + to_string(args.ID) + ".txt";

    ofstream outputFile(outFileName);

    outputFile << uniqueCount;

    outputFile.close();
    return NULL;
}

int main(int argc, char** argv) {
    int M = atoi(argv[1]);
    int R = atoi(argv[2]);
    string inputFileName = argv[3];

    int fileNumber = 0;
    queue<string> fileNames;
    readInputFile(inputFileName, fileNumber, fileNames);
    
    pthread_t threads[M + R];
    argumentM argumentsM[M];
    argumentR argumentsR[R];
    vector<vector<int>> mapValues[M];
    bool mappingDone = false;

    pthread_mutex_t mutexMap;
    pthread_mutex_init(&mutexMap, NULL);
    pthread_barrier_t barrierMap;
    pthread_barrier_init(&barrierMap, NULL, M);

    // Observatie: Am pornit thread-urile unele dupa altele (ca in cerinta) dar in for-uri diferite
    for (int i = 0; i < M; i++) {
        mapValues[i].reserve(R+2);
        argumentsM[i].ID = i;
        argumentsM[i].R = R;
        argumentsM[i].mutex = &mutexMap;
        argumentsM[i].fileNames = &fileNames;
        argumentsM[i].barrier = &barrierMap;
        argumentsM[i].mapValue = &mapValues[i];
        argumentsM[i].mappingDone = &mappingDone;
        pthread_create(&threads[i], NULL, map, &argumentsM[i]);
    }

    for (int i = 0; i < R; i++) {
        argumentsR[i].ID = i + 2;
        argumentsR[i].R = R;
        argumentsR[i].M = M;
        argumentsR[i].mappingDone = &mappingDone;
        argumentsR[i].mapValues = mapValues;
        pthread_create(&threads[i + M], NULL, reduce, &argumentsR[i]);
    }

    for (int i = 0; i < M + R; i++) {
		pthread_join(threads[i], NULL);
	}

    pthread_mutex_destroy(&mutexMap);
    pthread_barrier_destroy(&barrierMap);
    return 0;
}