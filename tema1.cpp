#include <iostream>
#include <fstream>
#include <pthread.h>
#include <stdlib.h>
#include <queue>

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

int powInt(int n, int exp) {
    int num = n;
    for (int i = 2; i <= exp; i++) {
        num = num * n;
    }

    return num;
}

void getPerfectPowers(vector<int> &exponents, int n, int R) {
    if (n == 1) {
        for (int i = 2; i <= R + 1; i++) {
            exponents.push_back(i);
        }

        return;
    }

    for (int i = 2; i <= R + 1; i++) {
        for (int j = 2; j <= n; j++) {
            int power = powInt(j, i);
            if (power == n) {
                exponents.push_back(i);
            }
            else if (power > n) {
                break;
            }
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

            // printf("M%d got %s\n", args.ID, fileName.c_str());

            //  if(fileName == "in1.txt") {
            //     for (unsigned int i = 2; i <= R + 2; i++) {
            //         args.mapValue->at(exponents[i]).push_back(num);
            //     }
            // }
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

    

    return NULL;
}

int main(int argc, char** argv) {
    int M = atoi(argv[1]);
    int R = atoi(argv[2]);
    string inputFileName = argv[3];

    int fileNumber = 0;
    queue<string> fileNames;
    readInputFile(inputFileName, fileNumber, fileNames);
    
    // cout << fileNumber << endl;

    // for (int i = 0; i < fileNumber; i++) {
    //     cout << fileNames.front() << endl;
    //     fileNames.pop();
    // }

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
        argumentsM[i].ID = i;
        argumentsM[i].R = R;
        argumentsM[i].mutex = &mutexMap;
        argumentsM[i].fileNames = &fileNames;
        argumentsM[i].barrier = &barrierMap;
        argumentsM[i].mapValue = &mapValues[i];
        argumentsM[i].mappingDone = &mappingDone;
        pthread_create(&threads[i], NULL, map, &argumentsM[i]);
        // cout << "Created thread " << i << endl;
    }

    for (int i = 0; i < R; i++) {
        argumentsR[i].ID = i + 2;
        argumentsR[i].R = R;
        argumentsR[i].M = M;
        argumentsR[i].mappingDone = &mappingDone;
        argumentsR[i].mapValues = mapValues;
        pthread_create(&threads[i + M], NULL, reduce, &argumentsR[i]);
        // cout << "Created thread " << i + M << endl;
    }

    for (int i = 0; i < M + R; i++) {
		pthread_join(threads[i], NULL);
	}

    // for (int i = 0; i < M; i++) {
    //     cout << "M" << i << ": ";

    //     for (int j = 2; j < R + 2; j++) {
    //         unsigned int size = mapValues[i].at(j).size();
    //         cout << "{";
    //         for (unsigned int k = 0; k < size; k++) {
    //             cout << mapValues[i].at(j).at(k) << " ";
    //         }
    //         cout << "} ";
    //     }

    //     cout << endl;
    // }

    // unsigned int microsecond = 1000000;
    // usleep(3 * microsecond); //sleeps for 3 second

    pthread_mutex_destroy(&mutexMap);
    pthread_barrier_destroy(&barrierMap);
    return 0;
}