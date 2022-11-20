#include <fstream>
#include <vector>
#include <queue>
#include <unordered_set>
#include <cmath>
#include <pthread.h>

using namespace std;

// structura de date care tine argumentele pentru functia de Map
struct argumentM {
    int ID;
    int R;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    queue<string> *fileNames;
    vector<vector<int>> *mapValue;
    bool *mappingDone;
};

// structura de date care tine argumentele pentru functia de Reduce
struct argumentR {
    int ID;
    int R;
    int M;
    vector<vector<int>> *mappedValues;
    bool *mappingDone;
};

// citeste fisierul de intrare si pune numele fisierelor ce trebuiesc procesate in coada
void readInputFile(string inputFileName, queue<string> &files) {
    ifstream inputFile(inputFileName);

    int num = 0;
    inputFile >> num;
    
    for (int i = 0; i < num; i++) {
        string file;
        inputFile >> file;
        files.push(file);
    }

    inputFile.close();
}

// verifica daca n este putere perfecta cu exponentul power folosind cautarea binara
bool checkNumber(int n, int power) {
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

// adauga in vectorul exponent exponentii de la 2 la R + 1 pentru care n este putere perfecta
void getPerfectPowers(vector<int> &exponents, int n, int R) {
    if (n <= 0) {
        return;
    }

    // daca n este 1 atunci adaugam toti exponentii
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
    }
}

// realizeaza operatia de Map
void *map(void *arg) {
    // preluam argumentele
    argumentM args = (*(argumentM *) arg);

    // initializam lista cu vectori (adaugam doi vectori goli in fata pentru ca indecsii din lista sa corespunda exponentilor)
    for (int i = 0; i < args.R + 2; i++) {
        args.mapValue->push_back(vector<int>());
    }

    string fileName;

    // in aceasta bucla thread-urile de Map preiau un nou fisier din coada si il proceseaza.
    // in momentul in care nu mai sunt fisiere care trebuiesc procesate (coada e goala) bucla se termina
    do {
        // coada este zona critica si doar un thread poate face operatii pe ea la un moment dat.
        // pentru a realiza acest lucru am folosit un mutex
        pthread_mutex_lock(args.mutex);

        // verifica daca mai sunt fisiere in coada, caz in care preia unul pentru procesare
        if (!args.fileNames->empty()) {
            fileName = args.fileNames->front();
            args.fileNames->pop();
        } else {
            fileName = "";
        }

        // am terminat operatile pe coada si iesim din zona critica
        pthread_mutex_unlock(args.mutex);

        // daca thread-ul are un fisier de procesat realizeaza operatia de Map pe acesta
        if (fileName != "") {
            ifstream inputFile(fileName);
            int n = 0;
            inputFile >> n;

            // citim fisierul numar cu numar si il adaugam in listele exponentilor pentru care este putere perfecta
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
    } while (fileName != "");

    // deoarece pornim thread-urile de Reduce in acelasi timp ca cele de Map folosim aceasta bariera (cu count egal cu numarul de threaduri de Map)
    // pentru a ne asigura ca toate operatile de Map s-au finalizat
    pthread_barrier_wait(args.barrier);

    // thread-ul Map cu ID-ul 0 (care exista intotdeauna) va seta variabila mappingDone la true, aceasta fiind folosita de thread-urile Reduce
    // pentru a stii cand sa inceapa executia
    if (args.ID == 0) {
        *(args.mappingDone) = true;
    }

    return NULL;
}

void *reduce(void *arg) {
    // preluam argumentele
    argumentR args = (*(argumentR *) arg);

    // asteptam sa se termine operatiile de Map
    while (!(*args.mappingDone)) {}

    // lista combinata pentru exponentul corespunzator thread-ului de Reduce
    vector<int> list;

    // pentru a nu fi nevoie de o redimensionare a vectorului list calculam suma marimilor listelor pe care le combinam in acesta
    // si rezervam acest spatiu
    unsigned int size = 0;
    for (int i = 0; i < args.M; i++) {
        size += args.mappedValues[i].at(args.ID).size();
    }
    list.reserve(size);

    // combinam listele corespunzatoare exponentului de care se ocupa thread-ul curent in list
    for (int i = 0; i < args.M; i++) {
        list.insert(list.end(), args.mappedValues[i].at(args.ID).begin(), args.mappedValues[i].at(args.ID).end());
    }

    // pentru a afla numarul de valori unice din lista concatenata vom crea un unordered_set (deoarece acesta nu retine elemente duplicate)
    // cu elementele din aceasta iar numarul de valori unice va fi marimea acestuia
    unsigned int uniqueCount = unordered_set<int>(list.begin(), list.end()).size();

    // scriem in fisierul corespunzator exponentului de care se ocupa thread-ul curent numarul de valori unice
    string outFileName = "out" + to_string(args.ID) + ".txt";
    ofstream outputFile(outFileName);
    outputFile << uniqueCount;
    outputFile.close();

    return NULL;
}

int main(int argc, char** argv) {
    // preluam argumentele de la linia de comanda:
    // M - numar de thread-uri pentru Map
    // R - numar de thread-uri pentru Reduce
    // inputFileName - fisierul de intrare
    int M = atoi(argv[1]);
    int R = atoi(argv[2]);
    string inputFileName = argv[3];

    // citim fisierul de intrare si populam coada cu numele fisierelor ce trebuiesc procesate
    queue<string> fileNames;
    readInputFile(inputFileName, fileNames);
    
    // declaram thread-urile, listele de argumente pentru fiecare thread si structura care va retine listele rezultate din operatia de Map
    pthread_t threads[M + R];
    argumentM argumentsM[M];
    argumentR argumentsR[R];
    vector<vector<int>> mappedValues[M];
    bool mappingDone = false;

    // declaram si initializam  mutex-ul si bariera folosite in functia map
    pthread_mutex_t mutexMap;
    pthread_mutex_init(&mutexMap, NULL);
    pthread_barrier_t barrierMap;
    pthread_barrier_init(&barrierMap, NULL, M);

    // Observatie: Am pornit thread-urile unele dupa altele (ca in cerinta) dar in for-uri diferite
    // pornim thread-urile de Map
    for (int i = 0; i < M; i++) {
        mappedValues[i].reserve(R+2);
        argumentsM[i].ID = i;
        argumentsM[i].R = R;
        argumentsM[i].mutex = &mutexMap;
        argumentsM[i].fileNames = &fileNames;
        argumentsM[i].barrier = &barrierMap;
        argumentsM[i].mapValue = &mappedValues[i];
        argumentsM[i].mappingDone = &mappingDone;
        pthread_create(&threads[i], NULL, map, &argumentsM[i]);
    }

    // pornim thread-urile de Reduce
    for (int i = 0; i < R; i++) {
        // ID-ul pentru thread-urile de Reduce va fi acelasi cu exponentul de care se ocupa (Ex: Thread-ul Reduce cu ID-ul 2 se ocupa de exponentul 2)
        argumentsR[i].ID = i + 2;
        argumentsR[i].R = R;
        argumentsR[i].M = M;
        argumentsR[i].mappingDone = &mappingDone;
        argumentsR[i].mappedValues = mappedValues;
        pthread_create(&threads[i + M], NULL, reduce, &argumentsR[i]);
    }

    // facem join la toate thread-urile
    for (int i = 0; i < M + R; i++) {
		pthread_join(threads[i], NULL);
	}

    // distrugem mutex-ul si bariera
    pthread_mutex_destroy(&mutexMap);
    pthread_barrier_destroy(&barrierMap);
    return 0;
}