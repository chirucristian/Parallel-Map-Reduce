build:
	g++ MapReduce.cpp -o MapReduce -lpthread -Wall

clean:
	rm -f MapReduce
