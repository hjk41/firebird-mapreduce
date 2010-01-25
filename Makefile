CC = g++ -march=i486
EXE = program

$(EXE): main.cpp firebird.h hash_table.h
	$(CC) main.cpp -o $(EXE)

clean:
	-rm program
