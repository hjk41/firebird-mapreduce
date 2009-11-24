#include<iostream>
using namespace std;

#include "firebird.h"
#include <stdlib.h>


class MyScheduler: public MapReduceScheduler<int,int, int, int>{
public:
	virtual void map(const int * data, const unsigned int len){
		for(int i=0;i<len;i++){
			emit_intermediate(data[i],1);
		}
	}
	virtual void reduce(const int &, const MapOutputValIter & valBegin, const MapOutputValIter & valEnd){
		int sum=0;
		for(MapOutputValIter it=valBegin; it!=valEnd; it++)
			sum++;
		emit(sum);
	}
};

const int N=1000;
const int MODE=10;
void gen_ints(int * arr, int n){
	srand(1000);
	for(int i=0;i<n;i++)
		arr[i]=rand()%MODE;
}


int main(){
	int * ints=new int[N];
	gen_ints(ints,N);
	
	MyScheduler my;
	my.set_input(ints, N);
	my.run();

	const vector<MyScheduler::KeyValT> & output=my.get_output();
	for(int i=0;i<10 && i<output.size();i++){
		cout<<"("<<output[i].key<<","<<output[i].val<<")"<<endl;
	}

	delete[] ints;
	return 0;
}
