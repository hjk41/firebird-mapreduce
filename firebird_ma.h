#include "HMMfirebird_maCPU.h"
#include <omp.h>
#include <vector>

class firebird_ma{
private:
	std::vector<char *> allocated_blocks;
	char * current_page;
	unsigned int current_offset;
private:
	static const unsigned int PAGE_SIZE=1024*1024;
	static const unsigned int ALIGN_SIZE=4;
	unsigned int RoundToAlign(unsigned int size){
		return (size+ALIGN_SIZE-1)&(~(ALIGN_SIZE-1));
	}
	char * MallocLargeBlock(unsigned int size){
		char * block=new char[size];
		allocated_blocks.push_back(block);
		return block;
	}
public:
	firebird_ma();
	~firebird_ma();

	void * firebird_malloc(unsigned int size);
	void firebird_ma_Free(void * p);
};

//---------------------------------------
//	interface
//---------------------------------------
static firebird_ma * firebird_ma_pool;

void firebird_ma_init(unsigned int num_threads);
void firebird_ma_destroy();
void * firebird_malloc(unsigned int size);



//---------------------------------------
//	implementation
//---------------------------------------
firebird_ma::firebird_ma(){
	current_page=MallocLargeBlock(PAGE_SIZE);
	current_offset=0;
}

firebird_ma::~firebird_ma(){
	for(int i=0;i<allocated_blocks.size();i++){
		delete[] allocated_blocks[i];
	}
}

void * firebird_ma::firebird_malloc(unsigned int size){
	size=RoundToAlign(size);
	if(size<PAGE_SIZE/2){
		// malloc from local page
		if(current_offset+size<PAGE_SIZE){
			unsigned int old_offset=current_offset;
			current_offset+=size;
			return current_page+old_offset;
		}
		else{
			current_page=MallocLargeBlock(PAGE_SIZE);
			current_offset=size;
			return current_page;
		}
	}
	else{
		// malloc from global 
		return MallocLargeBlock(size);
	}
}

void firebird_ma_init(unsigned int num_threads){
	firebird_ma_pool=new firebird_ma[num_threads];
}

void firebird_ma_destroy(){
	delete[] firebird_ma_pool;
}

void * firebird_malloc(unsigned int size){
	return firebird_ma_pool[omp_get_thread_num()].firebird_malloc(size);
}

