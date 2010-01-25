#ifndef FIREBIRD_H
#define FIREBIRD_H

#include <list>
#include <map>
#include <vector>
#include <omp.h>
#include <stdio.h>
#include <assert.h>
#include "time.h"

#define __TIMING__

#define dlog(...) //printf(__VA_ARGS__)


typedef unsigned int my_size_t;

template<class KeyT>
my_size_t FireBirdHash(const KeyT & key){
	return 0;
}

template <typename InputDataT, typename InterKeyT, typename InterValT, typename OutputValT,
	typename HashFuncT=FireBirdHash<InterKeyT>, bool incremental_combine=false, bool non_conflict_hash=false >
class MapReduceScheduler{
public:
	// typedefs
	struct MapInputT{
		MapInputT():ptr(NULL),size(0){};
		const InputDataT * ptr;
		my_size_t size;
	};

	struct InterKeyValT{
		InterKeyValT(){};
		InterKeyValT(const InterKeyT & k, const MapOutputValT & v):key(k),val(v){};
		InterKeyT key;
		MapOutputValT val;
	};

	typedef std::list<OutputValT> OutputValListT;

	struct KeyValsT{
		KeyValsT():vals(NULL),key(){};
		KeyValsT(const InterKeyT & k, const MapOutputValsT * v):key(k),vals(v){};
		InterKeyT key;
		const MapOutputValsT * vals;
	};

	struct InterKeyValsT{
		InterKeyValsT():vals(NULL),key(){};
		InterKeyValsT(const InterKeyT & k, const MapOutputValsT * v):key(k),vals(v){};
		InterKeyT key;
		const MapOutputValsT * vals;
	};
	typedef typename MapOutputValsT::const_iterator MapOutputValIter;

	typedef std::map<InterKeyT, MapOutputValsT> InterKeyValsMapT;
	typedef std::vector<KeyValT> KeyValVectorT;
	typedef std::vector<InterKeyValT> InterKeyValVector;

	// intermediate data
	class InterMap{
	private:
		mutable InterKeyValsMapT data;
		InterKeyValsMapT* _data;
	public :
		InterMap():_data(NULL){};
		~InterMap(){delete[] _data;};
		void reset()
		{
			data.clear();
			delete[] _data;
			_data = new InterKeyValsMapT[omp_get_max_threads()];
		}

	public:
		void insert(const InterKeyT & key, const MapOutputValT & val){
			int thread_id = omp_get_thread_num();
			_data[thread_id][key].push_back(val);
		}
		
		const InterKeyValsMapT & get_data() const {
			data.clear();
			for(int i = 0;i<omp_get_max_threads();i++)
			{
				for(typename InterKeyValsMapT::iterator iter = _data[i].begin(); iter != _data[i].end(); iter++)
				{
					MapOutputValsT & l=data[iter->first];
					l.splice(l.begin(),iter->second);
				}
			}
			return data;
		}
	};

	// output data
	class OutputVector{
	private:
		mutable KeyValVectorT data;
		KeyValVectorT* _data;
	public:
		OutputVector():_data(NULL){};
		~OutputVector(){delete[] _data;}
		void reset()
		{
			data.clear();
			delete[] _data;
			_data  = new KeyValVectorT[omp_get_max_threads()];
		}

		void insert(const InterKeyT & key, const OutputValT & val){
			int thread_id = omp_get_thread_num();
			_data[thread_id].push_back(KeyValT(key,val));

		}

		const KeyValVectorT & get_data() const{
			int num_elmts=0;
			for(int i=0;i<omp_get_max_threads();i++){
				num_elmts+=_data[i].size();
			}
			data.resize(num_elmts);
			int index=0;
			for(int i = 0;i<omp_get_max_threads();i++)
			{
				copy(_data[i].begin(),_data[i].end(),&data[0]+index);
				index+=_data[i].size();
			}
			return data;
		}
	};

public:
	// cntr and dstr
	MapReduceScheduler(){
		int num_procs=omp_get_max_threads();
		dlog("num processors: %d\n",num_procs);
		mNumMapThreads=num_procs;
		mNumReduceThreads=num_procs;
		mInputData=NULL;
		mInputDataSize=0;
		mUnitSize=10;

		map_time=merge_time=reduce_time=0;
	};
	virtual ~MapReduceScheduler(){};

	// fucntions to set runtime parameters
	my_size_t get_num_map_thread() const{
		return mNumMapThreads;
	};
	void set_num_map_thread(const my_size_t n){
		mNumMapThreads=n;
	};
	my_size_t get_num_reduce_thread() const{
		return mNumReduceThreads;
	};
	void set_num_reduce_thread(const my_size_t n){
		mNumReduceThreads=n;
	}

	// function to set input
	void set_input(const InputDataT * ptr, const my_size_t size){
		mInputData=ptr;
		mInputDataSize=size;
	}
	void set_unit_size(const my_size_t size){
		mUnitSize=size;
	}
	const MapInputT get_input_data(){
		MapInputT ret;
		ret.ptr=mInputData;
		ret.size=mInputDataSize;
		return ret;
	}

	// run the scheduler
	void run(){
		assert(mInputData!=NULL);
		// map
#ifdef __TIMING__
		double t1=get_time();
#endif
		int numMapTasks=(mInputDataSize+mUnitSize-1)/mUnitSize;
		omp_set_num_threads(mNumMapThreads);
		mInterData.reset();
		#pragma omp parallel for
		for(int i=0;i<numMapTasks;i++){
			int offset=mUnitSize*i;
			int len= mInputDataSize>(offset+mUnitSize)?mUnitSize:mInputDataSize-offset;
			map(mInputData+offset, len);
		}
#ifdef __TIMING__
		double t2=get_time();
		map_time+=t2-t1;
#endif

		// copy map into vector so that we can use parallel for
		const InterKeyValsMapT & interData=mInterData.get_data();
		std::vector<InterKeyValsT> interKeyVals(interData.size());
		int index=0;
		for(typename InterKeyValsMapT::const_iterator it=interData.begin(); it!=interData.end(); it++){
			interKeyVals[index++]=InterKeyValsT(it->first, &(it->second));
		}
#ifdef __TIMING__
		double t3=get_time();
		merge_time+=t3-t2;
#endif

		// reduce
		keyForThreads.clear();
		keyForThreads.resize(mNumReduceThreads);
		int numReduceTasks=interKeyVals.size();
		omp_set_num_threads(mNumReduceThreads);
		mOutputData.reset();
		#pragma omp parallel for
		for(int i=0;i<numReduceTasks;i++){
			const InterKeyValsT & kvp=interKeyVals[i];
			int threadNum=omp_get_thread_num();
			keyForThreads[threadNum]=kvp.key;
			reduce(kvp.key, kvp.vals->begin(), kvp.vals->end());
		}
#ifdef __TIMING__
		double t4=get_time();
		reduce_time+=t4-t3;
#endif
	}

	// function to get output
	const vector<KeyValT> & get_output() const{
		return mOutputData.get_data();
	}
	
	// emit
	void emit_intermediate(const InterKeyT & key, const MapOutputValT & val){
		mInterData.insert(key,val);
		
	}
	void emit(const OutputValT & val){
		mOutputData.insert(keyForThreads[omp_get_thread_num()], val);
	}


	virtual void print_time(){
#ifdef __TIMING__
		printf("-- map time: %f\n",map_time);
		printf("-- merge time: %f\n",merge_time);
		printf("-- reduce time: %f\n",reduce_time);
#endif
	}
private:
	// num threads
	my_size_t mNumMapThreads;
	my_size_t mNumReduceThreads;
	// input data
	const InputDataT * mInputData;
	my_size_t mInputDataSize;
	my_size_t mUnitSize;
	// intermediate data
	InterMap mInterData;
	// output data
	OutputVector mOutputData;
	// auxiliary data for the threads
	vector<InterKeyT> keyForThreads;

	// profiling data
	double map_time;
	double merge_time;
	double reduce_time;

private:
	// user specified functions
	virtual void map(const InputDataT *, const my_size_t)=0;
	virtual void reduce(const InterKeyT &, const MapOutputValIter & valBegin, const MapOutputValIter & valEnd)=0;
};

#endif
