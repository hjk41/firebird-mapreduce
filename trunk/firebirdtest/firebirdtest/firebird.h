#ifndef FIREBIRD_H
#define FIREBIRD_H

#include <list>
#include <map>
#include <vector>
#include <omp.h>
#include <stdio.h>
#include <assert.h>

#define dlog(...) //printf(__VA_ARGS__)

template <typename InputDataT, typename MapOutputValT,typename OutputKeyT, typename OutputValT>
class MapReduceScheduler{
public:
	// typedefs
	typedef unsigned int UINT;
	struct MapInputT{
		MapInputT():ptr(NULL),size(0){};
		const InputDataT * ptr;
		const UINT size;
	};
	struct KeyValT{
		KeyValT(){};
		KeyValT(const OutputKeyT & k, const OutputValT & v):key(k),val(v){};
		OutputKeyT key;
		OutputValT val;
	};

	struct InnerKeyValT
	{
		InnerKeyValT(){};
		InnerKeyValT(const OutputKeyT & k, const MapOutputValT & v):key(k),val(v){};
		OutputKeyT key;
		MapOutputValT val;
	};

	typedef std::list<OutputValT> OutputValsT;
	typedef std::list<MapOutputValT> MapOutputValsT;

	struct KeyValsT{
		KeyValsT():vals(NULL),key(){};
		KeyValsT(const OutputKeyT & k, const OutputValsT * v):key(k),vals(v){};
		OutputKeyT key;
		const OutputValsT * vals;
	};

	struct InnerKeyValsT{
		InnerKeyValsT():vals(NULL),key(){};
		InnerKeyValsT(const OutputKeyT & k, const MapOutputValsT * v):key(k),vals(v){};
		OutputKeyT key;
		const MapOutputValsT * vals;
	};
	typedef typename MapOutputValsT::const_iterator MapOutputValIter;

	typedef std::map<OutputKeyT, OutputValsT> KeyValsMapT;
	typedef std::map<OutputKeyT, MapOutputValsT> InnerKeyValsMapT;
	typedef std::vector<KeyValT> KeyValVectorT;
	typedef std::vector<InnerKeyValT> InnerKeyValVector;
public:
	// cntr and dstr
	MapReduceScheduler(){
		
		int num_procs=omp_get_num_procs();
		dlog("num processors: %d\n",num_procs);
		mNumMapThreads=num_procs;
		mNumReduceThreads=num_procs;
		mInputData=NULL;
		mInputDataSize=0;
		mUnitSize=10;
	};
	virtual ~MapReduceScheduler(){};

	// fucntions to set runtime parameters
	UINT get_num_map_thread() const{
		return mNumMapThreads;
	};
	void set_num_map_thread(const UINT n){
		mNumMapThreads=n;
	};
	UINT get_num_reduce_thread() const{
		return mNumReduceThreads;
	};
	void set_num_reduce_thread(const UINT n){
		mNumReduceThreads=n;
	}

	// function to set input
	void set_input(const InputDataT * ptr, const UINT size){
		mInputData=ptr;
		mInputDataSize=size;
	}
	void set_unit_size(const UINT size){
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
		mOutputData.clear();
		// map
		int numMapTasks=(mInputDataSize+mUnitSize-1)/mUnitSize;
		#pragma omp parallel for
		for(int i=0;i<numMapTasks;i++){
			int offset=mUnitSize*i;
			int len= mInputDataSize>(offset+mUnitSize)?mUnitSize:mInputDataSize-offset;
			map(mInputData+offset, len);
		}
		#pragma omp barrier
		const InnerKeyValsMapT & interData=mInterData.get_data();
		std::vector<InnerKeyValsT> interKeyVals(interData.size());
		int index=0;
		for(typename InnerKeyValsMapT::const_iterator it=interData.begin(); it!=interData.end(); it++){
			interKeyVals[index++]=InnerKeyValsT(it->first, &(it->second));
		}
		// reduce
		keyForThreads.clear();
		keyForThreads.resize(mNumReduceThreads);
		int numReduceTasks=interKeyVals.size();
		#pragma omp parallel for
		for(int i=0;i<numReduceTasks;i++){
			const InnerKeyValsT & kvp=interKeyVals[i];
			int threadNum=omp_get_thread_num();
			keyForThreads[threadNum]=kvp.key;
			reduce(kvp.key, kvp.vals->begin(), kvp.vals->end());
		}
	}

	// function to get output
	const vector<KeyValT> & get_output() const{
		return mOutputData.get_data();
	}
	
	// emit
	void emit_intermediate(const OutputKeyT & key, const MapOutputValT & val){
		mInterData.insert(key,val);
		
	}
	void emit(const OutputValT & val){
		mOutputData.insert(keyForThreads[omp_get_thread_num()], val);
	}

	// user specified functions
	virtual void map(const InputDataT *, const UINT)=0;
	virtual void reduce(const OutputKeyT &, const MapOutputValIter & valBegin, const MapOutputValIter & valEnd)=0;
private:
	// num threads
	UINT mNumMapThreads;
	UINT mNumReduceThreads;
	// input data
	const InputDataT * mInputData;
	UINT mInputDataSize;
	UINT mUnitSize;
	// intermediate data
	class InterMap{
	private:
		InnerKeyValsMapT data;
		
	public:
		void insert(const OutputKeyT & key, const MapOutputValT & val){
			#pragma omp critical(inter_insert)
			{
				data[key].push_back(val);
			}
		}
		const InnerKeyValsMapT & get_data() const{
			return data;
		}
	};
	InterMap mInterData;

	// output data
	class OutputVector{
	private:
		KeyValVectorT data;
	public:
		void insert(const OutputKeyT & key, const OutputValT & val){
			
			#pragma omp critical(output_insert)
			{
				data.push_back(KeyValT(key,val));
			}
		}

		void clear()
		{
			data.clear();
		}

		const KeyValVectorT & get_data() const{
			return data;
		}
	};
	OutputVector mOutputData;

	// auxiliary data for the threads
	vector<OutputKeyT> keyForThreads;
};


#endif
