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

	struct InterKeyValT
	{
		InterKeyValT(){};
		InterKeyValT(const OutputKeyT & k, const MapOutputValT & v):key(k),val(v){};
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

	struct InterKeyValsT{
		InterKeyValsT():vals(NULL),key(){};
		InterKeyValsT(const OutputKeyT & k, const MapOutputValsT * v):key(k),vals(v){};
		OutputKeyT key;
		const MapOutputValsT * vals;
	};
	typedef typename MapOutputValsT::const_iterator MapOutputValIter;

	typedef std::map<OutputKeyT, OutputValsT> KeyValsMapT;
	typedef std::map<OutputKeyT, MapOutputValsT> InterKeyValsMapT;
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
			delete[] _data;
			_data = new InterKeyValsMapT[omp_get_num_procs()];
		}

	public:
		void insert(const OutputKeyT & key, const MapOutputValT & val){
			int thread_id = omp_get_thread_num();
			_data[thread_id][key].push_back(val);
		}
		
		const InterKeyValsMapT & get_data() const {
			
			for(int i = 0;i<omp_get_num_procs();i++)
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
			delete[] _data;
			_data  = new KeyValVectorT[omp_get_num_procs()];
		}

		void insert(const OutputKeyT & key, const OutputValT & val){
			int thread_id = omp_get_thread_num();
			_data[thread_id].push_back(KeyValT(key,val));

		}

		const KeyValVectorT & get_data() const{
			int num_elmts=0;
			for(int i=0;i<omp_get_num_procs();i++){
				num_elmts+=_data[i].size();
			}
			data.resize(num_elmts);
			int index=0;
			for(int i = 0;i<omp_get_num_procs();i++)
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
		mInterData.reset();
		mOutputData.reset();
		omp_set_num_threads(omp_get_num_procs());
		// map
		int numMapTasks=(mInputDataSize+mUnitSize-1)/mUnitSize;
		#pragma omp parallel for
		for(int i=0;i<numMapTasks;i++){
			int offset=mUnitSize*i;
			int len= mInputDataSize>(offset+mUnitSize)?mUnitSize:mInputDataSize-offset;
			map(mInputData+offset, len);
		}
		#pragma omp barrier
		const InterKeyValsMapT & interData=mInterData.get_data();
		std::vector<InterKeyValsT> interKeyVals(interData.size());
		int index=0;
		for(typename InterKeyValsMapT::const_iterator it=interData.begin(); it!=interData.end(); it++){
			interKeyVals[index++]=InterKeyValsT(it->first, &(it->second));
		}
		// reduce
		keyForThreads.clear();
		keyForThreads.resize(mNumReduceThreads);
		int numReduceTasks=interKeyVals.size();
		#pragma omp parallel for
		for(int i=0;i<numReduceTasks;i++){
			const InterKeyValsT & kvp=interKeyVals[i];
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
	InterMap mInterData;
	// output data
	OutputVector mOutputData;
	// auxiliary data for the threads
	vector<OutputKeyT> keyForThreads;
};

#endif
