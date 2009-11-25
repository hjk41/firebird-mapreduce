#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <vector>
#include <assert.h>
using namespace std;

#include "firebird.h"
#include "time.h"

const float LONGEST_DIST=1<<30;

struct edge_t{
        edge_t(){};
        edge_t(unsigned int d, float w):dst(d),weight(w){};
        unsigned int dst;
        float weight;
};

struct src_dist_t{
	unsigned int src;
	float dist;
};

typedef src_dist_t InputDataT;
typedef unsigned int OutputKeyT;
typedef float MapOutputValT;
typedef float ReduceOutputValT;

class MyScheduler: public MapReduceScheduler<src_dist_t, OutputKeyT, MapOutputValT, ReduceOutputValT>{
public:
	virtual void map(const InputDataT * data, const unsigned int len){
		for(int i=0;i<len;i++){
			unsigned int src = data[i].src;
			edge_t * edges=graph+node_start[src];
			unsigned int num_edges=node_start[src+1]-node_start[src];
			for(int j=0;j<num_edges;j++){
				unsigned int dst=edges[j].dst;
				float w=edges[j].weight;
				float new_dist=dist[src]+w;
				if(new_dist<dist[dst]){
					emit_intermediate(dst, new_dist);
				}
			}
		}
	}

	virtual void reduce(const OutputKeyT & node, const MapOutputValIter & valBegin, const MapOutputValIter & valEnd){
		float min_dist=LONGEST_DIST;
		for(MapOutputValIter it=valBegin; it!=valEnd; it++){
			if(*it<min_dist)
				min_dist=*it;
		}
		dist[node]=min_dist;
		emit(min_dist);
	}

public:
	float * dist;
	edge_t * graph;
	unsigned int * node_start;
}; 


//--------------------------------------------------
//generate data
//--------------------------------------------------
bool get_graph(const char * filename, edge_t * & graph, unsigned int * & node_start, unsigned int & num_nodes, unsigned int & n_edges)
{
	assert(filename);
	ifstream in(filename);
	if(!in.good()){
		cout<<"error opening file "<<filename<<endl;
		return false;
	}

	unsigned int num_edges;
	in>>num_nodes>>num_edges;
	n_edges=num_edges*2;
	node_start=new unsigned int[num_nodes+1];
	graph=new edge_t[num_edges*2];

	unsigned int src, dst;
	float w;
	vector< vector<edge_t> > edges(num_nodes);
	for(unsigned int i=0;i<num_edges;i++){
		if(!in.good()){
			cout<<"file format not good."<<endl;
			return false;
		}
		in>>src>>dst>>w;
		edges[src].push_back(edge_t(dst,w));
		edges[dst].push_back(edge_t(src,w));
	}
	unsigned int index=0;
	for(unsigned int i=0;i<num_nodes;i++){
		vector<edge_t> & v=edges[i];
		node_start[i]=index;
		for(unsigned int j=0;j<v.size();j++){
			graph[index]=v[j];
			index++;
		}
	}
	node_start[num_nodes]=index;
	return true;
}

void dijkstra(const edge_t * graph, const unsigned int * node_start, unsigned int num_nodes, unsigned int num_edges, unsigned int src, float * dist){
	bool * closed=new bool[num_nodes];
	for(int i=0;i<num_nodes;i++){
		closed[i]=false;
		dist[i]=LONGEST_DIST;
	}
	dist[src]=0;

	unsigned int num_closed=0;
	while(num_closed<num_nodes){
		// find next mid
		unsigned int mid=src;
		float min_dist=LONGEST_DIST;
		for(int i=0;i<num_nodes;i++){
			if(!closed[i] && dist[i]<min_dist){
				mid=i;
				min_dist=dist[i];
			}
		}
		closed[mid]=true;
		num_closed++;
		// update neighbor node
		for(int idx=node_start[mid];idx<node_start[mid+1];idx++){
			unsigned int dst=graph[idx].dst;
			float w=graph[idx].weight;
			float new_dist=dist[mid]+w;
			if(!closed[dst])
				dist[dst]=new_dist<dist[dst]?new_dist:dist[dst];
		}
	}

	delete[] closed;
}


int main(int argc, char * argv[]){
double t1=get_time();
	if(argc!=3){
		cout<<"usage: "<<argv[0]<<" graph_file source"<<endl;
		return 1;
	}
	const char * input_file=argv[1];
	unsigned int source=atoi(argv[2]);

	edge_t * graph;
	unsigned int * node_start;
	unsigned int num_nodes;
	unsigned int num_edges;
	if(!get_graph(input_file, graph, node_start, num_nodes, num_edges)){
		return 1;
	}
	float * dist=new float[num_nodes];
	for(int i=0;i<num_nodes;i++){
		dist[i]=LONGEST_DIST;
	}
	dist[source]=0;

	MyScheduler s;
	s.graph=graph;
	s.node_start=node_start;
	s.dist=dist;
	s.set_unit_size(100);

	src_dist_t inp;
	inp.src=source;
	inp.dist=0;
	
double t2=get_time();
cout<<"init: "<<t2-t1<<endl;

	const src_dist_t * input=&inp;
	unsigned int input_size=1;
	do{
		s.set_input(input,input_size);
		s.run();
		const vector<MyScheduler::KeyValT> & output=s.get_output();
		if(output.size()==0)
			break;
		input=(const src_dist_t *)&output[0];
		input_size=output.size();
	}while(1);

	for(int i=0;i<num_nodes && i<10; i++){
		printf("(%d,%f)\n",i,dist[i]);
	}

	s.print_time();

double t3=get_time();
cout<<"mapreduce: "<<t3-t2<<endl;

double t4=get_time();
	float * dist_serial=new float[num_nodes];
	dijkstra(graph, node_start, num_nodes, num_edges, source, dist_serial);
	for(int i=0;i<10 && i<num_nodes;i++){
		printf("(%d,%f)\n",i,dist_serial[i]);
	}

//	cout<<dist_serial[341]<<endl;
double t5=get_time();
cout<<"dijkstra: "<<t5-t4<<endl;

	return 0;
}
