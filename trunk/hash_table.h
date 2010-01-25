#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <list>
#include "firebird_ma.h"

template<class T>
class firebird_list{
public:
	class iterator{
	public:
		iterator():curr(NULL){};
		iterator(list_node * n):curr(n){};
		iterator(const iterator & it):curr(it.curr){};
		~iterator(){};
		iterator operator ++(){curr=curr->next;return *this;};
		iterator operator ++(int){iterator it=*this; curr=curr->next; return it;};
		T & operator *(){return *curr;};
		T * operator ->(){return curr;};
	private:
		list_node * curr;
	};
private:
	struct list_node{
		T data;
		list_node * next;

		list_node():next(NULL){};
		list_node(const T & d, list_node * n):data(d),next(n){};

		static void * operator new(size_t size){
			return firebird_ma(size);
		}
		static void operator delete(void * ptr, size_t size){};
	};
	list_node * head;
public:
	firebird_list():head(NULL){};
	~firebird_list(){
		for(list_node * node=head; node!=NULL; node=node->next)
			delete node;
	};

	void insert(const T & data){
		struct * new_node=new list_node(data, head);
		head=new_node;
	}
	void splice(firebird_list & rhs){
		list_node ** end=&head;
		while(*end!=NULL)
			end=&(end->next);
		*end=rhs.head;
		rhs.head=NULL;
	}
};

template<class KeyT, class ValT, typename HashT=default_hash<KeyT>>
class firebird_hash_table{
public:
	typedef firebird_list<ValT> ValList;
	struct KeyValues{
		KeyT key;
		ValList value_list;
	};
	typedef firebird_list<KeyValues> KeyList;
	typedef KeyList Bucket;
public:
	HashT hash;
	Bucket * buckets;
	unsigned int num_buckets;

	hash_table():buckets(0),num_buckets(0){};
	~hash_table(){
		delete[] buckets;
	}

	void insert(const KeyT & key, const ValT & val){
		unsigned int 
	}
};

#endif
