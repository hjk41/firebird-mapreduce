#ifndef TIME_H
#define TIME_H

#include <sys/time.h>

double get_time(){
	timeval t;
	gettimeofday(&t, NULL);
	return (double)t.tv_sec+(double)t.tv_usec/1000000;
}

#endif
