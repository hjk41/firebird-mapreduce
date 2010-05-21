Firebird: simple and naiive implementation of MapReduce for shared-memory machines
							chuntao.hong@gmail.com



INTRODUCTION

  "Firebird is much easier to catch than Pheonix."

  We design Firebird with the ease of programming as the main subject. It is 
not designed for efficiency, but for demonstration of the MapReduce programming
model. We hope that through using Firebird, students can learn to express the
algorithms using the MapReduce model, so that they will be able to write 
real-world MapReduce code once they need to.
  If you need efficiency, please refer to MapCG, a much more efficient 
implementation:
  https://sourceforge.net/projects/mapp/



WRITING CODE WITH FIREBIRD

  Firebird is written in C++. In order to use Firebird, programmer should follow
these steps:
	1. include the template header file "firebird.h"
	2. derive a class from MapReduceScheduler
	3. override the map(), reduce() functions
	4. create a instance for the derived class
	5. set the input data
	6. set the parameters if needed
	7. invode run(), and run the map, reduce tasks
  When the MapReduce framework finishes working, it will put the output data in
a vector<MapReduceScheduler::KeyValT>, which can be obtained by calling 
get_data().



COMPILING CODE
    
  Firebird uses OpenMP to implement multi-threading. A compiler with OpenMP 
support is required to compile the code written with Firebird. Suppose you have
written you code in main.cpp, which includes firebird.h. Then you can compile 
you code like this:
	with Intel C++ Compiler:
		icpc main.cpp -openmp
	with GCC:
		g++ main.cpp -fopenmp



LIMITATIONS

  Firebird does not support splitter() or partition() or merge() yet. The input
to the framework should be an array. The output will not be ordered by the key. 
Instead, the output is organized by random order, depending on the order of the
thread execution. So you may get different order at different runs. We do plan
to implement merge() in the future. But for now, you should sort the result 
yourself if you need ordered result.



CONTACT US

  If you have any question or suggestions regarding Firebird, feel free to 
contact us at: chuntao.hong@gmail.com.

