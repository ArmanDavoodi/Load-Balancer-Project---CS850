# Load-Balancer-Project---CS850
* Course: CS850 - Advanced Topics in Computer Architecture: System Support for Next-Generation Computer Architectures
* Term: Fall 2024
* Instructor: Sihang Liu 

This projects implements a load balancer for dLSM[1] for a multi compute node/single memory node platform as the final project for CS850. It contains three load balancing methods, and uses a simulator to test how each algorithm works theoretically. The simulation uses the random.h file of the leveldb[2] and adds a zipf-like distribution implementation from [3] stackoverflow discusstion to it.

## Build/Run
To build the code, you can use the make command which generates all necessary files in the build directory.
To run the code, execute load_balancer_test in the build directory. You can also change defualt configurations of the simulation by passing necessary arguments when executing the program.
To view all available options run: 

build/load_balancer_test -h

## Project Structure
The root directory has three(four if you build the program) folders, and the makefile.

In the Include folder, you can find:
* config.h: includes some configuration which allows enabling/disabling logging options. Do not comment out the PRINTER_LOCK!
* random.h: it is the random.h file implemented by leveldb[2]. The zipf-like distribution implementation[3] is also appended to this file.
* testlog.h: allows for colored logs.
* load_balancer_container.h: contains the declarations regarding a container for load info of shards and compute nodes used by the load balancers.
* load_balancer.h: contains the declarations of the load_balancers.

In the src directory you can find:
* load_balancer.cpp and load_info_container.cpp: implementations of their header files.
* load_balancer_test.cpp: the implementation of a simulator for testing different load_balancers.

Lastly, you can find the results of some of the runs in the results directory:
* the results/raw directory contains the raw output of the experiments.
* the results/table directory contains table data derived from the raw outputs.

## Refrences:
[1] R. Wang, J. Wang, P. Kadam, M. Tamer Özsu and W. G. Aref, "dLSM: An LSM-Based Index for Memory Disaggregation," 2023 IEEE 39th International Conference on Data Engineering (ICDE), Anaheim, CA, USA, 2023, pp. 2835-2849, doi: 10.1109/ICDE55515.2023.00217.

[2] https://github.com/google/leveldb

[3] https://stackoverflow.com/a/44154095
