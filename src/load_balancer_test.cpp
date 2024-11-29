#include "load_balancer.h"
#include "random.h"
#include "testlog.h"

#include "config.h"

#include <iostream>
#include <thread>
#include <stdio.h>
#include <unistd.h>
#include <mutex>
#include <cstring>
#include <assert.h>
#include <concepts>

using namespace std;

struct Input {
    size_t num_compute = 2; // --num_compute -nc [2, inf)
    size_t num_shard_per_compute = 8; // --num_shard_per_compute -nspc [1, inf)
    size_t key_lb = 0; // --key_lb -klb // should be a power of two or 0?
    size_t key_log_ub = 16; // --key_log_ub -klub 2^key_log_ub should be more than 0 -> was 26 before
    size_t key_ub = (1ull << key_log_ub);
    size_t send_info_delay_time = 100000; // --send_info_delay_time -sidt
    size_t per_round_delay = 10000; // --per_round_delay -prd [1, inf) 0 means no delay
    size_t per_round_delay_time = 10; // --per_round_delay_time -prdt
    size_t random_seed = 65406; // --random_seed -rs 
    size_t rw_p = 25; // --read_write_percent -rwp [0, 100]
    size_t remote_read_per_read = 100; // --remote_read_per_read -rrpr 0 means no remote reads
    size_t flush_per_write = 1000; // --flush_per_write -fpw 0 means no flushes
    size_t print_delay_seconds = 1; // --print_delay_seconds -pds [1, inf) 
    size_t print_per_round = 5; // --print_per_round -ppr [1, inf)
    size_t local_read_time = 1; // --local_read_time -lrt 
    size_t remote_read_time = 10; // --remote_read_time -rert 
    size_t local_write_time = 1; // --local_write_time -lwt
    size_t flush_time = 100; // --flush_time -ft
    size_t min_shard_size = 1; // --min_shard_size -mss [1, inf), determins minimum number of keys in a shard -> was 1024 before

    size_t rebalance_period_seconds = 15; // --rebalance_period_seconds -rps
    size_t load_imbalance_ratio = 100; // --load_imbalance_ratio -lir
    size_t low_load_thresh = 0; // --low_load_thresh -llt

    size_t num_nodes_to_print = 0; // --num_nodes_to_print -nnp 0 means all nodes should be printed
    size_t num_shards_to_print = 0; // --num_shards_to_print -nstp 0 means all shards should be printed
    size_t num_shards_to_print_per_compute_node = 0; // --num_shards_to_print_per_compute_node -nstpcn 0 means all shards should be printed

    char lb_type = 'f'; // --lb_type -lt [f, d, r] f: fixed, d: dynamic, r: dynamic restricted
};

Input input;
#ifdef PRINTER_LOCK
std::shared_mutex print_mtx;
#endif

void print_input() {
    LOGF(stdout, "Input:\n\
        load_balancing_type: %s\n\
        num_compute: %lu\n\
        num_shard_per_compute: %lu\n\
        key_lb: %lu\n\
        key_log_ub: %lu\n\
        key_ub: %lu\n\
        send_info_delay_time: %lu\n\
        per_round_delay: %lu\n\
        per_round_delay_time: %lu\n\
        random_seed: %lu\n\
        rw_p: %lu\n\
        remote_read_per_read: %lu\n\
        flush_per_write: %lu\n\
        print_delay_seconds: %lu\n\
        print_per_round: %lu\n\
        local_read_time: %lu\n\
        remote_read_time: %lu\n\
        local_write_time: %lu\n\
        flush_time: %lu\n\
        min_shard_size: %lu\n\
        rebalance_period_seconds: %lu\n\
        load_imbalance_ratio: %lu\n\
        low_load_thresh: %lu\n\
        num_nodes_to_print: %lu\n\
        num_shards_to_print: %lu\n\
        num_shards_to_print_per_compute_node: %lu\n", 
        input.lb_type == 'f' ? "fixed" : input.lb_type == 'd' ? "dynamic" : "dynamic restricted",
        input.num_compute, input.num_shard_per_compute, input.key_lb, input.key_log_ub, input.key_ub, input.send_info_delay_time, input.per_round_delay, 
        input.per_round_delay_time, input.random_seed, input.rw_p, input.remote_read_per_read, input.flush_per_write, input.print_delay_seconds, 
        input.print_per_round, input.local_read_time, input.remote_read_time, input.local_write_time, input.flush_time, input.min_shard_size, input.rebalance_period_seconds, 
        input.load_imbalance_ratio, input.low_load_thresh, input.num_nodes_to_print, input.num_shards_to_print, 
        input.num_shards_to_print_per_compute_node);
}

void help() {
    LOGF(stderr, "USAGE: ./load_balancer_test [OPTIONS]\n\
        \tOPTIONS:\n\
            \t\t--help, -h -> for printing this message\n\
            \t\t--lb_type=<type>, -lt=<type> -> sets the load balancing type. type should be one of [f, d, r]. default value is f.\n\
            \t\t--num_compute=<number>, -nc=<number> -> sets the number of compute nodes to <number>. should be at least 2. default value is 2.\n\
            \t\t--num_shard_per_compute=<number>, -nspc=<number> -> sets the number of shards per compute node to <number>. cannot be 0. default value is 8.\n\
            \t\t--key_lb=<number>, -klb=<number> -> sets the lower bound of the key range. default value is 0.\n\
            \t\t--key_log_ub=<number>, -klub=<number> -> sets the upper bound of the key range as 2^<number>. default value is 16.\n\
            \t\t--send_info_delay_time=<number>, -sidt=<number> -> sets the delay time for sending info in microseconds. default value is 100000.\n\
            \t\t--per_round_delay=<number>, -prd=<number> -> per each <number> rounds, a delay will happen in the load generator. 0 means no delay. default value is 10000.\n\
            \t\t--per_round_delay_time=<number>, -prdt=<number> -> sets the delay time per load generator round in microseconds. default value is 10.\n\
            \t\t--random_seed=<number>, -rs=<number> -> sets the random seed. default value is 65406.\n\
            \t\t--read_write_percent=<number>, -rwp=<number> -> sets the read-write percent(0 means that all queries are writes). should be in range [0, 100]. default value is 25.\n\
            \t\t--remote_read_per_read=<number>, -rrpr=<number> -> does one remote reads after <number> local reads. default value is 100.\n\
            \t\t--flush_per_write=<number>, -fpw=<number> -> sets the flush per write. default value is 1000.\n\
            \t\t--print_delay_seconds=<number>, -pds=<number> -> sets the print delay in seconds. cannot be 0. default value is 1.\n\
            \t\t--print_per_round=<number>, -ppr=<number> -> sets the print per round. cannot be 0. default value is 5.\n\
            \t\t--local_read_time=<number>, -lrt=<number> -> sets the local read time. default value is 1.\n\
            \t\t--remote_read_time=<number>, -rert=<number> -> sets the remote read time. default value is 10.\n\
            \t\t--local_write_time=<number>, -lwt=<number> -> sets the local write time. default value is 1.\n\
            \t\t--flush_time=<number>, -ft=<number> -> sets the flush time. default value is 100.\n\
            \t\t--min_shard_size=<number>, -mss=<number> -> sets the minimum shard size. default value is 1 cannot be 0.\n\
            \t\t--rebalance_period_seconds=<number>, -rps=<number> -> sets the rebalance period in seconds. default value is 15.\n\
            \t\t--load_imbalance_ratio=<number>, -lir=<number> -> sets the load imbalance ratio. load imbalance threshold is mean_load / ratio. default value is 100.\n\
            \t\t--low_load_thresh=<number>, -llt=<number> -> sets the low load threshold to determine insignificant loads. default value is 0.\n\
            \t\t--num_nodes_to_print=<number>, -nnp=<number> -> sets the number of nodes to print. 0 means all nodes should be printed. default is 0.\n\
            \t\t--num_shards_to_print=<number>, -nstp=<number> -> sets the number of shards to print. 0 means all shards should be printed. default is 0.\n\
            \t\t--num_shards_to_print_per_compute_node=<number>, -nstpcn=<number> -> sets the number of shards to print per compute node. 0 means all shards should be printed. default is 0.\n\
        \t<number>: is a non-negative integer\n");
        
}

bool get_arg(const char* arg, const char* arg_name, size_t& res) {
    size_t len = strlen(arg_name);
    assert(len > 0 && arg_name[len - 1] == '=');

    if (strlen(arg) <= len)
        return false;

    size_t num_len = strlen(arg + len);

    if (!strncmp(arg, arg_name, len)) {
        if (num_len == 0) {
            throw std::invalid_argument("no number after argument " + std::string(arg_name, len - 1));
        }
        
        // if (arg[len] == 'd' && num_len == 1) {
        //     return true;
        // }

        if (strspn(arg + len, "0123456789") == strlen(arg + len)) {
            res = atoi(arg + len);
        }
        else {
            throw std::invalid_argument("invalid number " + std::string(arg + len) + " after argument " + std::string(arg_name, len - 1));
        }
        return true;
    }
    return false;
}

bool get_arg(const char* arg, const char* arg_name, char& res) {
    size_t len = strlen(arg_name);
    assert(len > 0 && arg_name[len - 1] == '=');

    if (strlen(arg) <= len)
        return false;

    if (!strncmp(arg, arg_name, len)) {
        if (strlen(arg + len) != 1) {
            throw std::invalid_argument("invalid character " + std::string(arg + len) + " after argument " + std::string(arg_name, len - 1));
        }
        res = arg[len];
        return true;
    }
    return false;
}

void parse_input(int argc, char** argv) {
    try {
        assert(write_buffer_size != 0);
        --argc;
        for (; argc > 0; --argc) {
            if (get_arg(argv[argc], "--lb_type=", input.lb_type) 
                || get_arg(argv[argc], "-lt=", input.lb_type)) {
                if (input.lb_type != 'f' && input.lb_type != 'd' && input.lb_type != 'r') {
                    throw std::invalid_argument("load balancing type should be one of [f, d, r]");
                }
            }
            else if (get_arg(argv[argc], "--num_compute=", input.num_compute) 
                || get_arg(argv[argc], "-nc=", input.num_compute)) {
                if (input.num_compute < 2) {
                    throw std::invalid_argument("num_compute should be more than 1");
                }
            }
            else if (get_arg(argv[argc], "--num_shard_per_compute=", input.num_shard_per_compute) 
                || get_arg(argv[argc], "-nspc=", input.num_shard_per_compute)) {
                if (input.num_compute == 0) {
                    throw std::invalid_argument("num_shards_per_compute cannot be 0");
                }
            }
            else if (get_arg(argv[argc], "--key_lb=", input.key_lb) 
                || get_arg(argv[argc], "-klb=", input.key_lb)) {
                
            }
            else if (get_arg(argv[argc], "--key_log_ub=", input.key_log_ub) 
                || get_arg(argv[argc], "-klub=", input.key_log_ub)) {
                input.key_ub = (1ull << input.key_log_ub);
            }
            else if (get_arg(argv[argc], "--send_info_delay_time=", input.send_info_delay_time) 
                || get_arg(argv[argc], "-sidt=", input.send_info_delay_time)) {
                
            }
            else if (get_arg(argv[argc], "--per_round_delay=", input.per_round_delay) 
                || get_arg(argv[argc], "-prd=", input.per_round_delay)) {
                
            }
            else if (get_arg(argv[argc], "--per_round_delay_time=", input.per_round_delay_time) 
                || get_arg(argv[argc], "-prdt=", input.per_round_delay_time)) {
                
            }
            else if (get_arg(argv[argc], "--random_seed=", input.random_seed) 
                || get_arg(argv[argc], "-rs=", input.random_seed)) {
                
            }
            else if (get_arg(argv[argc], "--read_write_percent=", input.rw_p) 
                || get_arg(argv[argc], "-rwp=", input.rw_p)) {
                if (input.rw_p > 100) {
                    throw std::invalid_argument("read-write percent should be in range [0, 100]");
                }
            }
            else if (get_arg(argv[argc], "--remote_read_per_read=", input.remote_read_per_read) 
                || get_arg(argv[argc], "-rrpr=", input.remote_read_per_read)) {
                
            }
            else if (get_arg(argv[argc], "--flush_per_write=", input.flush_per_write) 
                || get_arg(argv[argc], "-fpw=", input.flush_per_write)) {
                
            }
            else if (get_arg(argv[argc], "--min_shard_size=", input.min_shard_size) 
                || get_arg(argv[argc], "-mss=", input.min_shard_size)) {
                if (input.min_shard_size == 0) {
                    throw std::invalid_argument("min_shard_size cannot be 0");
                }
            }
            else if (get_arg(argv[argc], "--print_delay_seconds=", input.print_delay_seconds) 
                || get_arg(argv[argc], "-pds=", input.print_delay_seconds)) {
                if (input.print_delay_seconds == 0) {
                    throw std::invalid_argument("print_delay_seconds cannot be 0");
                }
            }
            else if (get_arg(argv[argc], "--print_per_round=", input.print_per_round) 
                || get_arg(argv[argc], "-ppr=", input.print_per_round)) {
                if (input.print_per_round == 0) {
                    throw std::invalid_argument("print_per_round cannot be 0");
                }
            }
            else if (get_arg(argv[argc], "--local_read_time=", input.local_read_time) 
                || get_arg(argv[argc], "-lrt=", input.local_read_time)) {
                
            }
            else if (get_arg(argv[argc], "--remote_read_time=", input.remote_read_time) 
                || get_arg(argv[argc], "-rert=", input.remote_read_time)) {
                
            }
            else if (get_arg(argv[argc], "--local_write_time=", input.local_write_time) 
                || get_arg(argv[argc], "-lwt=", input.local_write_time)) {
                
            }
            else if (get_arg(argv[argc], "--flush_time=", input.flush_time) 
                || get_arg(argv[argc], "-ft=", input.flush_time)) {
                
            }
            else if (get_arg(argv[argc], "--rebalance_period_seconds=", input.rebalance_period_seconds) 
                || get_arg(argv[argc], "-rps=", input.rebalance_period_seconds)) {
                
            }
            else if (get_arg(argv[argc], "--load_imbalance_ratio=", input.load_imbalance_ratio) 
                || get_arg(argv[argc], "-lir=", input.load_imbalance_ratio)) {
            }
            else if (get_arg(argv[argc], "--low_load_thresh=", input.low_load_thresh) 
                || get_arg(argv[argc], "-llt=", input.low_load_thresh)) {
                
            }
            else if (get_arg(argv[argc], "--num_nodes_to_print=", input.num_nodes_to_print) 
                || get_arg(argv[argc], "-nnp=", input.num_nodes_to_print)) {
                
            }
            else if (get_arg(argv[argc], "--num_shards_to_print=", input.num_shards_to_print) 
                || get_arg(argv[argc], "-nstp=", input.num_shards_to_print)) {
                
            }
            else if (get_arg(argv[argc], "--num_shards_to_print_per_compute_node=", input.num_shards_to_print_per_compute_node) 
                || get_arg(argv[argc], "-nstpcn=", input.num_shards_to_print_per_compute_node)) {
                
            }
            else {
                throw std::invalid_argument("Unknown argument: " + std::string(argv[argc]));
            }
        }
    } catch(std::exception& a) {
        LOGFC(COLOR_RED, stderr, "%s\n", a.what());
        help();
        #ifdef PRINT_INPUT
        print_input();
        #endif
        exit(1);
    }

    #ifdef PRINT_INPUT
    print_input();
    #endif
}

size_t key_to_shard(size_t key, size_t num_shards) {
    size_t num_keys = input.key_ub - input.key_lb;
    size_t remainder = num_keys % num_shards;
    size_t shard_size = num_keys / num_shards + (remainder != 0);
    size_t shard = key / shard_size;
    if (remainder != 0 && shard >= remainder) {
        shard = (key - remainder * shard_size) / (shard_size-1) + remainder;
    }
    return shard;
}

void send_info(load_vector& loads, TimberSaw::Load_Balancer& lb) {
    while(true) {
        usleep(input.send_info_delay_time);
        #ifdef PRINTER_LOCK
        print_mtx.lock_shared();
        #endif

        loads.flush();

        #ifdef PRINTER_LOCK
        print_mtx.unlock_shared();
        #endif
    }
}

void load_generator(TimberSaw::Load_Balancer& lb, load_vector& loads) {
    
    TimberSaw::Random32 type_gen(input.random_seed);
    // TimberSaw::Random64 key_gen(input.random_seed);
    TimberSaw::Random32 remote_gen(input.random_seed);
    TimberSaw::zipf_distribution<size_t> key_gen(input.random_seed, input.key_ub, 1.0);

    for (int round = 0;; ++round) {
        if (input.per_round_delay != 0 && round % input.per_round_delay == 0)
            usleep(input.per_round_delay_time);


        // size_t key = key_gen.Skewed(input.key_log_ub);
        size_t key = key_gen() - 1;
        assert(key < input.key_ub && key >= input.key_lb);

        size_t shard = key_to_shard(key, lb.num_shards());

        size_t lr = 0, rr = 0, lw = 0, fl = 0;
        if (type_gen.Uniform(100) < input.rw_p) {
            lr = 1;
            if (input.remote_read_per_read != 0)
                rr = ((remote_gen.Next() % input.remote_read_per_read) == 1);
        }
        else {
            lw = 1;
            if (input.flush_per_write != 0)
               fl = ((remote_gen.Next() % input.flush_per_write) == 1);
        }
        
        #ifdef PRINTER_LOCK
        print_mtx.lock_shared();
        #endif
        loads.increment_load(key, lr, rr, lw, fl);
        #ifdef PRINTER_LOCK
        print_mtx.unlock_shared();
        #endif
    }
}

void printer(TimberSaw::Load_Balancer& lb
    , size_t num_nodes_to_print = 0, size_t num_shards_to_print = 0, size_t num_shards_to_print_per_compute_node = 0) {

    TimberSaw::Random64 num_gen(input.random_seed);
    char* buffer = new char[write_buffer_size];

    for (int round = 0;; ++round) {
        sleep(input.print_delay_seconds);
        #ifdef PRINTER_LOCK
        print_mtx.lock();
        lb.pause();
        #endif
        #ifdef PRINT_COLORED
        sprintf(buffer + strlen(buffer), COLOR_BLUE "round: %d" COLOR_RESET "\n", round);
        #else
        sprintf(buffer + strlen(buffer), "round: %d\n", round);
        #endif
        if (round % input.print_per_round == 0) {
            lb.print(buffer, num_nodes_to_print, num_shards_to_print, num_shards_to_print_per_compute_node);
        }

        LOGF(stdout, "%s", buffer);
        #ifdef PRINTER_LOCK
        lb.resume();
        print_mtx.unlock();
        #endif
        memset(buffer, 0, write_buffer_size);
    }

    delete[] buffer;
}

int main(int argc, char **argv) {
    if (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-h"))) {
        help();
        return 0;
    }

    parse_input(argc, argv);
    TimberSaw::Load_Balancer* lb;
    if (input.lb_type == 'f') {
        lb = new TimberSaw::Fixed_Load_Balancer(input.num_compute, input.num_shard_per_compute
            , input.rebalance_period_seconds, input.load_imbalance_ratio, input.low_load_thresh);
    }
    else if (input.lb_type == 'd') {
        lb = new TimberSaw::Dynamic_Load_Balancer(input.num_compute, input.num_shard_per_compute
            , input.rebalance_period_seconds, input.load_imbalance_ratio, input.low_load_thresh);
    }
    else {
        // throw std::invalid_argument("not implemented");
        lb = new TimberSaw::Dynamic_Restricted_Load_Balancer(input.num_compute, input.num_shard_per_compute
            , input.rebalance_period_seconds, input.load_imbalance_ratio, input.low_load_thresh);
    }
    
    load_vector loads(input.key_lb, input.key_ub, *lb
        , input.local_read_time, input.remote_read_time, input.local_write_time, input.flush_time, input.min_shard_size);
    lb->set_vector(loads);

    std::thread t1(printer, std::ref(*lb)
        , input.num_nodes_to_print, input.num_shards_to_print, input.num_shards_to_print_per_compute_node);
    std::thread t2(send_info, std::ref(loads), std::ref(*lb));
    // for (size_t i = 0; i < input.num_compute * input.num_shard_per_compute; ++i) {
    //     // std::cout << i << " hi\n";
    //     loads[i].shard_id = i;
    // }
    std::thread t3(load_generator, std::ref(*lb), std::ref(loads));
    
    lb->start();

}