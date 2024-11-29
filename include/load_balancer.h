//
// Created by arman on 7/23/24.
//

#ifndef TimberSaw_LOAD_BALANCER_H
#define TimberSaw_LOAD_BALANCER_H

#include "load_info_container.h"
#include <atomic>
#include <mutex>
#include <memory>

#include <iostream>
#include <cstring>

#include "config.h"

struct load_vector;

namespace TimberSaw {

class Load_Balancer {
public:
    virtual void start() = 0; // runs a thread which periodically does load balancing and then sleeps
    // void new_bindings(); // returns a new ownership map -> will replace compute_node_info when ready
    void shut_down();
    virtual void set_up_new_plan() = 0; // gets a new optimal plan and executes a protocol to make sure things are running. -> run by load_balancer or main thread?

    // void rewrite_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes);
    // void increment_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes);
    void increment_load_info(size_t shard, size_t added_load);

    #ifdef PRINTER_LOCK
    void pause() {
        mtx.lock();
    }

    void resume() {
        mtx.unlock();
    }
    #endif

    size_t num_shards() {
        return container->num_shards();
    }

    size_t num_compute() {
        return container->num_compute();
    }

    void set_vector(load_vector& _lv) {
        lv = &_lv;
    }

    void print(char* buffer, size_t num_nodes_to_print, size_t num_shards_to_print, size_t num_shards_to_print_per_compute_node) {
        #ifdef PRINT_COLORED
        sprintf(buffer + strlen(buffer), COLOR_CYAN "num total shards: %lu" COLOR_RESET "\n", container->num_shards());
        #else
        sprintf(buffer + strlen(buffer), "num total shards: %lu\n", container->num_shards());
        #endif
        #ifdef PRINT_NODE_INFO
        #ifdef PRINT_COLORED
        sprintf(buffer + strlen(buffer), COLOR_YELLOW "node info:" COLOR_RESET"\n");
        #else
        sprintf(buffer + strlen(buffer), "node info:\n");
        #endif
        for(size_t node = 0; node < container->num_compute() && (num_nodes_to_print == 0 || node < num_nodes_to_print); ++node) {
            (*container)[node].print(buffer, num_shards_to_print_per_compute_node);
        }
        sprintf(buffer + strlen(buffer), "\n");
        #endif

        #ifdef PRINT_SHARD_INFO
        #ifdef PRINT_NODE_INFO
        sprintf(buffer + strlen(buffer), "\n");
        #endif
        sprintf(buffer + strlen(buffer), "shard info:\n");
        for(size_t shard = 0, counter = 0; shard < container->num_shards() && (num_shards_to_print == 0 || counter < num_shards_to_print); shard = container->shard_id(shard).next_id(), ++counter) {
            container->shard_id(shard).print(buffer);
        }
        #endif

        #ifdef ANALYZE
        container->new_round();
        #endif

        #if defined(PRINT_NODE_INFO) || defined(PRINT_SHARD_INFO)
        sprintf(buffer + strlen(buffer), "_____________________________________________________\n");
        #endif
    }

    // void set_num_shards_to_print_per_compute_node(size_t num_shards_to_print_per_compute_node) {
    //     container->num_shards_to_print_per_compute_node = num_shards_to_print_per_compute_node;
    // }
    
    // functions for updating load info per shard and node

protected:

    Load_Balancer(Load_Info_Container_Base* _container
            , size_t _rebalance_period_seconds, size_t _load_imbalance_ratio);

    ~Load_Balancer();

    inline int check_load(size_t load, size_t mean_load) {
        if (load > mean_load && load - mean_load > load_imbalance_threshold_half) {
            return 2;
        }
        else if (load > mean_load && load - mean_load <= load_imbalance_threshold_half) {
            return 1;
        }
        else if (load == mean_load) {
            return 0;
        }
        else if (load < mean_load && mean_load - load <= load_imbalance_threshold_half) {
            return -1;
        }
        else {
            return -2;
        }
    }
    load_vector* lv = nullptr;
    Load_Info_Container_Base* container;
    std::atomic<bool> started;
    #ifdef PRINTER_LOCK
    std::mutex mtx;
    #endif

    size_t rebalance_period_seconds;
    size_t load_imbalance_threshold;
    size_t load_imbalance_threshold_half;
    size_t load_imbalance_ratio;
};


class Fixed_Load_Balancer : public Load_Balancer {
public:
    Fixed_Load_Balancer(size_t num_compute, size_t num_shards_per_compute
        , size_t _rebalance_period_seconds, size_t _load_imbalance_threshold, size_t low_load_threshold);

    ~Fixed_Load_Balancer();
    void start(); // runs a thread which periodically does load balancing and then sleeps
    void set_up_new_plan();
};

class Dynamic_Load_Balancer : public Load_Balancer {
public:
    Dynamic_Load_Balancer(size_t num_compute, size_t num_shards_per_compute
        , size_t _rebalance_period_seconds, size_t _load_imbalance_threshold, size_t low_load_threshold);

    ~Dynamic_Load_Balancer();
    void start(); // runs a thread which periodically does load balancing and then sleeps
    void set_up_new_plan();
};

class Dynamic_Restricted_Load_Balancer : public Load_Balancer {
public:
    Dynamic_Restricted_Load_Balancer(size_t num_compute, size_t num_shards_per_compute
        , size_t _rebalance_period_seconds, size_t _load_imbalance_threshold, size_t low_load_threshold);

    ~Dynamic_Restricted_Load_Balancer();
    void start(); // runs a thread which periodically does load balancing and then sleeps
    void set_up_new_plan();

    void push_load_left(size_t node_idx, size_t load, size_t mean_load);
    void push_load_right(size_t node_idx, size_t load, size_t mean_load);
    // void pull_load_left(size_t node_idx, size_t load, size_t mean_load);
    // void pull_load_right(size_t node_idx, size_t load, size_t mean_load);

private:
    std::vector<std::pair<size_t, size_t>> lr_load;

    void get_load_req(size_t node_idx, size_t nl, size_t nr
        , size_t node_load, size_t left_load, size_t right_load, bool is_max
        , size_t& left_load_req, size_t& right_load_req);
};

}

struct load_batch {
    std::unique_ptr<std::atomic<size_t>> num_reads;
    std::unique_ptr<std::atomic<size_t>> num_writes;
    std::unique_ptr<std::atomic<size_t>> num_r_reads;
    std::unique_ptr<std::atomic<size_t>> num_flushes;

    size_t next;
    size_t lb;
    size_t ub;

    load_batch(size_t _lb, size_t _ub, size_t _next) : lb(_lb), ub(_ub), next(_next)
        , num_reads(new std::atomic<size_t>(0)), num_writes(new std::atomic<size_t>(0)), num_r_reads(new std::atomic<size_t>(0))
        , num_flushes(new std::atomic<size_t>(0)) //, shard_id(_id)
    {}

    // Move constructor
    load_batch(load_batch&& other) noexcept
        : num_reads(std::move(other.num_reads)), num_writes(std::move(other.num_writes)),
          num_r_reads(std::move(other.num_r_reads)), num_flushes(std::move(other.num_flushes)),
          lb(other.lb), ub(other.ub), next(other.next) {}

    // Move assignment operator
    load_batch& operator=(load_batch&& other) noexcept {
        if (this != &other) {
            num_reads = std::move(other.num_reads);
            num_writes = std::move(other.num_writes);
            num_r_reads = std::move(other.num_r_reads);
            num_flushes = std::move(other.num_flushes);
            lb = other.lb;
            ub = other.ub;
            next = other.next;
        }
        return *this;
    }

    // Delete copy constructor and copy assignment operator
    load_batch(const load_batch&) = delete;
    load_batch& operator=(const load_batch&) = delete;
};

class load_vector {
public:
    load_vector(size_t lbound, size_t ubound, TimberSaw::Load_Balancer& _lb
        , size_t lr_time, size_t rr_time, size_t lw_time, size_t fl_time
        , size_t minimum_shard_size) 
        : lb(_lb), local_read_time(lr_time), remote_read_time(rr_time), local_write_time(lw_time), flush_time(fl_time)
            , last(lb.num_shards()-1), min_shard_size(minimum_shard_size)
            #ifdef DEBUG
            , lower_bound(lbound), upper_bound(ubound)
            #endif
             {
        assert(lbound < ubound);
        size_t remainder = (ubound - lbound) % lb.num_shards(); // 3936
        size_t shard_size = (ubound - lbound) / lb.num_shards() + (remainder != 0); // 11 + 1 = 12
        assert(shard_size >= min_shard_size);
        size_t hload = lbound + shard_size;
        loads.reserve(lb.num_shards());
        for (size_t i = 0; i < lb.num_shards(); ++i) {
            loads.emplace_back(lbound, hload, i+1);
            ub_to_index[hload] = i;
            lbound = hload;
            if (remainder > 0) {
                --remainder;
                shard_size = shard_size - (remainder == 0);
                assert(shard_size >= min_shard_size);
            }
            hload += shard_size;
        }

        assert(lbound == ubound && hload == ubound + shard_size);

        #ifdef DEBUG
        for (size_t i = 0; i < loads.size(); i = loads[i].next) {
            assert(i == last || loads[i].ub == loads[loads[i].next].lb);
            assert(ub_to_index[loads[i].ub] == i);
            LOGF(stdout, "shard %lu: %lu ~ %lu :: %lu\n", i, loads[i].lb, loads[i].ub, loads[i].ub - loads[i].lb);
        }
        assert(loads[0].lb == lower_bound && loads[last].ub == upper_bound);
        #endif
    }

    void increment_load(size_t key, size_t lr, size_t rr, size_t lw, size_t fl) {
        std::shared_lock<std::shared_mutex> lock(mtx);
        auto itr = ub_to_index.upper_bound(key);
        assert(itr != ub_to_index.end());

        loads[itr->second].num_reads->fetch_add(lr);
        loads[itr->second].num_r_reads->fetch_add(rr);
        loads[itr->second].num_writes->fetch_add(lw);
        loads[itr->second].num_flushes->fetch_add(fl);
    }

    void flush() {
        std::shared_lock<std::shared_mutex> lock(mtx);
        flush_wo_lock();
    }

    void flush(size_t id) {
        std::shared_lock<std::shared_mutex> lock(mtx);
        flush_wo_lock(id);
    }

    // must be followed by a finish_signal
    size_t divide_signal(size_t id, size_t num) {
        assert(num > 1);
        assert(id < loads.size());

        mtx.lock();
        flush_wo_lock(id);
        

        size_t lbound = loads[id].lb;
        size_t ubound = loads[id].ub;
        assert(ubound - lbound >= min_shard_size);
        size_t max_divide = (ubound - lbound) / min_shard_size;

        if (max_divide == 1) {
            return 1;
        }
        else if (max_divide < num) {
            num = max_divide;
        }

        #ifdef PRINT_COLORED
        LOGFC(COLOR_RED,stdout, "divide signal: node %lu to %lu shards\n", id, num);
        #else
        LOGF(stdout, "divide signal: node %lu to %lu shards\n", id, num);
        #endif

        // 18 / 4 = 4, 2
        // 18 key, 4 shard: 0[5, 23) -> 0[5, 10), 1[10, 15), 2[15, 19), 3[19, 23)
        size_t remainder = (ubound - lbound) % num; // 23 - 5 = 18, 18 % 4 = 2
        size_t shard_size = (ubound - lbound) / num + (remainder != 0); // 18 / 4  + 1 = 5
        lbound += shard_size; // 5 + 5 = 10
        if (remainder > 0) {
            --remainder;
            shard_size -= (remainder == 0);
        }
        size_t hload = lbound + shard_size; // 10 + 5 = 15

        loads[id].ub = lbound;
        ub_to_index[lbound] = id;

        loads.reserve(loads.size() + num - 1);
        size_t n = loads.size() - 1;
        for (size_t i = 1; i < num; ++i) {
            loads.emplace_back(lbound, hload, n + i + 1);
            ub_to_index[hload] = i + n;
            lbound = hload; // 15
            if (remainder > 0) {
                --remainder;
                shard_size -= (remainder == 0);
            }
            hload += shard_size; // 19
        }
        if (id != last) {
            loads.back().next = loads[id].next;
            loads[last].next = n + num;
            loads[id].next = n + 1;
        }
        else {
            last = n + num - 1;
        }

        #ifdef DEBUG
        for (size_t i = 0; i < loads.size(); i = loads[i].next) {
            assert(i == last || loads[i].ub == loads[loads[i].next].lb);
            assert(ub_to_index[loads[i].ub] == i);
            assert(loads[i].ub - loads[i].lb >= min_shard_size);
            LOGF(stdout, "shard %lu: %lu ~ %lu :: %lu\n", i, loads[i].lb, loads[i].ub, loads[i].ub - loads[i].lb);
        }
        assert(loads[0].lb == lower_bound && loads[last].ub == upper_bound);
        #endif

        return num;
    }

    // must be followed by a finish_signal
    template<std::convertible_to<std::pair<size_t, size_t>>... Args>
    void merge_range_signal(const std::pair<size_t, size_t>& ids, const Args&... id_pairs) {

        mtx.lock();
        merge_range_wo_lock(id_pairs...);
    }

    // must be followed by a finish_signal
    void merge_range_signal(const std::pair<size_t, size_t>& id_pair) {
        mtx.lock();
        merge_range_wo_lock(id_pair.first, id_pair.second);
    }

    // must be followed by a finish_signal
    void merge_range_signal(size_t from, size_t to) { // to is included
        mtx.lock();
        merge_range_wo_lock(from, to);
    }

    // must be followed by a finish_signal
    template<std::convertible_to<std::pair<size_t, size_t>>... Args>
    void merge_pair_signal(const std::pair<size_t, size_t>& ids, const Args&... id_pairs) {

        mtx.lock();
        merge_pair_wo_lock(ids.first, ids.second);
        merge_pair_wo_lock(id_pairs...);
    }

    // must be followed by a finish_signal
    void merge_pair_signal(const std::pair<size_t, size_t>& id_pair) {
        mtx.lock();
        merge_pair_wo_lock(id_pair.first, id_pair.second);
    }

    // must be followed by a finish_signal
    void merge_pair_signal(size_t first, size_t second) {
        mtx.lock();
        merge_pair_wo_lock(first, second);
    }

    // must only be used after a merge or divide signal
    void finish_signal() {
        mtx.unlock();
    }


private:
    std::map<size_t, size_t> ub_to_index;
    std::vector<load_batch> loads;
    std::shared_mutex mtx;
    TimberSaw::Load_Balancer& lb;
    size_t local_read_time, remote_read_time, local_write_time, flush_time;
    size_t last;
    size_t min_shard_size;
    #ifdef DEBUG
    size_t lower_bound, upper_bound;
    #endif

    void flush_wo_lock() {
        for (size_t i = 0; i < loads.size(); ++i) {
            size_t added_load = *(loads[i].num_reads) * local_read_time 
                                + *(loads[i].num_r_reads) * remote_read_time 
                                + *(loads[i].num_writes) * local_write_time 
                                + *(loads[i].num_flushes) * flush_time;

            if (added_load > 0) {
                *(loads[i].num_reads) = 0;
                *(loads[i].num_r_reads) = 0;
                *(loads[i].num_writes)= 0;
                *(loads[i].num_flushes) = 0;
                lb.increment_load_info(i, added_load);
            }
        }
    }

    void flush_wo_lock(size_t id) {
        assert(id < loads.size());

        load_batch& load = loads[id];
        size_t added_load = *(load.num_reads) * local_read_time 
                            + *(load.num_r_reads) * remote_read_time 
                            + *(load.num_writes)* local_write_time 
                            + *(load.num_flushes) * flush_time;

        if (added_load > 0) {
            *(load.num_reads)= 0;
            *(load.num_r_reads) = 0;
            *(load.num_writes) = 0;
            *(load.num_flushes) = 0;
            lb.increment_load_info(id, added_load);
        }
    }

    void flush_wo_lock(size_t from_id, size_t to_id) {
        assert(from_id < to_id && to_id <= loads.size());

        for (size_t i = from_id; i < to_id; ++i) {
            size_t added_load = *(loads[i].num_reads) * local_read_time 
                                + *(loads[i].num_r_reads) * remote_read_time 
                                + *(loads[i].num_writes)* local_write_time 
                                + *(loads[i].num_flushes) * flush_time;

            if (added_load > 0) {
                *(loads[i].num_reads) = 0;
                *(loads[i].num_r_reads) = 0;
                *(loads[i].num_writes)= 0;
                *(loads[i].num_flushes) = 0;
                lb.increment_load_info(i, added_load);
            }
        }
    }

    template<std::convertible_to<std::pair<size_t, size_t>>... Args>
    void merge_range_wo_lock(const std::pair<size_t, size_t>& ids, const Args&... id_pairs) {
        merge_range_wo_lock(ids.first, ids.second);
        merge_range_wo_lock(id_pairs...);
    }

    void merge_range_wo_lock(const size_t& from, const size_t& to) {
        assert(from < to);
        assert(to < loads.size());
        flush_wo_lock(from, to);
        loads[from].ub = loads[to].ub;
        ub_to_index[loads[to].ub] = from;
        // for(size_t i = to + 1; i < loads.size(); ++i) {
        //     loads[i].shard_id -= to - from + 1;
        // }
        // #ifdef DEBUG
        for (size_t i = from; i < to; ++i) {
            assert(loads[i].ub == loads[i + 1].lb);
            ub_to_index.erase(loads[i].ub);
        }
        // #endif
        
        loads.erase(loads.begin() + from + 1, loads.begin() + to + 1);

    }

    template<std::convertible_to<std::pair<size_t, size_t>>... Args>
    void merge_pair_wo_lock(const std::pair<size_t, size_t>& ids, const Args&... id_pairs) {
        merge_pair_wo_lock(ids.first, ids.second);
        merge_pair_wo_lock(id_pairs...);
    }

    void merge_pair_wo_lock(const size_t& first, const size_t& second) {
        assert(first < second);
        assert(second < loads.size());
        assert(loads[first].ub == loads[second].lb);
        flush_wo_lock(first);
        flush_wo_lock(second);

        loads[first].ub = loads[second].ub;
        ub_to_index[loads[second].ub] = first;
        // for(size_t i = second + 1; i < loads.size(); ++i) {
        //     --loads[i].shard_id;
        // }
        loads.erase(loads.begin() + second);
        ub_to_index.erase(loads[first].ub);
        // implementation of merge_pair
    }
};

#endif