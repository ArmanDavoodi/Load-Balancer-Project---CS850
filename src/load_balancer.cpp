//
// Created by arman on 8/11/24.
//

#include "load_balancer.h"
#include "testlog.h"
#include "config.h"

#include <unistd.h>
#include <algorithm>
#include <vector>
#include <assert.h>

namespace TimberSaw {

    void Load_Balancer::shut_down() {
        started.store(false);
    }

    // void Load_Balancer::rewrite_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes) {
    //     container.rewrite_load_info(shard, num_reads, num_writes, num_remote_reads, num_flushes);
    // }

    // void Load_Balancer::increment_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes) {
    //     container.increment_load_info(shard, num_reads, num_writes, num_remote_reads, num_flushes);
    // }
    
    void Load_Balancer::increment_load_info(size_t shard, size_t added_load) {
        container->increment_load_info(shard, added_load);
    }

    // functions for updating load info per shard and node

    Load_Balancer::Load_Balancer(Load_Info_Container_Base* _container
        , size_t _rebalance_period_seconds, size_t _load_imbalance_ratio) 
        : container(_container), started(false)
            , rebalance_period_seconds(_rebalance_period_seconds), load_imbalance_ratio(_load_imbalance_ratio)
            , load_imbalance_threshold(0), load_imbalance_threshold_half(0) {
                assert(container != nullptr);
        }


    Load_Balancer::~Load_Balancer() {
        delete container;
    }


    Fixed_Load_Balancer::Fixed_Load_Balancer(size_t num_compute, size_t num_shards_per_compute
            , size_t _rebalance_period_seconds, size_t __load_imbalance_ratio, size_t low_load_threshold) 
        : Load_Balancer(new Load_Info_Container(num_compute, num_shards_per_compute, low_load_threshold)
            , _rebalance_period_seconds, __load_imbalance_ratio) {}

    Fixed_Load_Balancer::~Fixed_Load_Balancer() {}

    void Fixed_Load_Balancer::start() {
        started.store(true);
        Load_Info_Container& container = dynamic_cast<Load_Info_Container&>(*this->container);
        while (started.load()) {
            sleep(rebalance_period_seconds);
            #ifdef PRINTER_LOCK
            mtx.lock();
            #endif


            size_t min_load;
            size_t max_load;
            size_t mean_load = 0, sum_load = 0;

            container.compute_load_and_pass(min_load, max_load, mean_load, sum_load);
            load_imbalance_threshold = mean_load / load_imbalance_ratio;
            load_imbalance_threshold_half = load_imbalance_threshold / 2;

            if (max_load - min_load <= load_imbalance_threshold) { // use a statistic of shards(like max shard or mean shard as threshold)
                #ifdef PRINTER_LOCK
                mtx.unlock();
                #endif
                continue;
            }

            int min_stat = check_load(container.min_node().load(), mean_load);
            int max_stat = check_load(container.max_node().load(), mean_load);
            // TODO add something that if we have outlier do some shard, we recompute mean for the other nodes and try to balance those
            while ((max_stat > 1 || min_stat < -1) && max_stat > -2 && min_stat < 2) { // loop on nodes
                Compute_Node_Info& max_node = container.max_node();
                Shard_Iterator& itr = max_node.ordered_iterator();

                while (itr.is_valid()) { // loop on shards
                    if (container.is_insignificant(*(itr.shard()))) {
                        break;
                    }

                    int hload_stat = check_load(max_node.load() - itr.shard()->load() - container.get_current_change(), mean_load);
                    int lload_stat = check_load(container.min_node().load() + itr.shard()->load(), mean_load);
                    if (hload_stat < -1 || lload_stat > 1) {
                        // it may be possible that continuing with this would result in better balance
                        // while both nodes still remain out of prefered range but keep in mind that
                        // ownership transfer increases the load of a shard. Therefore, the oposit may happen
                        // as well and transfer is not worth it here.
                        // In these cases, it is better to increase num shards. (which we cannot do in current design)
                        ++itr;
                        continue;
                    }
                    
                    assert(&container.max_node() == &max_node);
                    container.change_owner_from_max_to_min(itr.index());
                    ++itr;
                    if (hload_stat < 2 && lload_stat > -2) {
                        break;
                    }
                    
                }
                container.update_max_load();

                if (&max_node == &container.max_node()) {
                    container.ignore_max(sum_load, mean_load);
                }

                min_stat = check_load(container.min_node().load(), mean_load);
                max_stat = check_load(container.max_node().load(), mean_load);
            }
            set_up_new_plan();
            #ifdef PRINTER_LOCK
            mtx.unlock();
            #endif
        }
    }

    void Fixed_Load_Balancer::set_up_new_plan() {
        auto updates = container->apply();
        #ifdef PRINT_UPDATE_INFO
        char* buffer_2 = new char[write_buffer_size];
        memset(buffer_2, 0, write_buffer_size);
        #ifdef PRINT_COLORED
        sprintf(buffer_2 + strlen(buffer_2), COLOR_RED "%lu new changes: " COLOR_RESET "\n", updates.size());
        #else
        sprintf(buffer_2 + strlen(buffer_2), "%lu new changes: \n", updates.size());
        #endif
        for (auto update : updates) {
            sprintf(buffer_2 + strlen(buffer_2), "Shard %lu from node %lu to node %lu\n", update.shard, update.from, update.to);
        }
        LOGF(stdout, "%s", buffer_2);
        delete[] buffer_2;
        #endif

    }

    
    Dynamic_Load_Balancer::Dynamic_Load_Balancer(size_t num_compute, size_t num_shards_per_compute
            , size_t _rebalance_period_seconds, size_t __load_imbalance_ratio, size_t low_load_threshold) 
        : Load_Balancer(new Load_Info_Container(num_compute, num_shards_per_compute, low_load_threshold)
            , _rebalance_period_seconds, __load_imbalance_ratio) {}

    Dynamic_Load_Balancer::~Dynamic_Load_Balancer() {}

    void Dynamic_Load_Balancer::start() {
        started.store(true);
        Load_Info_Container& container = dynamic_cast<Load_Info_Container&>(*this->container);
        while (started.load()) {
            sleep(rebalance_period_seconds);
            #ifdef PRINTER_LOCK
            mtx.lock();
            #endif

            size_t min_load;
            size_t max_load;
            size_t mean_load = 0, sum_load = 0;

            container.compute_load_and_pass(min_load, max_load, mean_load, sum_load);
            load_imbalance_threshold = mean_load / load_imbalance_ratio;
            load_imbalance_threshold_half = load_imbalance_threshold / 2;

            if (max_load - min_load <= load_imbalance_threshold) { // use a statistic of shards(like max shard or mean shard as threshold)
                #ifdef PRINTER_LOCK
                mtx.unlock();
                #endif
                continue;
            }

            int min_stat = check_load(container.min_node().load(), mean_load);
            int max_stat = check_load(container.max_node().load(), mean_load);
            // TODO add something that if we have outlier do some shard, we recompute mean for the other nodes and try to balance those
            while ((max_stat > 1 || min_stat < -1) && max_stat > -2 && min_stat < 2) { // loop on nodes
                Compute_Node_Info& max_node = container.max_node();
                Shard_Iterator& itr = max_node.ordered_iterator();

                // divide if needed
                while (itr.is_valid()) { // loop on shards
                    if (container.is_insignificant(*(itr.shard())) || itr.shard()->load() * 2 < load_imbalance_threshold_half) {
                        break;
                    }

                    int hload_stat = check_load(max_node.load() - itr.shard()->load(), mean_load);
                    int lload_stat = check_load(container.min_node().load() + itr.shard()->load(), mean_load);
                    if (hload_stat >= -1 && lload_stat <= 1) {
                        // passing this shard(and next ones) will not cause troble
                        break;
                    }

                    size_t mean_shard = sum_load / container.num_shards();

                    size_t divide_to = (itr.shard()->load() * 4) / load_imbalance_threshold_half;
                    divide_to = lv->divide_signal(itr.shard()->id(), divide_to);
                    if (divide_to > 1) {
                        container.divide_shard(itr.shard()->owner(), itr.index(), divide_to);
                        itr.reset(); // can do better
                    }
                    else {
                        ++itr;
                    }
                    lv->finish_signal();            
                }

                itr.reset();

                while (itr.is_valid()) { // loop on shards
                    if (container.is_insignificant(*(itr.shard()))) {
                        break;
                    }

                    int hload_stat = check_load(max_node.load() - itr.shard()->load() - container.get_current_change(), mean_load);
                    int lload_stat = check_load(container.min_node().load() + itr.shard()->load(), mean_load);
                    if (hload_stat < -1 || lload_stat > 1) {
                        // it may be possible that continuing with this would result in better balance
                        // while both nodes still remain out of prefered range but keep in mind that
                        // ownership transfer increases the load of a shard. Therefore, the oposit may happen
                        // as well and transfer is not worth it here.
                        // In these cases, it is better to increase num shards. (which we cannot do in current design)
                        ++itr;
                        continue;
                    }
                    
                    assert(&container.max_node() == &max_node);
                    container.change_owner_from_max_to_min(itr.index());
                    ++itr;
                    if (hload_stat < 2 && lload_stat > -2) {
                        break;
                    }
                }
                container.update_max_load();

                if (&max_node == &container.max_node()) {
                    container.ignore_max(sum_load, mean_load); // should not happen?
                }

                min_stat = check_load(container.min_node().load(), mean_load);
                max_stat = check_load(container.max_node().load(), mean_load);
            }

            set_up_new_plan();
            #ifdef PRINTER_LOCK
            mtx.unlock();
            #endif
        }
    }

    void Dynamic_Load_Balancer::set_up_new_plan() {
        auto updates = container->apply();
        #ifdef PRINT_UPDATE_INFO
        char* buffer_2 = new char[write_buffer_size];
        memset(buffer_2, 0, write_buffer_size);
        #ifdef PRINT_COLORED
        sprintf(buffer_2 + strlen(buffer_2), COLOR_RED "%lu new changes: " COLOR_RESET "\n", updates.size());
        #else
        sprintf(buffer_2 + strlen(buffer_2), "%lu new changes: \n", updates.size());
        #endif
        for (auto update : updates) {
            sprintf(buffer_2 + strlen(buffer_2), "Shard %lu from node %lu to node %lu\n", update.shard, update.from, update.to);
        }
        LOGF(stdout, "%s", buffer_2);
        delete[] buffer_2;
        #endif

    }

    Dynamic_Restricted_Load_Balancer::Dynamic_Restricted_Load_Balancer(size_t num_compute, size_t num_shards_per_compute
            , size_t _rebalance_period_seconds, size_t __load_imbalance_ratio, size_t low_load_threshold) 
        : Load_Balancer(new Load_Info_Container_Restricted(num_compute, num_shards_per_compute, low_load_threshold)
            , _rebalance_period_seconds, __load_imbalance_ratio)
            , lr_load(num_compute, {0, 0}) {}

    Dynamic_Restricted_Load_Balancer::~Dynamic_Restricted_Load_Balancer() {}

    void Dynamic_Restricted_Load_Balancer::get_load_req(size_t node_idx, size_t nl, size_t nr
        , size_t node_load, size_t left_load, size_t right_load, bool is_max
        , size_t& left_load_req, size_t& right_load_req) {
        
        size_t mean_load = (left_load + right_load + node_load) / (nr + nl + 1);
        size_t target_sum_left = mean_load * nl, target_sum_right = mean_load * nr;
        bool left_req = (is_max ? target_sum_left > left_load : target_sum_left < left_load);
        bool right_req = (is_max ? target_sum_right > right_load : target_sum_right < right_load);

        left_load_req = left_req ? (is_max ? target_sum_left - left_load : left_load - target_sum_left) : 0;
        right_load_req = right_req ? (is_max ? target_sum_right - right_load : right_load - target_sum_right) : 0;

        // do not push/pop more than needed by this node to avoid unnecessary transfers
        size_t check; 
        if (is_max) {
            check = check_load(node_load - (left_load_req + right_load_req), mean_load);
            if (check < 0) {
                //  checkload(node_load - right_load_req, mean) >= 0:
                //  node - k => mean -> k <= node - mean
                size_t min_load = node_load - mean_load;

                left_load_req = std::min(left_load_req, min_load * left_load / (left_load + right_load));
                right_load_req = std::min(right_load_req, min_load * right_load / (left_load + right_load));
            }
        }
        else {
            check = check_load(node_load + left_load_req + right_load_req, mean_load);
            if (check > 0) {
                //  checkload(node_load - right_load_req, mean) <= 0:
                //  node + k <= mean -> k <= mean - node
                size_t min_load = mean_load - node_load;

                left_load_req = std::min(left_load_req, min_load * left_load / (left_load + right_load));
                right_load_req = std::min(right_load_req, min_load * right_load / (left_load + right_load));
            }
        }

        // size_t check; // push max load
        // if (is_max) {
        //     check = check_load(node_load - (left_load_req + right_load_req), mean_load);
        //     if (check < -1) {
        //         //  checkload(node_load - right_load_req, mean) >= -1:
        //         //  node - k => mean -> k <= node - mean
        //         //  node - k < mean:
        //         //      mean - (load - k) < thresh_half
        //         //      -> k <= thresh_half + nodeload - mean
        //         size_t max_load = (load_imbalance_threshold_half + node_load >= mean_load ? 
        //             load_imbalance_threshold_half + node_load - mean_load : 
        //             node_load - mean_load);

        //         left_load_req = std::min(left_load_req, max_load * left_load / (left_load + right_load));
        //         right_load_req = std::min(right_load_req, max_load * right_load / (left_load + right_load));
        //     }
        // }
        // else {
        //     check = check_load(node_load + left_load_req + right_load_req, mean_load);
        //     if (check > 1) {
        //         //  checkload(node_load - right_load_req, mean) <= 1:
        //         //  node + k <= mean -> k <= mean - node
        //         //  node + k > mean:
        //         //      load + k - mean <= thresh_half
        //         //      -> k <= thresh_half - nodeload + mean
        //         size_t max_load = (load_imbalance_threshold_half + mean_load >= node_load ? 
        //             load_imbalance_threshold_half + mean_load - node_load : 
        //             mean_load - node_load);

        //         left_load_req = std::min(left_load_req, max_load * left_load / (left_load + right_load));
        //         right_load_req = std::min(right_load_req, max_load * right_load / (left_load + right_load));
        //     }
        // }
    }

    void Dynamic_Restricted_Load_Balancer::push_load_left(size_t node_idx, size_t load, size_t mean_load) {
        Load_Info_Container_Restricted& container = dynamic_cast<Load_Info_Container_Restricted&>(*this->container);

        if (load == 0) {
            return;
        }

        assert(node_idx < num_compute());
        assert(node_idx != 0);

        size_t left_idx = node_idx - 1;
        Compute_Node_Info& node = container[node_idx];
        Compute_Node_Info& left_node = container[left_idx];
        Shard_Info* shard_itr;
        size_t pushed = 0;
        for (size_t shard_id = node.first_shard_id(); shard_id != container.shard_id(node.last_shard_id()).next_id(); shard_id = shard_itr->next_id()) {
            shard_itr = &container.shard_id(shard_id);
            assert(shard_itr->owner() == node_idx);

            if (pushed >= load) {
                break;
            }

            if (shard_itr->load() > load - pushed) {
                size_t divide_to = (shard_itr->load() / (load - pushed)) + 1;
                divide_to = lv->divide_signal(shard_itr->id(), divide_to);
                if (divide_to > 1) {
                    container.divide_shard(node_idx, shard_itr->id(), divide_to);
                    lv->finish_signal();
                }
                else {
                    lv->finish_signal();
                    break;
                }
            }

            if (shard_id == node.last_shard_id() || shard_itr->load() > load - pushed) { 
                // bottleneck -> can not divide shard further(either duo to size-limit or the load)
                break;
            }

            pushed += shard_itr->load();
            container.change_owner_and_update_load(node_idx, true, shard_itr->id());
        }

        lr_load[node_idx].first += pushed;
        if (left_idx != 0, check_load(left_node.load(), mean_load) > 1) {
            size_t left_req = 0, right_req = 0;
            get_load_req(left_idx, left_idx, 0, left_node.load(), lr_load[left_idx].first, 0, true, left_req, right_req);
            push_load_left(left_idx, left_req, mean_load);
        }
    }

    void Dynamic_Restricted_Load_Balancer::push_load_right(size_t node_idx, size_t load, size_t mean_load) {
        Load_Info_Container_Restricted& container = dynamic_cast<Load_Info_Container_Restricted&>(*this->container);

        if (load == 0) {
            return;
        }

        assert(node_idx < num_compute() - 1);

        size_t right_idx = node_idx + 1;
        Compute_Node_Info& node = container[node_idx];
        Compute_Node_Info& right_node = container[right_idx];
        Shard_Info* shard_itr;
        size_t pushed = 0;
        for (size_t shard_id = node.last_shard_id(); shard_id != container.shard_id(node.first_shard_id()).prev_id(); shard_id = shard_itr->prev_id()) {
            shard_itr = &container.shard_id(shard_id);
            assert(shard_itr->owner() == node_idx);

            if (pushed >= load) {
                break;
            }

            if (shard_itr->load() > load - pushed) {
                size_t divide_to = (shard_itr->load() / (load - pushed)) + 1;
                divide_to = lv->divide_signal(shard_itr->id(), divide_to);
                if (divide_to > 1) {
                    bool was_last = shard_itr->id() == node.last_shard_id();
                    container.divide_shard(node_idx, shard_itr->id(), divide_to);
                    lv->finish_signal();
                    assert(container.shard_id(container.num_shards() - 1).owner() == node_idx);
                    // assert((!was_last 
                    //         && container.shard_id(container.num_shards() - 1).next_id() == container[node_idx].last_shard_id())
                    //     || (was_last && container.num_shards() - 1 == container[node_idx].last_shard_id()));
                    shard_itr = &container.shard_id(container.num_shards() - 1);
                    shard_id = shard_itr->id();
                }
                else {
                    lv->finish_signal();
                    break;
                }
            }

            // assert(shard_itr->load() <= load - pushed);
            if (shard_id == node.first_shard_id() || shard_itr->load() > load - pushed) { 
                // bottleneck -> can not divide shard further(either duo to size-limit or the load)
                break;
            }

            pushed += shard_itr->load();
            container.change_owner_and_update_load(node_idx, false, shard_id);
        }
        
        lr_load[node_idx].second += pushed;
        if (right_idx != 0, check_load(right_node.load(), mean_load) > 1) {
            size_t left_req = 0, right_req = 0;
            get_load_req(right_idx, 0, num_compute() - right_idx - 1, right_node.load(), 0, lr_load[right_idx].second, true, left_req, right_req);
            push_load_right(right_idx, right_req, mean_load);
        }
    }

    // void Dynamic_Restricted_Load_Balancer::pull_load_left(size_t node_idx, size_t load, size_t mean_load) {
    //     Load_Info_Container_Base& container = *this->container;
    // }

    // void Dynamic_Restricted_Load_Balancer::pull_load_right(size_t node_idx, size_t load, size_t mean_load) {
    //     Load_Info_Container_Base& container = *this->container;
    // }

    void Dynamic_Restricted_Load_Balancer::start() {
        started.store(true);
        Load_Info_Container_Base& container = *this->container;
        while (started.load()) {
            sleep(rebalance_period_seconds);
            #ifdef PRINTER_LOCK
            mtx.lock();
            #endif

            size_t min_load;
            size_t max_load;
            size_t mean_load = 0, sum_load = 0;

            container.compute_load_and_pass(min_load, max_load, mean_load, sum_load);
            load_imbalance_threshold = mean_load / load_imbalance_ratio;
            load_imbalance_threshold_half = load_imbalance_threshold / 2;

            if (max_load - min_load <= load_imbalance_threshold) { // use a statistic of shards(like max shard or mean shard as threshold)
                #ifdef PRINTER_LOCK
                mtx.unlock();
                #endif
                continue;
            }

            size_t left = 0, right = sum_load;
            for(size_t i = 0; i < num_compute(); ++i) {
                right -= container[i].load();
                lr_load[i].first = left;
                lr_load[i].second = right;
                if (i < num_compute() - 1) {
                    left += container[i].load();
                }
            }

            int min_stat = check_load(container.min_node().load(), mean_load);
            int max_stat = check_load(container.max_node().load(), mean_load);
            // TODO add something that if we have outlier do some shard, we recompute mean for the other nodes and try to balance those
            while (max_stat > 1 && max_stat > -2 && min_stat < 2) { // loop on nodes
                Compute_Node_Info& max_node = container.max_node();
                size_t left_req = 0, right_req = 0;
                get_load_req(max_node.id(), max_node.id(), num_compute() - max_node.id() - 1
                    , max_node.load(), lr_load[max_node.id()].first, lr_load[max_node.id()].second, true
                    , left_req, right_req);
                
                push_load_left(max_node.id(), left_req, mean_load);
                push_load_right(max_node.id(), right_req, mean_load);

                if (&max_node == &container.max_node()) {
                    container.ignore_max(sum_load, mean_load); // should not happen?
                }

                min_stat = check_load(container.min_node().load(), mean_load);
                max_stat = check_load(container.max_node().load(), mean_load);
            }

            set_up_new_plan();
            #ifdef PRINTER_LOCK
            mtx.unlock();
            #endif
        }
    }

    void Dynamic_Restricted_Load_Balancer::set_up_new_plan() {
        auto updates = container->apply();
        #ifdef PRINT_UPDATE_INFO
        char* buffer_2 = new char[write_buffer_size];
        memset(buffer_2, 0, write_buffer_size);
        #ifdef PRINT_COLORED
        sprintf(buffer_2 + strlen(buffer_2), COLOR_RED "%lu new changes: " COLOR_RESET "\n", updates.size());
        #else
        sprintf(buffer_2 + strlen(buffer_2), "%lu new changes: \n", updates.size());
        #endif
        for (auto update : updates) {
            sprintf(buffer_2 + strlen(buffer_2), "Shard %lu from node %lu to node %lu\n", update.shard, update.from, update.to);
        }
        LOGF(stdout, "%s", buffer_2);
        delete[] buffer_2;
        #endif

    }

}