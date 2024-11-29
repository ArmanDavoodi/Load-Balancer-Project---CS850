#ifndef LOAD_INFO_CONTAINER_H_
#define LOAD_INFO_CONTAINER_H_

#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <stddef.h>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <memory>

#include <iostream>
#include <stdio.h>
#include <assert.h>
#include <cstring>

#include "config.h"

#include "testlog.h"

//TODO add cuncurrency -> we may need to update load info at the time of sorting and stuff

namespace TimberSaw {

struct Load_Info {
    std::unique_ptr<std::atomic<size_t>> current_load; // accessed by other threads incrementing load
    size_t last_load; // accessed only by lb thread so no concurrent access
    #ifdef ANALYZE
    std::unique_ptr<std::atomic<size_t>> round_load;
    #endif
    
    Load_Info() : last_load(0), current_load(new std::atomic<size_t>(0))
    #ifdef ANALYZE
    , round_load(new std::atomic<size_t>(0)) 
    #endif
    {}

    // Move constructor
    Load_Info(Load_Info&& other) noexcept
        : current_load(std::move(other.current_load)), last_load(other.last_load)
        #ifdef ANALYZE
        , round_load(std::move(other.round_load))
        #endif
         {}

    // Move assignment operator
    Load_Info& operator=(Load_Info&& other) noexcept {
        if (this != &other) {
            current_load = std::move(other.current_load);
            #ifdef ANALYZE
            round_load = std::move(other.round_load);
            #endif
            last_load = other.last_load;
        }
        return *this;
    }


    void compute_load_and_pass() { //TODO memory order && do I need to make last_load atomic? -> it is accessed as write only in lb -> one writer + one reader
        last_load = (*current_load).exchange(0) + last_load / 2;
    }

    void print(char* buffer) const {
        // sprintf(buffer + strlen(buffer), "last_load: %lu, num_reads: %lu, num_writes: %lu, num_remote_reads: %lu, num_flushes: %lu\n"
        //     , last_load, num_reads.load(), num_writes.load(), num_remote_reads.load(), num_flushes.load());
        sprintf(buffer + strlen(buffer), "last_load = %lu, current_load = %lu", last_load, (*current_load).load());
        #ifdef ANALYZE
        sprintf(buffer + strlen(buffer), ", round_load = %lu", (*round_load).load());
        #endif
        sprintf(buffer + strlen(buffer), "\n");
    }

};

class Shard_Info {
public:
    Shard_Info() = default;

    // Move constructor
    Shard_Info(Shard_Info&& other) noexcept
        : _owner(other._owner), _id(other._id), _next_shard_id(other._next_shard_id), _prev_shard_id(other._prev_shard_id)
        , _load(std::move(other._load)) {
        
    }

    // Move assignment operator
    Shard_Info& operator=(Shard_Info&& other) noexcept {
        if (this != &other) {
            _owner = other._owner;
            _id = other._id;
            _next_shard_id = other._next_shard_id;
            _prev_shard_id = other._prev_shard_id;
            _load = std::move(other._load);
        }
        return *this;
    }

    inline size_t load() const {
        return _load.last_load;
    }

    inline size_t owner() const {
        return _owner;
    }

    inline size_t id() const {
        return _id;
    }

    inline size_t next_id() const {
        return _next_shard_id;
    }

    inline size_t prev_id() const {
        return _prev_shard_id;
    }

    inline void set_owner(size_t new_owner) {
        _owner = new_owner;
    }

    inline void print(char* buffer) const {
        sprintf(buffer + strlen(buffer), "shard %lu: owner=%lu, prev=%lu, next= %lu, load: "
            , _id, _owner, _prev_shard_id, _next_shard_id);
        _load.print(buffer);
    }

    #ifdef ANALYZE
    inline void new_round() {
        _load.round_load->store(0);
    }
    #endif

    // #ifdef DEBUG
    // ~Shard_Info() {
    //     LOGF(stderr, "Shard %lu is destructed\n", _id);
    // }
    // #endif

    friend class Load_Info_Container_Base;
    friend class Load_Info_Container;
    friend class Load_Info_Container_Restricted;
    friend class Compute_Node_Info;
    
private:
    size_t _owner;
    size_t _id;
    size_t _next_shard_id, _prev_shard_id;
    Load_Info _load;
};

class Compute_Node_Info;

class Shard_Iterator {
public:
    Shard_Iterator(const Shard_Iterator&) = delete;
    Shard_Iterator& operator=(const Shard_Iterator&) = delete;

    bool is_valid();

    Shard_Iterator& operator++();

    Shard_Info* shard();

    inline size_t index() {
        return _shard_idx;
    }
    void reset();

    friend class Compute_Node_Info;
    friend class Load_Info_Container;
    friend class Load_Info_Container_Restricted;
    friend class Load_Info_Container_Base;
private:
    explicit Shard_Iterator(Compute_Node_Info& owner);
    // explicit Shard_Iterator(Compute_Node_Info& owner) : _owner(owner) {
    //     reset();
    // }
    // explicit Shard_Iterator(Compute_Node_Info& owner) : _owner(owner), _shard_idx(owner.num_shards() - 1) {}


private:
    size_t _shard_idx;
    Compute_Node_Info& _owner;
    bool valid;
};

class Compute_Node_Info {
public:
    Compute_Node_Info() : _overal_load(0), itr(*this) {}
    // Compute_Node_Info(size_t num_shards) : _overal_load(0), _shards(num_shards), itr(*this) {}

    inline size_t load() const {
        return _overal_load;
    }

    inline size_t num_shards() const {
        return _num_shards;
    }

    inline size_t id() const {
        return _id;
    }

    inline size_t first_shard_id() const {
        return first_id;
    }

    inline size_t last_shard_id() const {
        return last_id;
    }   

    inline Shard_Info& operator[](size_t shard_idx) { // should not be used by restricted
        return (*all_shards)[_shards[shard_idx]];
    }

    inline Shard_Iterator& ordered_iterator() {
        sort_shards_if_needed();
        return itr;
    }

    inline void print(char* buffer, size_t num_shards_to_print_per_compute_node) const {
        assert(_num_shards == _shards.size());
        size_t current_load = 0;
        #ifdef ANALYZE
        size_t round_load = 0;
        #endif
        for (auto i : _shards) {
            current_load += (*all_shards)[i]._load.current_load->load();
            #ifdef ANALYZE
            round_load += (*all_shards)[i]._load.round_load->load();
            #endif
        }

        #ifdef PRINT_COLORED
        sprintf(buffer + strlen(buffer), COLOR_PURPLE "cnode with id %lu has last load of %lu, current load of %lu", _id, _overal_load, current_load);
        #else
        sprintf(buffer + strlen(buffer), "cnode with id %lu has last load of %lu, current load of %lu", _id, _overal_load, current_load);
        #endif
        #ifdef ANALYZE
        sprintf(buffer + strlen(buffer), ", round load of %lu", round_load);
        #endif
        #ifdef PRINT_COLORED
        sprintf(buffer + strlen(buffer), " and %lu shards" COLOR_RESET "\n", _num_shards);
        #else
        sprintf(buffer + strlen(buffer), " and %lu shards\n", _num_shards);
        #endif
        #ifdef PRINT_SHARD_PER_NODE
        sprintf(buffer + strlen(buffer), ":\n\n");

        for (size_t i = 0; i < _num_shards && (num_shards_to_print_per_compute_node == 0 || i < num_shards_to_print_per_compute_node); ++i) {
            assert((*all_shards)[_shards[i]].owner() == _id);
            #if defined(DEBUG) && defined(ANALYZE)
            sprintf(buffer + strlen(buffer), "%lu(cload: %lu, rload:%lu, lload:%lu, prev:%lu, next:%lu), "
                , (*all_shards)[_shards[i]].id(), (*all_shards)[_shards[i]]._load.current_load->load(), (*all_shards)[_shards[i]]._load.round_load->load(), (*all_shards)[_shards[i]].load(), (*all_shards)[_shards[i]].prev_id(), (*all_shards)[_shards[i]].next_id());
            #elif defined(DEBUG)
            sprintf(buffer + strlen(buffer), "%lu(cload: %lu, lload:%lu, prev:%lu, next:%lu), "
                , (*all_shards)[_shards[i]].id(), (*all_shards)[_shards[i]]._load.current_load->load(), (*all_shards)[_shards[i]].load(), (*all_shards)[_shards[i]].prev_id(), (*all_shards)[_shards[i]].next_id());
            #elif defined(ANALYZE)
            sprintf(buffer + strlen(buffer), "%lu(rload: %lu), "
                , (*all_shards)[_shards[i]].id(), (*all_shards)[_shards[i]]._load.round_load->load());
            #else
            sprintf(buffer + strlen(buffer), "%lu, "
                , (*all_shards)[_shards[i]].id());
            #endif
        }

        sprintf(buffer + strlen(buffer), "\n");
        #endif
        sprintf(buffer + strlen(buffer), "\n");
        
    }

    friend class Load_Info_Container;
    friend class Load_Info_Container_Restricted;
    friend class Load_Info_Container_Base;

private:
    class Shard_Info_Pointer_Cmp {
    public:
        inline bool operator()(const size_t& a, const size_t& b) const {
            assert(all_shards != nullptr && a < all_shards->size() && b < all_shards->size());
            return (*all_shards)[a].load() < (*all_shards)[b].load();
        }

        std::vector<Shard_Info>* all_shards;
    };

    void sort_shards_if_needed();

    inline void compute_load_and_pass() {
        _overal_load = 0;
        is_sorted = false;
        itr.reset();
        assert(_shards.size() == _num_shards);
        for (auto i : _shards) {
            (*all_shards)[i]._load.compute_load_and_pass();
            _overal_load += (*all_shards)[i].load();
        }
    }

private:
    size_t _overal_load;
    size_t _id;
    std::vector<size_t> _shards; 
    std::vector<Shard_Info>* all_shards;
    Shard_Iterator itr;
    bool is_sorted = false;
    size_t first_id, last_id;
    size_t _num_shards;
};

struct Owner_Ship_Transfer {
    size_t from;
    size_t to;
    size_t shard;
};

class Load_Info_Container_Base {
public:
    Load_Info_Container_Base(size_t num_compute, size_t num_shards_per_compute, size_t low_load_threshold)
        : cnodes(num_compute), shards(num_compute * num_shards_per_compute), low_load_thresh(low_load_threshold)
          , last_shard_id((num_compute * num_shards_per_compute) - 1) {

        size_t shard_id = 0;
        for (size_t i = 0; i < num_compute; ++i) {
            cnodes[i]._id = i;
            cnodes[i]._shards.resize(num_shards_per_compute);
            cnodes[i].itr.reset();
            cnodes[i].all_shards = &shards;
            cnodes[i].first_id = shard_id;
            for (size_t j = 0; j < num_shards_per_compute; ++j, ++shard_id) {
                shards[shard_id]._id = shard_id;
                shards[shard_id]._owner = i;
                shards[shard_id]._next_shard_id = shard_id + 1;
                shards[shard_id]._prev_shard_id = shard_id - 1;
                cnodes[i]._shards[j] = shard_id;
            }
            cnodes[i].last_id = shard_id - 1;
            cnodes[i]._num_shards = cnodes[i]._shards.size();
        }
        shards[0]._prev_shard_id = last_shard_id + 1;

    }

    ~Load_Info_Container_Base() {}

    // void rewrite_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes);
    // void increment_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes);
    void increment_load_info(size_t shard, size_t added_load);
    void compute_load_and_pass(size_t& min_load, size_t& max_load, size_t& mean_load, size_t& sum_load);
    void update_max_load();
    void change_owner_from_max_to_min(size_t shard_idx);

    virtual std::vector<Owner_Ship_Transfer> apply() = 0;
    virtual void divide_shard(size_t owner, size_t index, size_t num) = 0;

    inline bool is_insignificant(Shard_Info& shard) {
        return shard.load() <= low_load_thresh; // TODO better approaches?
    }

    inline void ignore_max(size_t& sum_load, size_t& mean_load) {
        sum_load -= ordered_nodes.rbegin()->first;
        mean_load = sum_load;
        ordered_nodes.erase(std::prev(ordered_nodes.end()));
        mean_load /= ordered_nodes.size();
        max_load_change = 0;
    }

    inline void ignore_min(size_t& sum_load, size_t& mean_load) {
        sum_load -= ordered_nodes.begin()->first;
        mean_load = sum_load;
        ordered_nodes.erase(ordered_nodes.begin());
        mean_load /= ordered_nodes.size();
        max_load_change = 0;
    }

    inline Compute_Node_Info& max_node() {
        return cnodes[ordered_nodes.rbegin()->second];
    }

    inline Compute_Node_Info& min_node() {
        return cnodes[ordered_nodes.begin()->second];
    }

    inline Shard_Info& shard_id(size_t shard_id) {
        return shards[shard_id];
    }

    inline Compute_Node_Info& operator[](size_t compute_id) {
        return cnodes[compute_id];
    }

    inline size_t num_shards() {
        return shards.size();
    }

    inline size_t num_compute() {
        return cnodes.size();
    }

    inline size_t get_current_change() {
        return max_load_change;
    }

    #ifdef ANALYZE
    void new_round() {
        for (Shard_Info& shard : shards) {
            shard.new_round();
        }
    }
    #endif

protected:
    std::vector<Compute_Node_Info> cnodes;
    std::vector<Shard_Info> shards;
    std::vector<Owner_Ship_Transfer> updates;
    std::multimap<size_t, size_t> ordered_nodes;
    size_t max_load_change = 0;
    size_t low_load_thresh;
    size_t last_shard_id;
};

class Load_Info_Container : public Load_Info_Container_Base {
public:
    Load_Info_Container(size_t num_compute, size_t num_shards_per_compute, size_t low_load_threshold);
    ~Load_Info_Container();

    std::vector<Owner_Ship_Transfer> apply();
    void divide_shard(size_t owner, size_t index, size_t num);
};

class Load_Info_Container_Restricted : public Load_Info_Container_Base {
public:
    Load_Info_Container_Restricted(size_t num_compute, size_t num_shards_per_compute, size_t low_load_threshold);
    ~Load_Info_Container_Restricted();

    std::vector<Owner_Ship_Transfer> apply();
    void divide_shard(size_t owner, size_t shard_id, size_t num);
    void change_owner_and_update_load(size_t node_idx, bool left, size_t end_shard_id); // should update load of both cnodes as well

private:
    // struct range_update {
    //     size_t from_cnode;
    //     bool to_left;
    //     size_t beg_shard_id;
    //     size_t end_shard_id; // (last_shard_id + 1) 
    //     size_t num;
    // };

    std::vector<std::pair<size_t, size_t>> new_first_last;
    std::vector<Owner_Ship_Transfer> tmp_updates; 

};

}

#endif