#include "load_info_container.h"
#include "config.h"

#include <assert.h>
#include <algorithm>
#include <cstring>

namespace TimberSaw {

    Shard_Iterator::Shard_Iterator(Compute_Node_Info& owner) : _owner(owner) {
        reset();
    }

    bool Shard_Iterator::is_valid() {
        if (_shard_idx >= _owner.num_shards()) {
            valid = false;
        }
        
        return valid;
    }

    Shard_Iterator& Shard_Iterator::operator++() {
            if (!valid || _shard_idx == 0 || _shard_idx >= _owner.num_shards()) {
                valid = false;
            }
            else {
                --_shard_idx;
            }
            return *this;
    }

    Shard_Info* Shard_Iterator::shard() {
        if (_shard_idx >= _owner.num_shards())
            valid = false;
        return (valid ? &_owner[_shard_idx] : nullptr);
    }

    void Shard_Iterator::reset() {
        _shard_idx = _owner.num_shards() - 1;
        valid = true;
    }

    void Compute_Node_Info::sort_shards_if_needed() {
        if (!is_sorted) {
            std::sort(_shards.begin(), _shards.end(), Shard_Info_Pointer_Cmp{all_shards});
            is_sorted = true;
            itr.reset();
        }
    }


    void Load_Info_Container_Base::increment_load_info(size_t shard, size_t added_load) { 
        // TODO add memory order
        Shard_Info& _shard = shard_id(shard);
        _shard._load.current_load->fetch_add(added_load);
        #ifdef ANALYZE
        _shard._load.round_load->fetch_add(added_load);
        #endif
    }

    void Load_Info_Container_Base::compute_load_and_pass(size_t& min_load, size_t& max_load, size_t& mean_load, size_t& sum_load) {
        updates.clear();
        ordered_nodes.clear();
        max_load_change = 0;
        cnodes[0].compute_load_and_pass();
        sum_load = cnodes[0]._overal_load;
        min_load = cnodes[0]._overal_load;
        max_load = cnodes[0]._overal_load;
        ordered_nodes.insert({cnodes[0]._overal_load, 0});
        for (size_t i = 1; i < cnodes.size(); ++i) {
            cnodes[i].compute_load_and_pass();
            sum_load += cnodes[i]._overal_load;
            if (min_load > cnodes[i]._overal_load) {
                min_load = cnodes[i]._overal_load;
            }
            if (max_load < cnodes[i]._overal_load) {
                max_load = cnodes[i]._overal_load;
            }
            ordered_nodes.insert({cnodes[i]._overal_load, i});
        }

        mean_load = sum_load / cnodes.size();
    }

    void Load_Info_Container_Base::update_max_load() {
        if (max_load_change == 0)
            return;

        Compute_Node_Info& _max_node = max_node();
        assert(_max_node._overal_load > max_load_change);
        _max_node._overal_load -= max_load_change;
        ordered_nodes.erase(std::prev(ordered_nodes.end()));
        ordered_nodes.insert({_max_node._overal_load, _max_node._id});
        max_load_change = 0;
    }

    void Load_Info_Container_Base::change_owner_from_max_to_min(size_t shard_idx) {
        Compute_Node_Info& from = max_node();
        Compute_Node_Info& to = min_node();
        Shard_Info& shard = from[shard_idx];
        assert(shard._owner == from._id);
        shard._owner = to._id;
        to._overal_load += shard.load();
        updates.push_back({from._id, to._id, shard_idx});
        ordered_nodes.erase(ordered_nodes.begin());
        ordered_nodes.insert({to._overal_load, to._id});
        max_load_change += shard.load();
    }

    Load_Info_Container::Load_Info_Container(size_t num_compute, size_t num_shards_per_compute, size_t low_load_threshold) 
        : Load_Info_Container_Base(num_compute, num_shards_per_compute, low_load_threshold) {}

    Load_Info_Container::~Load_Info_Container() {}

    std::vector<Owner_Ship_Transfer> Load_Info_Container::apply() {
        for (auto update : updates) {
            size_t to = update.to;
            size_t shard_id = cnodes[update.from][update.shard].id();

            cnodes[to]._shards.push_back(shard_id);
            cnodes[to].is_sorted = false;
            shards[shard_id]._owner = to;
            cnodes[to]._num_shards = cnodes[to]._shards.size();
        }

        for (auto& update : updates) {
            size_t from = update.from;
            size_t shard_index = update.shard;
            size_t id = cnodes[update.from][shard_index].id();

            std::swap(cnodes[from]._shards[shard_index], cnodes[from]._shards.back());
            cnodes[from]._shards.pop_back();
            cnodes[from].is_sorted = false;
            cnodes[from]._num_shards = cnodes[from]._shards.size();

            update.shard = id;
        }

        return updates;
    }

    void Load_Info_Container::divide_shard(size_t owner, size_t index, size_t num) { 
        assert(num > 1);

        Shard_Info* target = &cnodes[owner][index];
        size_t load = target->load() / num;

        size_t last_size = shards.size();
        target->_load.last_load = load;
        size_t pre_next = target->_next_shard_id;
        target->_next_shard_id = last_size;
        size_t target_id = target->_id;
        cnodes[owner]._shards.erase(cnodes[owner]._shards.begin() + index);
        auto insertion_idx = std::upper_bound(cnodes[owner]._shards.begin(), cnodes[owner]._shards.end(), target_id, Compute_Node_Info::Shard_Info_Pointer_Cmp{&shards});
        std::vector<size_t> new_shards;
        // new_shards.reserve(num);
        new_shards.push_back(target_id);
        // cnodes[owner]._shards.insert(insertion_idx, target);

        shards.resize(last_size + num - 1);
        shards[last_size]._prev_shard_id = target_id;
        for (size_t i = 0; i < num - 1; ++i) {
            shards[i + last_size]._id = i + last_size;
            shards[i + last_size]._owner = owner;
            shards[i + last_size]._load.last_load = load;
            shards[i + last_size]._next_shard_id = i + last_size + 1;
            if (i != 0) {
                shards[i + last_size]._prev_shard_id = i + last_size - 1;
            }
            new_shards.push_back(i + last_size);
            // cnodes[owner]._shards.insert(insertion_idx, &shards[i + last_size]);
        }

        if (target_id != last_shard_id) {
            shards.back()._next_shard_id = pre_next;
            shards[pre_next]._prev_shard_id = shards.size() - 1;
            shards[last_shard_id]._next_shard_id = last_size + num - 1;
        }
        else {
            last_shard_id = last_size + num - 2;
        }

        cnodes[owner]._shards.insert(insertion_idx, new_shards.begin(), new_shards.end());
        cnodes[owner]._num_shards = cnodes[owner]._shards.size();
        shards[0]._prev_shard_id = shards.size();

        #ifdef DEBUG
        for (size_t i = 0; i < cnodes[owner]._shards.size() - 1; ++i) {
            assert(cnodes[owner][i].load() <= cnodes[owner][i + 1].load());
        }
        #endif
    }

    Load_Info_Container_Restricted::Load_Info_Container_Restricted(size_t num_compute, size_t num_shards_per_compute, size_t low_load_threshold) 
        : Load_Info_Container_Base(num_compute, num_shards_per_compute, low_load_threshold)
        , new_first_last(num_compute, {0, 0}) {}

    Load_Info_Container_Restricted::~Load_Info_Container_Restricted() {}

    void Load_Info_Container_Restricted::change_owner_and_update_load(size_t node_idx, bool left, size_t end_shard_id) {    
        assert(node_idx < cnodes.size() && ((left && node_idx != 0) || (!left && node_idx != cnodes.size() - 1)));
        assert(end_shard_id < shards.size());

        if (left) {
            // assert(range_updates[node_idx].first < end_shard_id); this assertion will not work as shard ranges are not sorted by id but can we do something else?
            assert(node_idx != 0);
            if (end_shard_id == 0 || end_shard_id == cnodes[node_idx].first_id) {
                return;
            }
            size_t shard_id = cnodes[node_idx].first_id;
            assert(end_shard_id != shards[cnodes[node_idx].last_shard_id()].next_id());
            assert(shards[end_shard_id].owner() == node_idx);

            auto nodes = ordered_nodes.equal_range(cnodes[node_idx]._overal_load);
            auto node_it = nodes.first;
            for (; node_it != nodes.second; ++node_it) {
                if (node_it->second == node_idx) {
                    break;
                }
            }
            bool nignored_s = (node_it != nodes.second && node_it->second == node_idx);
            nodes = ordered_nodes.equal_range(cnodes[node_idx - 1]._overal_load);
            auto left_it = nodes.first;
            for (; left_it != nodes.second; ++left_it) {
                if (left_it->second == node_idx - 1) {
                    break;
                }
            }
            bool nignored_n = (left_it != nodes.second && left_it->second == node_idx - 1);

            size_t counter = 1;
            for (; shards[shard_id].next_id() != end_shard_id; shard_id = shards[shard_id].next_id(), ++counter) {
                assert(shards[shard_id].owner() == node_idx);
                assert(cnodes[node_idx]._overal_load > shards[shard_id].load());
                assert(counter < cnodes[node_idx]._num_shards);
                cnodes[node_idx]._overal_load -= shards[shard_id].load();
                cnodes[node_idx - 1]._overal_load += shards[shard_id].load();
                shards[shard_id]._owner = node_idx - 1;
                tmp_updates.push_back({node_idx, node_idx - 1, shard_id});
            }
            assert(shards[shard_id].owner() == node_idx);
            assert(cnodes[node_idx]._overal_load > shards[shard_id].load());
            assert(counter < cnodes[node_idx]._num_shards);

            cnodes[node_idx]._overal_load -= shards[shard_id].load();
            cnodes[node_idx - 1]._overal_load += shards[shard_id].load();
            shards[shard_id]._owner = node_idx - 1;
            cnodes[node_idx].first_id = end_shard_id;
            cnodes[node_idx - 1].last_id = shard_id;
            cnodes[node_idx]._num_shards -= counter;
            cnodes[node_idx - 1]._num_shards += counter;
            tmp_updates.push_back({node_idx, node_idx - 1, shard_id});
            

            if (nignored_s) {
                ordered_nodes.erase(node_it);
            }
            ordered_nodes.insert({cnodes[node_idx]._overal_load, node_idx});

            if (nignored_n) {
                ordered_nodes.erase(left_it);
                ordered_nodes.insert({cnodes[node_idx - 1]._overal_load, node_idx - 1});
            }
        }
        else {
            assert(node_idx != cnodes.size() - 1);
            if (end_shard_id == last_shard_id || end_shard_id == cnodes[node_idx].last_id) {
                return;
            }
            size_t shard_id = cnodes[node_idx].last_id;
            assert(end_shard_id != shards[cnodes[node_idx].first_shard_id()].prev_id());
            assert(shards[end_shard_id].owner() == node_idx);

            auto nodes = ordered_nodes.equal_range(cnodes[node_idx]._overal_load);
            auto node_it = nodes.first;
            for (; node_it != nodes.second; ++node_it) {
                if (node_it->second == node_idx) {
                    break;
                }
            }
            bool nignored_s = (node_it != nodes.second && node_it->second == node_idx);
            nodes = ordered_nodes.equal_range(cnodes[node_idx + 1]._overal_load);
            auto right_it = nodes.first;
            for (; right_it != nodes.second; ++right_it) {
                if (right_it->second == node_idx + 1) {
                    break;
                }
            }
            bool nignored_n = (right_it != nodes.second && right_it->second == node_idx + 1);

            size_t counter = 1;
            for (; shards[shard_id].prev_id() != end_shard_id; shard_id = shards[shard_id].prev_id(), ++counter) {
                assert(shards[shard_id].owner() == node_idx);
                assert(cnodes[node_idx]._overal_load > shards[shard_id].load());
                assert(counter < cnodes[node_idx]._num_shards);
                cnodes[node_idx]._overal_load -= shards[shard_id].load();
                cnodes[node_idx + 1]._overal_load += shards[shard_id].load();
                shards[shard_id]._owner = node_idx + 1;
                tmp_updates.push_back({node_idx, node_idx + 1, shard_id});
            }
            assert(shards[shard_id].owner() == node_idx);
            assert(cnodes[node_idx]._overal_load > shards[shard_id].load());
            assert(counter < cnodes[node_idx]._num_shards);
            
            cnodes[node_idx]._overal_load -= shards[shard_id].load();
            cnodes[node_idx + 1]._overal_load += shards[shard_id].load();
            shards[shard_id]._owner = node_idx + 1;
            cnodes[node_idx].last_id = end_shard_id;
            cnodes[node_idx + 1].first_id = shard_id;
            cnodes[node_idx]._num_shards -= counter;
            cnodes[node_idx + 1]._num_shards += counter;
            tmp_updates.push_back({node_idx, node_idx + 1, shard_id});

            if (nignored_s) {
                ordered_nodes.erase(node_it);
            }
            ordered_nodes.insert({cnodes[node_idx]._overal_load, node_idx});
            
            if (nignored_n) {
                ordered_nodes.erase(right_it);
                ordered_nodes.insert({cnodes[node_idx + 1]._overal_load, node_idx + 1});
            }
        }
    }

    std::vector<Owner_Ship_Transfer> Load_Info_Container_Restricted::apply() {
        assert(updates.empty());
        std::map<size_t, Owner_Ship_Transfer> shard_changes;
        for (auto update : tmp_updates) {
            size_t from = update.from;
            size_t to = update.to;

            assert(from < cnodes.size() && to < cnodes.size());

            size_t shard_id = update.shard;
            assert(shard_id < shards.size());

            auto shard_it = shard_changes.find(shard_id);
            if (shard_it != shard_changes.end()) {
                assert(shard_it->second.to == from);
                shard_it->second.from = from;
                shard_it->second.to = to;
                shard_it->second.shard = shard_id;
            }
            else {
                shard_changes[shard_id] = {from, to, shard_id};
            }
        }
        tmp_updates.clear();

        for (auto update : shard_changes) {
            assert(update.second.shard == update.first);
            assert(shards[update.first].owner() == update.second.to);
            updates.push_back(update.second);
        }   

        for (Compute_Node_Info& cnode : cnodes) {
            cnode._shards.clear();
            size_t first = cnode.first_id;
            size_t last = cnode.last_id;
            size_t i = 0;
            assert(cnode._num_shards > 0);
            for (; i < cnode._num_shards && first != shards[last].next_id(); ++i, first = shards[first].next_id()) {
                assert(shards[first].owner() == cnode._id);
                assert(shards[first].next_id() == shards.size() || shards[shards[first].next_id()].prev_id() == first);
                assert(shards[first].prev_id() == shards.size() || shards[shards[first].prev_id()].next_id() == first);
                cnode._shards.push_back(first);
            }
            assert(i == cnode._num_shards && first == shards[last].next_id());
            assert(cnode._shards.size() == cnode._num_shards);
        }

        return updates;
    }

    void Load_Info_Container_Restricted::divide_shard(size_t owner, size_t shard_id, size_t num) { 
        assert(num > 1);

        Shard_Info* target = &shards[shard_id];
        assert(target->owner() == owner);
        // assert(target->id() == cnodes[owner].first_id || target->id() == cnodes[owner].last_id);
        size_t load = target->load() / num;

        size_t last_size = shards.size();
        target->_load.last_load = load;
        size_t pre_next = target->_next_shard_id;
        target->_next_shard_id = last_size;
        size_t target_id = target->_id;
        #ifdef ANALYZE
        target->_load.round_load->store(0);
        #endif
        // cnodes[owner]._shards.erase(cnodes[owner]._shards.begin() + index);
        // auto insertion_idx = std::upper_bound(cnodes[owner]._shards.begin(), cnodes[owner]._shards.end(), target_id, Compute_Node_Info::Shard_Info_Pointer_Cmp{&shards});
        // new_shards.push_back(target_id);
        // cnodes[owner]._shards.insert(insertion_idx, target);

        shards.resize(last_size + num - 1);
        shards[last_size]._prev_shard_id = target_id;
        for (size_t i = 0; i < num - 1; ++i) {
            shards[i + last_size]._id = i + last_size;
            shards[i + last_size]._owner = owner;
            shards[i + last_size]._load.last_load = load;
            shards[i + last_size]._next_shard_id = i + last_size + 1;
            if (i != 0) {
                shards[i + last_size]._prev_shard_id = i + last_size - 1;
            }
            // cnodes[owner]._shards.insert(insertion_idx, &shards[i + last_size]);
        }

        if (target_id != last_shard_id) {
            shards.back()._next_shard_id = pre_next;
            shards[pre_next]._prev_shard_id = shards.size() - 1;
            shards[last_shard_id]._next_shard_id = last_size + num - 1;
        }
        else {
            last_shard_id = last_size + num - 2;
        }

        if (target_id == cnodes[owner].last_id) {   
            cnodes[owner].last_id = last_size + num - 2;
        }

        cnodes[owner]._num_shards += num - 1;
        shards[0]._prev_shard_id = shards.size();

        #ifdef DEBUG
        for (Compute_Node_Info& cnode : cnodes) {
            size_t first = cnode.first_id;
            size_t last = cnode.last_id;
            size_t i = 0;
            assert(cnode._num_shards > 0);
            for (; i < cnode._num_shards && first != shards[last].next_id(); ++i, first = shards[first].next_id()) {
                assert(shards[first].owner() == cnode._id);
                assert(shards[first].next_id() == shards.size() || shards[shards[first].next_id()].prev_id() == first);
                assert(shards[first].prev_id() == shards.size() || shards[shards[first].prev_id()].next_id() == first);
            }
            assert(i == cnode._num_shards && first == shards[last].next_id());
        }
        #endif
    }
}