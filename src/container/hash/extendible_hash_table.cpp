//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key, const int &depth) -> size_t {
  int mask = (1 << depth) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.RLock();
  int ret = global_depth_;
  latch_.RUnlock();
  return ret;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.RLock();
  int ret = dir_[dir_index]->GetDepth();
  latch_.RUnlock();
  return ret;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.RLock();
  int ret = num_buckets_;
  latch_.RUnlock();
  return ret;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.RLock();
  size_t idx = IndexOf(key);
  bool ret = dir_[idx]->Find(key, value);
  latch_.RUnlock();
  return ret;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  // std::cout << "Removing: " << key << std::endl;
  latch_.WLock();
  size_t idx = IndexOf(key);
  bool ret = dir_[idx]->Remove(key);
  latch_.WUnlock();
  return ret;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // std::scoped_lock<std::mutex> lock(latch_);
  // std::cout << "Inserting: " << key << std::endl;
  latch_.WLock();
  size_t idx = IndexOf(key);

  if (dir_[idx]->IsFull()) {
    int local_depth = dir_[idx]->GetDepth();

    if (local_depth == global_depth_) {
      // Double directory size
      size_t dir_size = dir_.size();
      dir_.resize(2 * dir_size);
      for (size_t i = dir_size; i < dir_size * 2; i++) {
        dir_[i] = dir_[i & ~(1 << global_depth_)];
      }
      global_depth_++;
    } else {
      idx = IndexOf(key, local_depth);
    }

    // Allocate new bucket
    size_t new_bucket_idx = idx | (1 << local_depth);
    dir_[new_bucket_idx] = std::make_shared<Bucket>(bucket_size_, ++local_depth);
    dir_[idx]->IncrementDepth();
    num_buckets_++;
    for (size_t i = 0; i < dir_.size(); i++) {
      if ((i & ~(1 << local_depth)) == new_bucket_idx) {
        dir_[i] = dir_[new_bucket_idx];
      }
    }

    // Redistribute kv pairs in the bucket.
    auto items = dir_[idx]->GetItems();
    for (auto const &item : items) {
      size_t new_idx = IndexOf(item.first, local_depth);
      if (new_idx != idx) {
        dir_[idx]->Remove(item.first);
        dir_[new_idx]->Insert(item.first, item.second);
      }
    }
    // latch_.unlock();
    latch_.WUnlock();
    Insert(key, value);
  } else {
    dir_[idx]->Insert(key, value);
    latch_.WUnlock();
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(bucket_latch_);
  for (auto const &elem : list_) {
    if (elem.first == key) {
      value = elem.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(bucket_latch_);
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::scoped_lock<std::mutex> lock(bucket_latch_);
  // If a key already exists, the value should be updated.
  if (Exists(key)) {
    for (auto &elem : list_) {
      if (elem.first == key) {
        elem.second = value;
        return true;
      }
    }
  }

  // If the bucket is full, do nothing and return false.
  if (list_.size() == size_) {
    return false;
  }

  list_.push_back({key, value});
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Exists(const K &key) -> bool {
  // std::scoped_lock<std::mutex> lock(bucket_latch_);
  bool ret = std::any_of(list_.begin(), list_.end(), [key](std::pair<K, V> elem) { return elem.first == key; });
  return ret;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
