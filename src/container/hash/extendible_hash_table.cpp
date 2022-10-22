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
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      dir_.push_back(std::make_shared<Bucket>(bucket_size));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx] -> Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx] -> Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // std::cout << "Inserting key: " << key << std::endl;

  size_t idx = IndexOf(key);
  {
    std::scoped_lock<std::mutex> lock(latch_);
    if (dir_[idx] -> Exists(key)){
      dir_[idx] -> Insert(key, value);
      return;
    }
  }

  if (dir_[idx] -> IsFull()){
    {
      std::scoped_lock<std::mutex> lock(latch_);
      int local_depth = dir_[idx] -> GetDepth();

      if (local_depth == global_depth_){
        // Double directory size
        size_t dir_size = dir_.size();
        dir_.resize(2*dir_size);
        for (size_t i=dir_size; i<dir_size*2; i++){
          dir_[i] = dir_[i & ~(1 << global_depth_)]; 
        }
        global_depth_++;
      }

      // Allocate new bucket
      size_t new_bucket_idx = idx | (1 << local_depth);
      dir_[new_bucket_idx] = std::make_shared<Bucket>(bucket_size_, ++local_depth);
      dir_[idx] -> IncrementDepth();
      num_buckets_++;
      
      // Redistribute kv pairs in the bucket.
      auto items = dir_[idx] -> GetItems();
      for (auto item : items){
        size_t new_idx = IndexOf(item.first);
        if (new_idx != idx){
          dir_[idx] -> Remove(item.first);
          dir_[new_idx] -> Insert(item.first, item.second);
        } 
      }
    }
    Insert(key, value);
  } else {
    std::scoped_lock<std::mutex> lock(latch_);
    dir_[idx] -> Insert(key, value);
  }

}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // std::scoped_lock<std::mutex> lock(bucket_latch_);
  for (auto elem : list_){
    if (elem.first == key){
      value = elem.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // std::scoped_lock<std::mutex> lock(bucket_latch_);
  for (auto it = list_.begin(); it != list_.end(); it++){
    if ((*it).first == key){
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // std::scoped_lock<std::mutex> lock(bucket_latch_);
  // If the bucket is full, do nothing and return false.
  if (ExtendibleHashTable<K, V>::Bucket::IsFull()) {
    return false;
  }
  
  // If a key already exists, the value should be updated.
  for (auto elem : list_){
    if (elem.first == key){
      elem.second = value;
      return true;
    }
  }

  list_.push_back({key, value});
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Exists(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(bucket_latch_);
  for (auto elem : list_){
    if (elem.first == key){
      return true;
    }
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
