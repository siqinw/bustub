//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    {
        std::scoped_lock<std::mutex> lock(latch_);
        current_timestamp_++;

        if (frame_map_.size() == 0) {
            return false;
        }

        FindEvictFrame(frame_id);
        frame_map_.erase(*frame_id);
        curr_size_--;
    }
    // std::cout << "Evicting Frame " << *frame_id << std::endl;
    return true;
 }

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, "Invalid Frame ID");
    {
        std::scoped_lock<std::mutex> lock(latch_);
        current_timestamp_++;

        if (!frame_map_.count(frame_id) && !non_evict_frames_.count(frame_id)) {
            frame_map_.insert({frame_id, Frame(frame_id)});
            curr_size_++;
            frame_map_.at(frame_id).RecordAccess(current_timestamp_);
        } else if (frame_map_.count(frame_id)) {
            frame_map_.at(frame_id).RecordAccess(current_timestamp_);
        } else {
            non_evict_frames_.at(frame_id).RecordAccess(current_timestamp_);
        } 
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, "Invalid Frame ID");
    {
        std::scoped_lock<std::mutex> lock(latch_);
        current_timestamp_++;

        // If evictable, set to non-evictable
        if (frame_map_.count(frame_id) && set_evictable == false){
            Frame frame = frame_map_.at(frame_id);
            frame_map_.erase(frame_id);
            non_evict_frames_.insert({frame_id, frame});
            curr_size_--;
        }

        // If non-evictable, set to evictable
        else if (non_evict_frames_.count(frame_id) && set_evictable == true){
            Frame frame = non_evict_frames_.at(frame_id);
            frame_map_.insert({frame_id, frame});
            non_evict_frames_.erase(frame_id);
            curr_size_++;
        }

        // else, return
        else {}
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, "Invalid Frame ID");
    {
        std::scoped_lock<std::mutex> lock(latch_);
        current_timestamp_++;

        BUSTUB_ASSERT(!non_evict_frames_.count(frame_id), "Evicting non-evictable frame");

        if (frame_map_.count(frame_id)){
            frame_map_.erase(frame_id);
            curr_size_--;
        }
    }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::FindEvictFrame(frame_id_t *frame_id){
    std::vector<LRUKReplacer::Frame> largest_k_frames;
    size_t largest_k_distance = 0;

    for (auto elem : frame_map_){
        Frame frame = elem.second;
        std::vector<size_t> timestamps = frame.GetTimestamps();
        BUSTUB_ASSERT(timestamps.size(), "ZERO Timestamp entry");

        if (timestamps.size() < k_){
            largest_k_frames.push_back(frame);
        } else {
            size_t k_distance = current_timestamp_ - frame.FindKthPreviousTimestamp(k_);
            if (k_distance > largest_k_distance) {
                largest_k_distance = k_distance;
                *frame_id = frame.GetFrameID();
            }
        }
    }

    if (largest_k_frames.size() == 1) {
        *frame_id = largest_k_frames[0].GetFrameID();
    } else if (largest_k_frames.size() > 1) {
        *frame_id = FindEarliestFrame(largest_k_frames);
    }
}

auto LRUKReplacer::FindEarliestFrame(std::vector<LRUKReplacer::Frame> frames) -> frame_id_t {
    BUSTUB_ASSERT(frames.size(), "ZERO Frame");
    
    frame_id_t earliest_frame = -1;
    size_t earliest_timestamp = INT32_MAX;

    for (auto frame : frames){
        if (frame.GetTimestamps()[0] < earliest_timestamp){
            earliest_timestamp = frame.GetTimestamps()[0];
            earliest_frame = frame.GetFrameID();
        }
    }

    BUSTUB_ASSERT(earliest_frame != -1, "Invalid Frame ID");
    return earliest_frame;
}


LRUKReplacer::Frame::Frame(frame_id_t frame_id) : frame_id_(frame_id) {}

LRUKReplacer::Frame::~Frame() = default;

void LRUKReplacer::Frame::RecordAccess(size_t timestamp){
    timestamps.push_back(timestamp);
}

auto LRUKReplacer::Frame::GetTimestamps() -> std::vector<size_t> {
    // BUSTUB_ASSERT(timestamps.size(), "ZERO Timestamp");
    return timestamps;
}

auto LRUKReplacer::Frame::FindKthPreviousTimestamp(size_t k) -> size_t {
    BUSTUB_ASSERT(timestamps.size() >= k, "Less Than K Timestamps");

    auto iter = timestamps.rbegin();
    while (k > 1) {
        iter++;
        k--;
    }

    return *iter;
}

}  // namespace bustub
