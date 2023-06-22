//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  latch_.lock();

  if (mp_.empty()) {
    latch_.unlock();
    return false;
  }

  *frame_id = lst_.back();
  mp_.erase(lst_.back());
  lst_.pop_back();

  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();

  if (mp_.find(frame_id) != mp_.end()) {
    lst_.erase(mp_[frame_id]);
    mp_.erase(frame_id);
  }

  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();

  if (mp_.find(frame_id) == mp_.end() && mp_.size() != capacity_) {
    lst_.push_front(frame_id);
    mp_[frame_id] = lst_.begin();
  }

  latch_.unlock();
}

auto LRUReplacer::Size() -> size_t { return mp_.size(); }

}  // namespace bustub
