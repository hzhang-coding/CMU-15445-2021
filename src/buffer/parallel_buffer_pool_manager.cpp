//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances), pool_size_(pool_size), start_index_(0) {
  // Allocate and create individual BufferPoolManagerInstances
  buffer_pool_manager_instance_ = new BufferPoolManagerInstance *[num_instances_];
  for (size_t i = 0; i < num_instances_; ++i) {
    buffer_pool_manager_instance_[i] =
        new BufferPoolManagerInstance(pool_size, num_instances_, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; ++i) {
    delete buffer_pool_manager_instance_[i];
  }
  delete[] buffer_pool_manager_instance_;
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pool_manager_instance_[page_id % num_instances_];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->FetchPgImp(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->UnpinPgImp(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->FlushPgImp(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  latch_.lock();

  size_t curr_index = start_index_;
  start_index_ = (start_index_ + 1) % num_instances_;

  latch_.unlock();

  Page *page = nullptr;

  for (int i = 0; i < static_cast<int>(num_instances_); ++i) {
    page = dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager_instance_[(curr_index + i) % num_instances_])
               ->NewPgImp(page_id);
    if (page != nullptr) {
      break;
    }
  }

  return page;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; ++i) {
    dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager_instance_[i])->FlushAllPgsImp();
  }
}

}  // namespace bustub
