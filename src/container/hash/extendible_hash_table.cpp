//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  Page *page = buffer_pool_manager_->NewPage(&directory_page_id_);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());

  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id);

  dir_page->SetPageId(directory_page_id_);
  dir_page->SetBucketPageId(0, bucket_page_id);

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  // index &= dir_page->GetLocalDepth(index);
  return dir_page->GetBucketPageId(index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  return reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  page_id_t bucket_page_id = dir_page->GetBucketPageId(index);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->RLatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();

  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool is_find = bucket_page->MyGetValue(key, comparator_, result);

  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);

  return is_find;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  page_id_t bucket_page_id = dir_page->GetBucketPageId(index);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();

  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool is_insert = false;

  if (!bucket_page->IsExist(key, value, comparator_)) {
    is_insert = bucket_page->MyInsert(key, value, comparator_);

    if (!is_insert) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(bucket_page_id, false);
      return SplitInsert(transaction, key, value);
    }
  }

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, is_insert);

  return is_insert;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  page_id_t bucket_page_id = dir_page->GetBucketPageId(index);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();
  page->WUnlatch();

  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool is_insert = false;
  bool is_dir_dirty = false;

  if (!bucket_page->IsExist(key, value, comparator_)) {
    while (bucket_page->IsFull()) {
      page_id_t image_bucket_page_id;
      Page *image_page = buffer_pool_manager_->NewPage(&image_bucket_page_id);
      auto image_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(image_page->GetData());
      uint32_t image_index = index ^ (1 << dir_page->GetLocalDepth(index));

      dir_page->IncrLocalDepth(index);
      uint32_t local_depth = dir_page->GetLocalDepth(index);
      uint32_t mask = dir_page->GetLocalDepthMask(index);
      uint32_t n = 1 << dir_page->GetGlobalDepth();

      if (local_depth > dir_page->GetGlobalDepth()) {
        for (uint32_t i = 0; i < n; ++i) {
          dir_page->SetBucketPageId(i + n, dir_page->GetBucketPageId(i));
          dir_page->SetLocalDepth(i + n, dir_page->GetLocalDepth(i));
        }

        dir_page->IncrGlobalDepth();
        // dir_page->SetLocalDepth(image_index, local_depth);
        dir_page->SetBucketPageId(image_index, image_bucket_page_id);
      } else {
        uint32_t diff = 1 << local_depth;

        for (uint32_t i = index & mask; i < n; i += diff) {
          dir_page->SetLocalDepth(i, local_depth);
        }

        for (uint32_t i = image_index & mask; i < n; i += diff) {
          dir_page->SetLocalDepth(i, local_depth);
          dir_page->SetBucketPageId(i, image_bucket_page_id);
        }
      }

      std::vector<MappingType> result;
      bucket_page->GetAllPairs(&result);
      bucket_page->Clear();

      for (auto &&[k, v] : result) {
        if ((Hash(k) & mask) == index) {
          bucket_page->MyInsert(k, v, comparator_);
        } else {
          image_bucket_page->MyInsert(k, v, comparator_);
        }
      }

      page_id_t to_insert_page_id = dir_page->GetBucketPageId(Hash(key) & mask);
      if (to_insert_page_id == bucket_page_id) {
        buffer_pool_manager_->UnpinPage(image_bucket_page_id, true);
      } else {
        buffer_pool_manager_->UnpinPage(bucket_page_id, true);
        index = image_index;
        bucket_page_id = image_bucket_page_id;
        bucket_page = image_bucket_page;
      }

      is_dir_dirty = true;
    }

    is_insert = bucket_page->MyInsert(key, value, comparator_);
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, is_insert);
  buffer_pool_manager_->UnpinPage(directory_page_id_, is_dir_dirty);
  table_latch_.WUnlock();

  return is_insert;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  page_id_t bucket_page_id = dir_page->GetBucketPageId(index);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();

  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool is_remove = bucket_page->MyRemove(key, value, comparator_);

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, is_remove);

  if (is_remove) {
    uint32_t local_depth = dir_page->GetLocalDepth(index);
    uint32_t high_bit = local_depth > 0 ? 1 << (local_depth - 1) : 0;
    uint32_t image_index = index ^ high_bit;

    if (local_depth > 0 && dir_page->GetLocalDepth(image_index) == local_depth && bucket_page->IsEmpty()) {
      Merge(transaction, key, value);
    }
  }

  return is_remove;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  page_id_t bucket_page_id = dir_page->GetBucketPageId(index);
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();

  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bool is_merge = false;

  uint32_t local_depth = dir_page->GetLocalDepth(index);
  uint32_t high_bit = local_depth > 0 ? 1 << (local_depth - 1) : 0;
  uint32_t image_index = index ^ high_bit;

  while (local_depth > 0 && dir_page->GetLocalDepth(image_index) == local_depth && bucket_page->IsEmpty()) {
    page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_index);
    uint32_t diff = 1 << local_depth;
    uint32_t n = 1 << dir_page->GetGlobalDepth();
    --local_depth;

    for (uint32_t i = index & (diff - 1); i < n; i += diff) {
      dir_page->SetLocalDepth(i, local_depth);
      dir_page->SetBucketPageId(i, image_bucket_page_id);
    }

    for (uint32_t i = image_index & (diff - 1); i < n; i += diff) {
      dir_page->SetLocalDepth(i, local_depth);
      // dir_page->SetBucketPageId(i, image_bucket_page_id);
    }

    dir_page->CanShrink();

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->DeletePage(bucket_page_id);

    bucket_page_id = image_bucket_page_id;
    page = buffer_pool_manager_->FetchPage(bucket_page_id);
    page->WLatch();
    bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());

    high_bit = local_depth > 0 ? 1 << (local_depth - 1) : 0;
    image_index = index ^ high_bit;

    is_merge = true;
  }

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, is_merge);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
