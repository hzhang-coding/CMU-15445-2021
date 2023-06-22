//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool is_find = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }

    if (cmp(key, array_[i].first) == 0 && IsReadable(i)) {
      is_find = true;
      result->push_back(array_[i].second);
    }
  }

  return is_find;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::MyGetValue(const KeyType &key, const KeyComparator &cmp, std::vector<ValueType> *result)
    -> bool {
  bool is_find = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }

    if (cmp(key, array_[i].first) == 0 && IsReadable(i)) {
      is_find = true;
      result->push_back(array_[i].second);
    }
  }

  return is_find;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  if (IsFull()) {
    return false;
  }

  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }

    if (cmp(key, array_[i].first) == 0 && value == array_[i].second && IsReadable(i)) {
      return false;
    }
  }

  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
      array_[i].first = key;
      array_[i].second = value;
      SetOccupied(i);
      SetReadable(i);
      break;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::MyInsert(const KeyType &key, const ValueType &value, const KeyComparator &cmp) -> bool {
  /*
  if (IsFull()) {
    return false;
  }

  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }

    if (cmp(key, array_[i].first) == 0 && value == array_[i].second && IsReadable(i)) {
      return false;
    }
  }
*/
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
      array_[i].first = key;
      array_[i].second = value;
      SetOccupied(i);
      SetReadable(i);
      return true;
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsExist(const KeyType &key, const ValueType &value, const KeyComparator &cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }

    if (cmp(key, array_[i].first) == 0 && value == array_[i].second && IsReadable(i)) {
      return true;
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }

    if (cmp(key, array_[i].first) == 0 && value == array_[i].second && IsReadable(i)) {
      RemoveAt(i);
      return true;
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::MyRemove(const KeyType &key, const ValueType &value, const KeyComparator &cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      break;
    }

    if (cmp(key, array_[i].first) == 0 && value == array_[i].second && IsReadable(i)) {
      RemoveAt(i);
      return true;
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  return occupied_[bucket_idx / 8] & (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] |= 1 << (bucket_idx % 8);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  return readable_[bucket_idx / 8] & (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] |= 1 << (bucket_idx % 8);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  uint32_t n = (BUCKET_ARRAY_SIZE - 1) / 8;

  for (uint32_t i = 0; i < n; ++i) {
    if (readable_[i] != static_cast<char>(0xFF)) {
      return false;
    }
  }

  for (uint32_t i = (1 << (BUCKET_ARRAY_SIZE - 8 * n)) >> 1; i > 0; i >>= 1) {
    if ((readable_[n] & i) == 0) {
      return false;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t n = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  uint32_t cnt = 0;

  for (uint32_t i = 0; i < n; ++i) {
    uint32_t curr = readable_[i];
    // curr = ((curr >> 1) & 0x55) + (curr & 0x55);
    // curr = ((curr >> 2) & 0x33) + (curr & 0x33);
    // curr = (curr >> 4) + (curr & 0x0F);
    curr = ((curr >> 1) & 0x55) + (curr & 0x55);
    curr = ((curr >> 2) & 0x33) + (curr & 0x33);
    curr = (curr >> 4) + (curr & 0x0F);
    cnt += curr;
  }

  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  uint32_t n = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;

  for (uint32_t i = 0; i < n; ++i) {
    if (readable_[i] != 0) {
      return false;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::GetAllPairs(std::vector<MappingType> *result) {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsOccupied(i)) {
      return;
    }

    if (IsReadable(i)) {
      result->push_back(array_[i]);
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Clear() {
  uint32_t n = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;

  for (uint32_t i = 0; i < n; ++i) {
    occupied_[i] = 0;
    readable_[i] = 0;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
