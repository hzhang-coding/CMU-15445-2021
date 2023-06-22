//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *buffer_pool_manager, Page *page, int index);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;
  auto isEnd() -> bool { return IsEnd(); }

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    // throw std::runtime_error("unimplemented");
    return node_->GetPageId() == itr.node_->GetPageId() && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    // throw std::runtime_error("unimplemented");
    return !(*this == itr);
  }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  Page *page_;
  int index_;
  LeafPage *node_;
};

}  // namespace bustub
