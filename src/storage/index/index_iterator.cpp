/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, Page *page, int index)
    : buffer_pool_manager_(buffer_pool_manager), page_(page), index_(index) {
  node_ = reinterpret_cast<LeafPage *>(page_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {  // NOLINT
  page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // throw std::runtime_error("unimplemented");
  return node_->GetNextPageId() == INVALID_PAGE_ID && index_ >= node_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // throw std::runtime_error("unimplemented");
  return node_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // throw std::runtime_error("unimplemented");
  ++index_;
  if (index_ == node_->GetSize() && node_->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = node_->GetNextPageId();

    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);

    page_ = buffer_pool_manager_->FetchPage(next_page_id);
    page_->RLatch();

    node_ = reinterpret_cast<LeafPage *>(page_->GetData());
    index_ = 0;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
