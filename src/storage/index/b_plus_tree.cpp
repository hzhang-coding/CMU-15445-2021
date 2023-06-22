//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  rwlatch_.RLock();
  if (IsEmpty()) {
    rwlatch_.RUnlock();
    return false;
  }

  Page *leaf_page = FindLeafPage(key);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType value;
  bool is_find = leaf_node->Lookup(key, &value, comparator_);

  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

  if (!is_find) {
    return false;
  }

  result->push_back(value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  rwlatch_.WLock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    rwlatch_.WUnlock();

    return true;
  }

  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  auto root_node = reinterpret_cast<LeafPage *>(page->GetData());
  root_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  root_node->Insert(key, value, comparator_);

  root_page_id_ = page_id;
  UpdateRootPageId(1);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewRoot(BPlusTreePage *left_node, const KeyType &key, BPlusTreePage *right_node) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  left_node->SetParentPageId(page_id);
  right_node->SetParentPageId(page_id);

  auto root_node = reinterpret_cast<InternalPage *>(page->GetData());
  root_node->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
  root_node->PopulateNewRoot(left_node->GetPageId(), key, right_node->GetPageId());
  root_page_id_ = page_id;
  UpdateRootPageId(0);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}
/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  bool is_root_latched = true;

  page_id_t page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();

  auto *curr_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!curr_node->IsLeafPage()) {
    auto *node = reinterpret_cast<InternalPage *>(page->GetData());
    page_id = node->Lookup(key, comparator_);

    transaction->AddIntoPageSet(page);

    page = buffer_pool_manager_->FetchPage(page_id);
    page->WLatch();

    curr_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

    if (curr_node->GetSize() + 1 < curr_node->GetMaxSize()) {
      if (is_root_latched) {
        is_root_latched = false;
        rwlatch_.WUnlock();
      }

      for (Page *pg : *transaction->GetPageSet()) {
        pg->WUnlatch();
        buffer_pool_manager_->UnpinPage(pg->GetPageId(), false);
      }
      transaction->GetPageSet()->clear();
    }
  }

  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());

  int size = leaf_node->GetSize();
  if (leaf_node->Insert(key, value, comparator_) == size) {
    if (is_root_latched) {
      is_root_latched = false;
      rwlatch_.WUnlock();
    }

    for (Page *pg : *transaction->GetPageSet()) {
      pg->WUnlatch();
      buffer_pool_manager_->UnpinPage(pg->GetPageId(), false);
    }
    transaction->GetPageSet()->clear();

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    return false;
  }

  if (leaf_node->GetSize() == leaf_node->GetMaxSize()) {
    LeafPage *left_node = leaf_node;
    LeafPage *right_node = SplitLeafNode(left_node);

    if (left_node->IsRootPage()) {
      StartNewRoot(left_node, right_node->KeyAt(0), right_node);
    } else {
      InsertIntoParent(left_node, right_node->KeyAt(0), right_node, transaction);
    }

    buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
  }

  if (is_root_latched) {
    is_root_latched = false;
    rwlatch_.WUnlock();
  }

  for (Page *pg : *transaction->GetPageSet()) {
    pg->WUnlatch();
    buffer_pool_manager_->UnpinPage(pg->GetPageId(), false);
  }
  transaction->GetPageSet()->clear();

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafNode(LeafPage *left_node) -> LeafPage * {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  auto *right_node = reinterpret_cast<LeafPage *>(page->GetData());
  right_node->Init(page_id, left_node->GetParentPageId(), left_node->GetMaxSize());
  left_node->MoveHalfTo(right_node);
  right_node->SetNextPageId(left_node->GetNextPageId());
  left_node->SetNextPageId(right_node->GetPageId());

  return right_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalNode(InternalPage *left_node) -> InternalPage * {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  auto *right_node = reinterpret_cast<InternalPage *>(page->GetData());
  right_node->Init(page_id, left_node->GetParentPageId(), left_node->GetMaxSize());
  left_node->MoveHalfTo(right_node, buffer_pool_manager_);

  return right_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // Page *page = transaction->GetPageSet()->back();
  // transaction->GetPageSet()->pop_back();

  Page *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto internal_node = reinterpret_cast<InternalPage *>(page->GetData());
  internal_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (internal_node->GetSize() == internal_node->GetMaxSize()) {
    InternalPage *left_node = internal_node;
    InternalPage *right_node = SplitInternalNode(left_node);

    if (left_node->IsRootPage()) {
      StartNewRoot(left_node, right_node->KeyAt(0), right_node);
    } else {
      InsertIntoParent(left_node, right_node->KeyAt(0), right_node, transaction);
    }

    buffer_pool_manager_->UnpinPage(right_node->GetPageId(), true);
  }

  // page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  rwlatch_.WLock();
  bool is_root_latched = true;

  if (IsEmpty()) {
    rwlatch_.WUnlock();
    return;
  }

  page_id_t page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  page->WLatch();

  auto *curr_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!curr_node->IsLeafPage()) {
    auto *node = reinterpret_cast<InternalPage *>(page->GetData());
    page_id = node->Lookup(key, comparator_);

    transaction->AddIntoPageSet(page);

    page = buffer_pool_manager_->FetchPage(page_id);
    page->WLatch();

    curr_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

    if (curr_node->GetSize() > curr_node->GetMinSize()) {
      if (is_root_latched) {
        is_root_latched = false;
        rwlatch_.WUnlock();
      }

      for (Page *pg : *transaction->GetPageSet()) {
        pg->WUnlatch();
        buffer_pool_manager_->UnpinPage(pg->GetPageId(), false);
      }
      transaction->GetPageSet()->clear();
    }
  }

  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());

  int size = leaf_node->GetSize();
  if (leaf_node->RemoveAndDeleteRecord(key, comparator_) == size) {
    if (is_root_latched) {
      is_root_latched = false;
      rwlatch_.WUnlock();
    }

    for (Page *pg : *transaction->GetPageSet()) {
      pg->WUnlatch();
      buffer_pool_manager_->UnpinPage(pg->GetPageId(), false);
    }
    transaction->GetPageSet()->clear();

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    return;
  }

  if (leaf_node->GetSize() < leaf_node->GetMinSize()) {
    AdjustLeafNode(leaf_node, key, transaction);
  }

  if (is_root_latched) {
    is_root_latched = false;
    rwlatch_.WUnlock();
  }

  for (Page *pg : *transaction->GetPageSet()) {
    pg->WUnlatch();
    buffer_pool_manager_->UnpinPage(pg->GetPageId(), false);
  }
  transaction->GetPageSet()->clear();

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  for (auto page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
    // std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  transaction->GetDeletedPageSet()->clear();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustLeafNode(LeafPage *leaf_node, const KeyType &key, Transaction *transaction) {
  if (leaf_node->IsRootPage()) {
    if (leaf_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);

      transaction->AddIntoDeletedPageSet(leaf_node->GetPageId());
    }

    return;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(leaf_node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int index = parent_node->KeyIndex(key, comparator_);

  if (index - 1 >= 0) {
    Page *left_neigh_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index - 1));
    left_neigh_page->WLatch();
    auto left_neigh_node = reinterpret_cast<LeafPage *>(left_neigh_page->GetData());

    if (left_neigh_node->GetSize() > left_neigh_node->GetMinSize()) {
      left_neigh_node->MoveLastToFrontOf(leaf_node);
      parent_node->SetKeyAt(index, leaf_node->KeyAt(0));
    } else {
      leaf_node->MoveAllTo(left_neigh_node);
      left_neigh_node->SetNextPageId(leaf_node->GetNextPageId());
      parent_node->Remove(index);
      transaction->AddIntoDeletedPageSet(leaf_node->GetPageId());
    }

    left_neigh_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_neigh_page->GetPageId(), true);

  } else if (index + 1 < parent_node->GetSize()) {
    Page *right_neigh_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index + 1));
    right_neigh_page->WLatch();
    auto right_neigh_node = reinterpret_cast<LeafPage *>(right_neigh_page->GetData());

    if (right_neigh_node->GetSize() > right_neigh_node->GetMinSize()) {
      right_neigh_node->MoveFirstToEndOf(leaf_node);
      parent_node->SetKeyAt(index + 1, right_neigh_node->KeyAt(0));
    } else {
      right_neigh_node->MoveAllTo(leaf_node);
      leaf_node->SetNextPageId(right_neigh_node->GetNextPageId());
      parent_node->Remove(index + 1);
      transaction->AddIntoDeletedPageSet(right_neigh_node->GetPageId());
    }

    right_neigh_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_neigh_page->GetPageId(), true);
  }

  if (parent_node->GetSize() < parent_node->GetMinSize()) {
    AdjustInternalNode(parent_node, key, transaction);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustInternalNode(InternalPage *internal_node, const KeyType &key, Transaction *transaction) {
  if (internal_node->IsRootPage()) {
    if (internal_node->GetSize() == 1) {
      page_id_t child_page_id = internal_node->RemoveAndReturnOnlyChild();
      Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
      auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);

      root_page_id_ = child_page_id;
      UpdateRootPageId(0);

      transaction->AddIntoDeletedPageSet(internal_node->GetPageId());
    }

    return;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(internal_node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int index = parent_node->KeyIndex(key, comparator_);

  if (index - 1 >= 0) {
    Page *left_neigh_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index - 1));
    left_neigh_page->WLatch();
    auto left_neigh_node = reinterpret_cast<InternalPage *>(left_neigh_page->GetData());

    if (left_neigh_node->GetSize() > left_neigh_node->GetMinSize()) {
      left_neigh_node->MoveLastToFrontOf(internal_node, parent_node->KeyAt(index), buffer_pool_manager_);
      parent_node->SetKeyAt(index, internal_node->KeyAt(0));
    } else {
      internal_node->MoveAllTo(left_neigh_node, parent_node->KeyAt(index), buffer_pool_manager_);
      parent_node->Remove(index);
      transaction->AddIntoDeletedPageSet(internal_node->GetPageId());
    }

    left_neigh_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_neigh_page->GetPageId(), true);

  } else if (index + 1 < parent_node->GetSize()) {
    Page *right_neigh_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index + 1));
    right_neigh_page->WLatch();
    auto right_neigh_node = reinterpret_cast<InternalPage *>(right_neigh_page->GetData());

    if (right_neigh_node->GetSize() > right_neigh_node->GetMinSize()) {
      right_neigh_node->MoveFirstToEndOf(internal_node, parent_node->KeyAt(index + 1), buffer_pool_manager_);
      parent_node->SetKeyAt(index + 1, right_neigh_node->KeyAt(0));
    } else {
      right_neigh_node->MoveAllTo(internal_node, parent_node->KeyAt(index + 1), buffer_pool_manager_);
      parent_node->Remove(index + 1);
      transaction->AddIntoDeletedPageSet(right_neigh_node->GetPageId());
    }

    right_neigh_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_neigh_page->GetPageId(), true);
  }

  if (parent_node->GetSize() < parent_node->GetMinSize()) {
    AdjustInternalNode(parent_node, key, transaction);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) -> bool {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  rwlatch_.RLock();
  Page *page = FindLeafPage(KeyType(), 1);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  rwlatch_.RLock();
  Page *page = FindLeafPage(key);
  int index = reinterpret_cast<LeafPage *>(page->GetData())->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  rwlatch_.RLock();
  Page *page = FindLeafPage(KeyType(), 2);
  int index = reinterpret_cast<LeafPage *>(page->GetData())->GetSize();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, index);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, int option) -> Page * {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  page_id_t page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  page->RLatch();
  rwlatch_.RUnlock();

  auto *curr_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!curr_node->IsLeafPage()) {
    auto *node = reinterpret_cast<InternalPage *>(page->GetData());
    if (option == 0) {
      page_id = node->Lookup(key, comparator_);
    } else if (option == 1) {
      page_id = node->ValueAt(0);
    } else if (option == 2) {
      page_id = node->ValueAt(node->GetSize() - 1);
    }

    Page *child_page = buffer_pool_manager_->FetchPage(page_id);
    child_page->RLatch();

    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    page = child_page;
    curr_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
