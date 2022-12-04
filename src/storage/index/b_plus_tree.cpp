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
  if (IsEmpty()) {
    return false;
  }

  page_id_t leaf_page_id = GetLeafPage(key);
  Page *page = buffer_pool_manager_->FetchPage(leaf_page_id);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // ToString(leaf_page, buffer_pool_manager_);

  int sz = leaf_page->GetSize();
  bool found = false;
  for (int i = 0; i < sz; i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      found = true;
    }
  }

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return found;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) -> page_id_t {
  page_id_t page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  auto bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!bpt_page->IsLeafPage()) {
    // Search in internal pages until reach a leaf page
    auto internal_page = static_cast<InternalPage *>(bpt_page);
    int sz = internal_page->GetSize();
    for (int i = 1; i <= sz; i++) {
      // returns true if lhs > rhs
      // std:: cout << internal_page -> KeyAt(i) << ", " << key << ", Comparator Output: " << comparator_(internal_page
      // -> KeyAt(i), key) << std::endl;
      if (i == sz || comparator_(internal_page->KeyAt(i), key) == 1) {
        buffer_pool_manager_->UnpinPage(page_id, false);
        page_id = internal_page->ValueAt(i - 1);
        page = buffer_pool_manager_->FetchPage(page_id);
        bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
        break;
      }
    }
  }

  return bpt_page->GetPageId();
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
  if (IsEmpty()) {
    // Allocate new page for root page
    page_id_t page_id;
    Page *page = buffer_pool_manager_->NewPage(&page_id);
    root_page_id_ = page_id;
    UpdateRootPageId(1);

    auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    leaf_page->Init(page_id, page_id, leaf_max_size_);
    leaf_page->SetMappingAt(0, key, value);
    leaf_page->IncreaseSize(1);
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }

  page_id_t leaf_page_id = GetLeafPage(key);
  Page *page = buffer_pool_manager_->FetchPage(leaf_page_id);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  // Find out if inserting a duplicate key
  int sz = leaf_page->GetSize();
  for (int i = 0; i < sz; i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return false;
    }
  }

  InsertInLeaf(leaf_page, key, value);
  if (leaf_page->GetSize() >= leaf_max_size_) {
    // Create new page
    page_id_t page_id;
    Page *page = buffer_pool_manager_->NewPage(&page_id);
    auto new_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    new_leaf_page->Init(page_id, leaf_page->GetParentPageId(), leaf_max_size_);
    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(page_id);

    int middle = Ceiling(leaf_max_size_);
    leaf_page->SetSize(middle);
    new_leaf_page->SetSize(leaf_max_size_ - middle);

    for (int i = 0; i < leaf_max_size_ - middle; i++) {
      new_leaf_page->SetMappingAt(i, leaf_page->KeyAt(middle + i), leaf_page->ValueAt(middle + i));
    }

    InsertInParent(leaf_page, new_leaf_page, new_leaf_page->KeyAt(0));
    buffer_pool_manager_->UnpinPage(page_id, true);
  }

  buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInLeaf(LeafPage *leaf_page, const KeyType &key, const ValueType &value) {
  int sz = leaf_page->GetSize();

  int i = 0;
  // returns true if lhs > rhs
  // std:: cout << leaf_page -> KeyAt(i) << ", " << key << ", Comparator Output: " << comparator_(leaf_page -> KeyAt(i),
  // key) << std::endl;
  while (i < sz && comparator_(leaf_page->KeyAt(i), key) != 1) {
    i++;
  }
  // Insert here and re-place all the key-value pairs after this position
  MappingType copy[leaf_max_size_];
  MappingType *arr = leaf_page->GetData();
  // memcpy(copy, arr, sizeof(MappingType) * sz);
  for (int j = 0; j < sz; j++) {
    copy[j] = arr[j];
  }

  arr[i] = MappingType(key, value);
  for (int j = i + 1; j < sz + 1; j++) {
    arr[j] = copy[j - 1];
  }
  leaf_page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_page, BPlusTreePage *right_page, const KeyType &key) {
  if (left_page->IsRootPage()) {
    page_id_t page_id;
    Page *page = buffer_pool_manager_->NewPage(&page_id);
    auto root_page = reinterpret_cast<InternalPage *>(page->GetData());
    root_page->Init(page_id, page_id, internal_max_size_);
    root_page->SetMappingAt(0, key, left_page->GetPageId());
    root_page->SetMappingAt(1, key, right_page->GetPageId());
    root_page->SetSize(2);
    root_page_id_ = page_id;
    UpdateRootPageId(0);
    left_page->SetParentPageId(page_id);
    right_page->SetParentPageId(page_id);
    buffer_pool_manager_->UnpinPage(page_id, true);
    return;
  }

  page_id_t parent_page_id = left_page->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_page = reinterpret_cast<InternalPage *>(page->GetData());

  InsertInNonLeaf(parent_page, key, right_page->GetPageId());

  if (!parent_page->IsFull()) {
    parent_page->IncreaseSize(1);
    right_page->SetParentPageId(parent_page_id);
  } else {
    int middle = Ceiling(internal_max_size_);

    page_id_t page_id;
    Page *page = buffer_pool_manager_->NewPage(&page_id);
    auto new_internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    new_internal_page->Init(page_id, parent_page->GetParentPageId(), internal_max_size_);

    parent_page->SetSize(middle);
    new_internal_page->SetSize(internal_max_size_ - middle + 1);

    for (int i = 0; i < internal_max_size_ - middle + 1; i++) {
      new_internal_page->SetMappingAt(i, parent_page->KeyAt(middle + i), parent_page->ValueAt(middle + i));
      Page *page = buffer_pool_manager_->FetchPage(new_internal_page->ValueAt(i));
      auto bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
      bpt_page->SetParentPageId(page_id);
      buffer_pool_manager_->UnpinPage(new_internal_page->ValueAt(i), true);
    }

    InsertInParent(parent_page, new_internal_page, parent_page->KeyAt(middle));
    buffer_pool_manager_->UnpinPage(page_id, true);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInNonLeaf(InternalPage *internal_page, const KeyType &key, const page_id_t &value) {
  int sz = internal_page->GetSize();
  int i = 1;
  // returns true if lhs > rhs
  while (i < sz && comparator_(internal_page->KeyAt(i), key) <= 0) {
    i++;
  }
  // BUSTUB_ASSERT(comparator_(parent_page -> KeyAt(i), key) == 0, "Left page not found in parent page");
  // Insert here and re-place all the key-value pairs after this position
  std::pair<KeyType, page_id_t> copy[internal_max_size_];
  std::pair<KeyType, page_id_t> *arr = internal_page->GetData();
  // memcpy(copy, arr, sizeof(std::pair<KeyType, page_id_t>) * sz);
  for (int j = 0; j < sz; j++) {
    copy[j] = arr[j];
  }

  arr[i] = std::pair<KeyType, page_id_t>(key, value);
  for (int j = i + 1; j < sz + 1; j++) {
    arr[j] = copy[j - 1];
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Ceiling(int sz) -> int { return (sz % 2 == 0) ? sz / 2 : sz / 2 + 1; }

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
  if (IsEmpty()) {
    return;
  }

  page_id_t leaf_page_id = GetLeafPage(key);
  Page *page = buffer_pool_manager_->FetchPage(leaf_page_id);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  RemoveEntryInLeaf(key, leaf_page);
  buffer_pool_manager_->UnpinPage(leaf_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntryInLeaf(const KeyType &key, LeafPage *leaf_page) {
  auto arr = leaf_page->GetData();
  int sz = leaf_page->GetSize();
  int i = 0;
  while (i < sz && comparator_(arr[i].first, key) != 0) {
    i++;
  }
  for (; i < sz; i++) {
    arr[i] = arr[i + 1];
  }
  leaf_page->SetSize(--sz);

  if (leaf_page->IsRootPage() && sz == 1) {
    root_page_id_ = leaf_page->GetPageId();
    UpdateRootPageId(0);
  } else if (!leaf_page->IsRootPage() && sz < Ceiling(leaf_max_size_)) {
    KeyType middle_key;
    page_id_t sibling;
    int index;  // index of middle_key in parent
    Page *page = buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId());
    auto parent_page = reinterpret_cast<InternalPage *>(page->GetData());

    bool is_next = GetPrevOrNextSibiling(parent_page, sibling, key, middle_key, index);
    Page *sib_page = buffer_pool_manager_->FetchPage(sibling);
    auto sibling_page = reinterpret_cast<LeafPage *>(sib_page->GetData());

    if (sz + sibling_page->GetSize() <= leaf_max_size_) {
      if (is_next) {
        // swap
        LeafPage *tmp = leaf_page;
        leaf_page = sibling_page;
        sibling_page = tmp;
      }

      auto arr1 = sibling_page->GetData();
      auto arr2 = leaf_page->GetData();
      sz = sibling_page->GetSize();
      for (int i = sz; i < sz + leaf_page->GetSize(); i++) {
        arr1[i] = arr2[i - sz];
      }

      sibling_page->IncreaseSize(leaf_page->GetSize());
      sibling_page->SetNextPageId(leaf_page->GetNextPageId());

      RemoveEntryInNonLeaf(middle_key, parent_page);

      buffer_pool_manager_->UnpinPage(leaf_page->GetParentPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling, true);
    } else {
      // Borrow one node from sibling
      int sz = sibling_page->GetSize();
      auto arr = sibling_page->GetData();
      if (is_next) {
        KeyType first_key = sibling_page->KeyAt(0);
        ValueType first_val = sibling_page->ValueAt(0);
        // Delete first value and first valid key from sibling page
        for (int i = 0; i < sz - 1; i++) {
          arr[i] = arr[i + 1];
        }
        sibling_page->SetSize(--sz);
        // if (sz < Ceiling(internal_max_size_)) {
        // CoalesceNonLeaf(sibling_page);
        // }

        // Insert the value and middle_key to internal page
        // InsertInNonLeaf(internal_page, first_validKey, first_page);
        leaf_page->SetMappingAt(leaf_page->GetSize(), first_key, first_val);
        leaf_page->IncreaseSize(1);

        parent_page->SetKeyAt(index, first_key);
      } else {
        // Delete last value and key from sibling page
        KeyType last_key = sibling_page->KeyAt(sz);
        ValueType last_val = sibling_page->ValueAt(sz);
        RemoveEntryInLeaf(last_key, sibling_page);
        InsertInLeaf(leaf_page, last_key, last_val);
        parent_page->SetKeyAt(index + 1, last_key);
      }
    }

    buffer_pool_manager_->UnpinPage(leaf_page->GetParentPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPrevOrNextSibiling(InternalPage *parent_page, page_id_t &sibling, const KeyType &key,
                                           KeyType &middle_key, int &index) -> bool {
  bool is_next = true;
  auto arr = parent_page->GetData();
  int i = 1;
  int sz = parent_page->GetSize();
  while (i < sz && comparator_(arr[i].first, key) <= 0) {
    i++;
  }

  // If key is largest in parent node
  if (i == sz) {
    index = sz - 2;
    middle_key = arr[sz - 1].first;
    sibling = arr[index].second;
    is_next = false;
  } else {
    index = i;
    is_next = true;
    middle_key = arr[index].first;
    sibling = arr[index].second;
  }

  return is_next;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntryInNonLeaf(const KeyType &key, InternalPage *internal_page) {
  auto arr = internal_page->GetData();
  int sz = internal_page->GetSize();

  int i = 1;
  while (i < sz && comparator_(arr[i].first, key) != 0) {
    i++;
  }
  for (; i < sz; i++) {
    arr[i] = arr[i + 1];
  }
  internal_page->SetSize(--sz);

  if (internal_page->IsRootPage() && sz == 1) {
    root_page_id_ = internal_page->ValueAt(0);
    UpdateRootPageId(0);
  } else if (!internal_page->IsRootPage() && sz < Ceiling(internal_max_size_)) {
    CoalesceNonLeaf(key, internal_page);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceNonLeaf(const KeyType &key, InternalPage *internal_page) {
  KeyType middle_key;
  page_id_t sibling;
  int index;  // index of middle_key in parent
  Page *page = buffer_pool_manager_->FetchPage(internal_page->GetParentPageId());
  auto parent_page = reinterpret_cast<InternalPage *>(page->GetData());

  bool is_next = GetPrevOrNextSibiling(parent_page, sibling, key, middle_key, index);
  Page *sib_page = buffer_pool_manager_->FetchPage(sibling);
  auto sibling_page = reinterpret_cast<InternalPage *>(sib_page->GetData());

  if (internal_page->GetSize() + sibling_page->GetSize() <= internal_max_size_) {
    if (is_next) {
      // swap
      InternalPage *tmp = internal_page;
      internal_page = sibling_page;
      sibling_page = tmp;
    }

    auto arr1 = sibling_page->GetData();
    auto arr2 = internal_page->GetData();
    int sz = sibling_page->GetSize();
    for (int i = sz; i < sz + internal_page->GetSize(); i++) {
      arr1[i] = arr2[i - sz];
      // update parent page id
      Page *page = buffer_pool_manager_->FetchPage(arr2[i - sz].second);
      auto bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
      bpt_page->SetParentPageId(sibling_page->GetPageId());
      buffer_pool_manager_->UnpinPage(arr2[i - sz].second, true);
    }
    arr1[sz].first = middle_key;
    sibling_page->IncreaseSize(internal_page->GetSize());

    RemoveEntryInNonLeaf(middle_key, parent_page);

  } else {
    // borrow one node from sibling
    int sz = sibling_page->GetSize();
    auto arr = sibling_page->GetData();
    if (is_next) {
      KeyType first_key = sibling_page->KeyAt(1);
      page_id_t first_page = sibling_page->ValueAt(0);
      // Delete first value and first valid key from sibling page
      for (int i = 0; i < sz - 1; i++) {
        arr[i] = arr[i + 1];
      }
      sibling_page->SetSize(--sz);
      // if (sz < Ceiling(internal_max_size_)) {
      // CoalesceNonLeaf(sibling_page);
      // }

      // Insert the value and middle_key to internal page
      // InsertInNonLeaf(internal_page, first_validKey, first_page);
      internal_page->SetMappingAt(internal_page->GetSize(), middle_key, first_page);
      internal_page->IncreaseSize(1);

      parent_page->SetKeyAt(index, first_key);
    } else {
      // Delete last value and key from sibling page
      KeyType last_key = sibling_page->KeyAt(sz);
      page_id_t last_page = sibling_page->ValueAt(sz);
      RemoveEntryInNonLeaf(last_key, sibling_page);
      InsertInNonLeaf(internal_page, last_key, last_page);
      parent_page->SetKeyAt(index + 1, last_key);
    }
  }

  buffer_pool_manager_->UnpinPage(internal_page->GetParentPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetIndex(page_id_t page_id, const KeyType &key) -> int {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto arr = leaf_page->GetData();
  int size = leaf_page->GetSize();
  for (int i = 0; i < size; i++) {
    if (comparator_(arr[i].first, key) == 0) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindMinLeaf() -> page_id_t {
  page_id_t page_id = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  auto bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!bpt_page->IsLeafPage()) {
    // Search in internal pages until reach a leaf page
    auto internal_page = static_cast<InternalPage *>(bpt_page);
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = internal_page->ValueAt(0);
    page = buffer_pool_manager_->FetchPage(page_id);
    bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  return bpt_page->GetPageId();
}

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
  page_id_t page_id = FindMinLeaf();
  return INDEXITERATOR_TYPE(page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t page_id = GetLeafPage(key);
  int index = GetIndex(page_id, key);
  return INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
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
  out.flush();
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
