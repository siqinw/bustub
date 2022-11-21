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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  return root_page_id_ == INVALID_PAGE_ID; 
}
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
  Page* page = buffer_pool_manager_ -> FetchPage(leaf_page_id);
  auto leafPage = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*> (page->GetData());
  // ToString(leafPage, buffer_pool_manager_);
  
  int sz = leafPage -> GetSize();
  bool found = false;
  for (int i=0; i<sz; i++) {
    if (comparator_(leafPage -> KeyAt(i), key) == 0) {
      result -> push_back(leafPage -> ValueAt(i));
      found = true;
    }
  }

  buffer_pool_manager_ -> UnpinPage(leafPage->GetPageId(), false);
  return found;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key)  -> page_id_t {
  page_id_t page_id = root_page_id_;
  Page* page = buffer_pool_manager_ -> FetchPage(page_id);
  auto bptPage = reinterpret_cast<BPlusTreePage*> (page->GetData());
  
  while (!bptPage -> IsLeafPage()) {
    // Search in internal pages until reach a leaf page
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internalPage = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*> (bptPage);
    int sz = internalPage -> GetSize();
    for (int i=1; i<=sz; i++) {
      // returns true if lhs > rhs
      // std:: cout << internalPage -> KeyAt(i) << ", " << key << ", Comparator Output: " << comparator_(internalPage -> KeyAt(i), key) << std::endl;
      if (i == sz || comparator_(internalPage -> KeyAt(i), key) == 1) {
          buffer_pool_manager_ -> UnpinPage(page_id, false);
          page_id = internalPage -> ValueAt(i-1);
          page = buffer_pool_manager_ -> FetchPage(page_id);
          bptPage = reinterpret_cast<BPlusTreePage*> (page->GetData());
          break;
      }
    }
  }

  return bptPage -> GetPageId();
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
    Page* page = buffer_pool_manager_ -> NewPage(&page_id);
    root_page_id_ = page_id;
    UpdateRootPageId(1);

    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* leafPage = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*> (page->GetData());;
    leafPage -> Init(page_id, page_id, leaf_max_size_);
    leafPage -> SetMappingAt(0, key, value);
    leafPage -> IncreaseSize(1);
    buffer_pool_manager_ -> UnpinPage(page_id, true);
    return true;
  }

  page_id_t leaf_page_id = GetLeafPage(key);
  Page* page = buffer_pool_manager_ -> FetchPage(leaf_page_id);
  auto leafPage = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*> (page->GetData());
  
  // Find out if inserting a duplicate key
  int sz = leafPage -> GetSize();
  for (int i=0; i<sz; i++) {
    if (comparator_(leafPage -> KeyAt(i), key) == 0) {
      buffer_pool_manager_ -> UnpinPage(leafPage->GetPageId(), false);
      return false;
    }
  }

  InsertInLeaf(leafPage, key, value);
  if (leafPage -> GetSize() == leaf_max_size_) {
    // Create new page
    page_id_t page_id;
    Page* page = buffer_pool_manager_ -> NewPage(&page_id);    
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* newLeafPage = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*> (page->GetData());;
    newLeafPage -> Init(page_id, leafPage->GetParentPageId(), leaf_max_size_);
    leafPage -> SetNextPageId(page_id);
    newLeafPage -> SetNextPageId(leafPage -> GetNextPageId());

    int middle = ceiling(leaf_max_size_);
    leafPage -> SetSize(middle);
    newLeafPage -> SetSize(leaf_max_size_-middle);

    for (int i=0; i<leaf_max_size_-middle; i++) {
      newLeafPage -> SetMappingAt(i, leafPage -> KeyAt(middle+i), leafPage -> ValueAt(middle+i));
    }

    InsertInParent(leafPage, newLeafPage, newLeafPage->KeyAt(0));
    buffer_pool_manager_ -> UnpinPage(page_id, true);
  }
  
  buffer_pool_manager_ -> UnpinPage(leaf_page_id, true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInLeaf(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* leafPage, 
const KeyType &key, const ValueType &value) {
  int sz = leafPage -> GetSize();

  int i=0;
  // returns true if lhs > rhs
  // std:: cout << leafPage -> KeyAt(i) << ", " << key << ", Comparator Output: " << comparator_(leafPage -> KeyAt(i), key) << std::endl;
  while (i<sz && comparator_(leafPage -> KeyAt(i), key) != 1) {
    i++;
  }
  // Insert here and re-place all the key-value pairs after this position
  MappingType copy[sz];
  MappingType* arr = leafPage -> GetData();
  memcpy(copy, arr, sizeof(MappingType)*sz);

  arr[i] = MappingType(key, value);
  for (int j=i+1; j<sz+1; j++) {
    arr[j] = copy[j-1];
  }
  leafPage -> IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage* leftPage, BPlusTreePage* rightPage, const KeyType &key) {
  if (leftPage -> IsRootPage()) {
    page_id_t page_id;
    Page* page = buffer_pool_manager_ -> NewPage(&page_id);    
    auto rootPage = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*> (page->GetData());
    rootPage -> Init(page_id, page_id, internal_max_size_);
    rootPage -> SetMappingAt(0, key, leftPage -> GetPageId());
    rootPage -> SetMappingAt(1, key, rightPage -> GetPageId());
    rootPage -> SetSize(2);
    root_page_id_ = page_id;
    UpdateRootPageId(0);
    leftPage -> SetParentPageId(page_id);
    rightPage -> SetParentPageId(page_id);
    buffer_pool_manager_ -> UnpinPage(page_id, true);
    return;    
  }

  page_id_t parent_page_id = leftPage -> GetParentPageId();
  Page* page = buffer_pool_manager_ -> FetchPage(parent_page_id);
  auto parentPage = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*> (page->GetData());
  
  InsertInNonLeaf(parentPage, key, rightPage -> GetPageId());

  if (!parentPage -> IsFull()) {
    parentPage -> IncreaseSize(1);
    rightPage -> SetParentPageId(parent_page_id);
  } else {
      int middle = ceiling(internal_max_size_);

      page_id_t page_id;
      Page* page = buffer_pool_manager_ -> NewPage(&page_id);    
      auto newInternalPage = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*> (page->GetData());;
      newInternalPage -> Init(page_id, parentPage->GetParentPageId(), internal_max_size_);

      parentPage -> SetSize(middle+1);
      newInternalPage -> SetSize(internal_max_size_-middle);

      // newInternalPage -> SetMappingAt(0, parentPage -> KeyAt(middle+1), parentPage -> ValueAt(middle+1));
      for (int i=0; i<internal_max_size_-middle; i++) {
        newInternalPage -> SetMappingAt(i, parentPage -> KeyAt(middle+1+i), parentPage -> ValueAt(middle+1+i));
        Page *page = buffer_pool_manager_ -> FetchPage(newInternalPage -> ValueAt(i));
        auto bptPage = reinterpret_cast<BPlusTreePage*> (page->GetData());
        bptPage -> SetParentPageId(page_id);
        buffer_pool_manager_ -> UnpinPage(newInternalPage -> ValueAt(i), true);
      }

      InsertInParent(parentPage, newInternalPage, parentPage->KeyAt(middle+1));
      buffer_pool_manager_ -> UnpinPage(page_id, true);
  }
  buffer_pool_manager_ -> UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInNonLeaf(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internalPage,
const KeyType &key, const page_id_t &value) {
    int sz = internalPage -> GetSize();
    int i=1;
    // returns true if lhs > rhs
    while (i<sz && comparator_(internalPage -> KeyAt(i), key) <= 0) {
      i++;
    }
    // BUSTUB_ASSERT(comparator_(parentPage -> KeyAt(i), key) == 0, "Left page not found in parent page");
    // Insert here and re-place all the key-value pairs after this position
    std::pair<KeyType, page_id_t> copy[sz];
    std::pair<KeyType, page_id_t>* arr = internalPage -> GetData();
    memcpy(copy, arr, sizeof(std::pair<KeyType, page_id_t>)*sz);

    arr[i] = std::pair<KeyType, page_id_t>(key, value);
    for (int j=i+1; j<sz+1; j++) {
      arr[j] = copy[j-1];
    }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ceiling(int sz) -> int {
  return (sz%2 == 0) ? sz/2 : sz/2+1;
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
