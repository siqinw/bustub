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
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int offset, BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), offset_(offset), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  Page *page = buffer_pool_manager_->FetchPage(page_id_);
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  m_ = {leaf_page->KeyAt(offset_), leaf_page->ValueAt(offset_)};
  buffer_pool_manager_->UnpinPage(page_id_, false);
  return m_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  offset_++;
  Page *page = buffer_pool_manager_->FetchPage(page_id_);
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  int size = leaf_page->GetSize();
  page_id_t tmp = page_id_;
  if (offset_ >= size) {
    page_id_ = leaf_page->GetNextPageId();
    offset_ = 0;
  }
  buffer_pool_manager_->UnpinPage(tmp, false);
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  page_id_t page_id;
  int offset;
  itr.GetParam(page_id, offset);
  return page_id == page_id_ && offset == offset_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::GetParam(page_id_t &page_id, int &offset) const {
  page_id = page_id_;
  offset = offset_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
