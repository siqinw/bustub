//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock lock(latch_);
  Page *page = nullptr;
  frame_id_t frame_id = INVALID_PAGE_ID;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    bool evicted = replacer_->Evict(&frame_id);
    // No page evictable
    if (!evicted) {
      page_id = nullptr;
      return nullptr;
    }

    page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_->Remove(page->page_id_);
  }

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  *page_id = AllocatePage();
  page_table_->Insert(*page_id, frame_id);

  page = &pages_[frame_id];
  ResetPage(page);
  page->page_id_ = *page_id;
  page->pin_count_++;
  disk_manager_->WritePage(page->GetPageId(), page->GetData());
  // std::cout << "Created New Page " << *page_id << std::endl;
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock lock(latch_);
  // std::cout << "Fetching Page " << page_id << std::endl;
  frame_id_t frame_id = INVALID_PAGE_ID;

  if (!page_table_->Find(page_id, frame_id)) {
    // Not in memory
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      bool evicted = replacer_->Evict(&frame_id);
      // No page evictable
      if (!evicted) {
        return nullptr;
      }

      Page *page = &pages_[frame_id];
      if (page->IsDirty()) {
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
      }
      page_table_->Remove(page->page_id_);
    }

    Page *page = &pages_[frame_id];
    ResetPage(page);
    page->page_id_ = page_id;
    disk_manager_->ReadPage(page_id, page->data_);
    page_table_->Insert(page_id, frame_id);
  }

  Page *page = &pages_[frame_id];
  page->pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);
  // std::cout << "Unpin Page " << page_id << " Dirty: " << is_dirty << std::endl;
  frame_id_t frame_id = INVALID_PAGE_ID;

  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  Page *page = &pages_[frame_id];
  page->is_dirty_ |= is_dirty;
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
    if (page->is_dirty_) {
      disk_manager_->WritePage(page_id, page->data_);
      page->is_dirty_ = false;
    }
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  // std::cout << "Flushing Page " << page_id << std::endl;
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "Invalid Page ID");
  frame_id_t frame_id = INVALID_PAGE_ID;

  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (page_id_t page_id = 0; page_id < next_page_id_; page_id++) {
    frame_id_t frame_id;
    if (page_table_->Find(page_id, frame_id)) {
      BUSTUB_ASSERT(FlushPgImp(page_id) != false, "Fail to flush all pages");
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  frame_id_t frame_id = INVALID_PAGE_ID;

  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);

  ResetPage(&pages_[frame_id]);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::ResetPage(Page *page) {
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
}

}  // namespace bustub
