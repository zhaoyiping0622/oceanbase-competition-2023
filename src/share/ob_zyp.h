#pragma once

#include"lib/string/ob_string.h"
#include"lib/container/ob_array.h"
#include"common/object/ob_object.h"
#include"common/row/ob_row.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/allocator/ob_safe_arena.h"

extern __thread bool zyp_come;
using zyp_string = oceanbase::common::ObString;
extern zyp_string zyp_extra_info;

#define ZYP_LOG_INFO(...) if(zyp_come) LOG_INFO(__VA_ARGS__)

void zyp_enable();
bool zyp_enabled();
void zyp_disable();
void zyp_set_extra(const zyp_string&s);

void zyp_unlimit_log(const char* buf, size_t size);

class ConcurrentPageArena {
public:
  // using SpinRWLock;
  void* alloc(size_t size) {
    oceanbase::common::SpinWLockGuard guard(lock_);
    return alloc_.alloc(size);
  }
  void free() {
    oceanbase::common::SpinWLockGuard guard(lock_);
    return alloc_.free();
  }
private:
  oceanbase::common::PageArena<> alloc_;
  oceanbase::common::SpinRWLock lock_;
};


namespace oceanbase {
namespace common {
  class ObDatum;
}
}

class ZypRow {
  public:
    using ObDatum = oceanbase::common::ObDatum;
    using ObObj = oceanbase::common::ObObj;
    using ObNewRow = oceanbase::common::ObNewRow;
    template<typename T>
    using ObArray = oceanbase::common::ObArray<T>;
    using ObString = oceanbase::common::ObString;
    virtual void init_objs() = 0;
    virtual ObObj* get_cells() const = 0;
    virtual ObDatum* get_datums() const = 0;
    virtual size_t get_cells_cnt() const = 0;
    oceanbase::common::ObNewRow new_row();
    virtual ObArray<ZypRow*> gen_core_rows(std::atomic_long &row_id) = 0;
    virtual ~ZypRow() {}
    void add_varchar(ObObj* obj, ObDatum* datum, const ObString&s);
    void add_varbinary(ObObj* obj, ObDatum* datum, const ObString&s);
    void add_longtext(ObObj* obj, ObDatum* datum, const ObString&s);
    void add_bigint(ObObj* obj, ObDatum* datum, int64_t v);
    void add_tinyint(ObObj* obj, ObDatum* datum, int8_t v);
    void add_bigunsigned(ObObj* obj, ObDatum* datum, uint64_t v);
    void add_null(ObObj* obj, ObDatum* datum);
    void add_timestamp(ObObj* obj, ObDatum* datum, int64_t timestamp);
    size_t to_string(const char* buf, size_t size) const {return 0;}
    static ConcurrentPageArena allocator;
};

class ZypInsertInfo {
public:
  template<typename T>
  using ObArray = oceanbase::common::ObArray<T>;
  using LightyQueue = oceanbase::LightyQueue;
  ZypInsertInfo(LightyQueue& queue):queue_(queue) {}
  ObArray<ZypRow*> get_row(int64_t count);
  long count() { return queue_.size(); }
private:
  LightyQueue& queue_;
};

extern thread_local ZypInsertInfo* zyp_insert_info;
extern thread_local ZypRow** zyp_row_head;
extern thread_local ZypRow** zyp_row_tail;
extern thread_local ZypRow** zyp_current_row;
extern thread_local bool zyp_inited;
