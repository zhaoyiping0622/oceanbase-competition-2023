#pragma once

#include"lib/string/ob_string.h"
#include"lib/container/ob_array.h"
#include"common/object/ob_object.h"
#include"common/row/ob_row.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/allocator/ob_safe_arena.h"
#include "share/ob_srv_rpc_proxy.h"
#include <set>

extern __thread bool zyp_come;
using zyp_string = oceanbase::common::ObString;
extern zyp_string zyp_extra_info;

#define ZYP_LOG_INFO(...) if(zyp_come) LOG_INFO(__VA_ARGS__)

#define ZYP_LOCAL_ALLOC(type, size) (type*)local_allocator->alloc(size) // OB_NEW(type,label,__VA_ARGS__)
#define ZYP_LOCAL_FREE(p) // OB_NEW(type,label,__VA_ARGS__)
#define ZYP_LOCAL_NEW(type,label,...) local_allocator->alloc<type>(__VA_ARGS__) // OB_NEW(type,label,__VA_ARGS__)
#define ZYP_LOCAL_DELETE(type,label,ptr) local_allocator->free<type>(ptr) // OB_DELETE(type,label,point,__VA_ARGS__)

void zyp_enable();
bool zyp_enabled();
void zyp_disable();
void zyp_set_extra(const zyp_string&s);

void zyp_unlimit_log(const char* buf, size_t size);

class ConcurrentPageArena {
public:
  template<typename T, typename ...Args>
  void alloc(T*& p, Args&& ...args) {
    p = (T*) alloc(sizeof(T));
    new (p) T(std::forward<Args>(args)...);
  }
  // using SpinRWLock;
  void* alloc(size_t size) {
    lock_.wrlock();
    auto* ret = alloc_.alloc(size);
    lock_.wrunlock();
    // memset(ret, 0, size);
    return ret;
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
    template<typename T>
    using ObArray = oceanbase::common::ObArray<T>;
    using ObString = oceanbase::common::ObString;
    virtual void init_datums() = 0;
    virtual ObDatum* get_datums() = 0;
    virtual size_t get_cells_cnt() const = 0;
    virtual ObArray<ZypRow*> gen_core_rows(std::atomic_long &row_id) = 0;
    virtual ~ZypRow() {}
    static void add_varchar(ObDatum* datum, const ObString&s);
    static void add_varbinary(ObDatum* datum, const ObString&s);
    static void add_longtext(ObDatum* datum, const ObString&s);
    static void add_bigint(ObDatum* datum, int64_t v);
    static void add_tinyint(ObDatum* datum, int8_t v);
    static void add_bigunsigned(ObDatum* datum, uint64_t v);
    static void add_null(ObDatum* datum);
    static void add_timestamp(ObDatum* datum, int64_t timestamp);
    size_t to_string(const char* buf, size_t size) const {return 0;}
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

class ZypAllocator {
public:
  template<typename T, typename ...Args>
  void alloc(T*& p, Args&& ...args) {
    p = (T*) alloc(sizeof(T));
    new (p) T(std::forward<Args>(args)...);
  }
  template<typename T, typename ...Args>
  T* alloc(Args&& ...args) {
    auto* p = (T*) alloc(sizeof(T));
    new (p) T(std::forward<Args>(args)...);
    return p;
  }
  template<typename T>
  void free(T* t) {
    t->~T();
  }
  // using SpinRWLock;
  void* alloc(size_t size) {
    return alloc_.alloc(size);
  }
  void free() {
    return alloc_.free();
  }
private:
  oceanbase::common::PageArena<> alloc_;
};

extern thread_local ZypInsertInfo* zyp_insert_info;
extern thread_local ZypRow** zyp_row_head;
extern thread_local ZypRow** zyp_row_tail;
extern thread_local ZypRow** zyp_current_row;
extern thread_local bool zyp_inited;
extern thread_local ZypAllocator* local_allocator;
extern oceanbase::LightyQueue local_allocator_gc_;
void zyp_real_sleep(int seconds);
void zyp_real_usleep(int useconds);

namespace oceanbase {
namespace rootserver {
class ObDDLService;
}
namespace share {
namespace schema {
class ObTableSchema;
}
}
}

void zyp_create_table_async(oceanbase::obrpc::ObSrvRpcProxy* rpc_proxy, oceanbase::obrpc::ObServerInfoList rs_list,
    oceanbase::rootserver::ObDDLService* ddl_service, const int64_t tenant_id);

typedef int (*schema_create_func)(oceanbase::share::schema::ObTableSchema &table_schema);

extern schema_create_func import_schemas [];
extern schema_create_func not_import_schemas [];
