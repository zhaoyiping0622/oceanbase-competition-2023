#include "share/ob_zyp.h"
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/datum/ob_datum.h"
#include <mutex>

__thread bool zyp_come = false;
zyp_string zyp_extra_info;

void zyp_enable() { zyp_come=true; zyp_inited=false; }
void zyp_disable() { zyp_come=false; zyp_inited=false; }
void zyp_set_extra(const zyp_string&s) {zyp_extra_info=s;}

int zyp_fd=-1;
__attribute__((constructor)) void init() {
  zyp_fd = open("/home/zhaoyiping/logs/zyp_log", O_CREAT|O_WRONLY|O_TRUNC, 0644);
}
__attribute__((destructor)) void fini() {
  if(zyp_fd!=-1) close(zyp_fd);
}

std::mutex mutex;
void zyp_unlimit_log(const char* buf, size_t size) {
  if(zyp_fd==-1)return;
  std::unique_lock<std::mutex>unique_lock(mutex);
  write(zyp_fd, buf, size);
}

bool zyp_enabled(){return zyp_come;}

thread_local ZypInsertInfo* zyp_insert_info = nullptr;
thread_local ZypRow** zyp_current_row = nullptr;
thread_local ZypRow** zyp_row_head = nullptr;
thread_local ZypRow** zyp_row_tail = nullptr;
thread_local bool zyp_inited = false;

using namespace oceanbase::common;

oceanbase::common::ObSafeArena ZypRow::allocator("zyp_row");

void ZypRow::add_varchar(ObObj* obj, ObDatum* datum, const ObString&s) {
  if(s.ptr() == NULL) {
    // obj->set_varchar("");
    datum->set_null();
  } else {
    obj->set_varchar(s);
    datum->set_string(s);
  }
  obj->set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
}
void ZypRow::add_varbinary(ObObj* obj, ObDatum* datum, const ObString&s) {
  if(s.ptr() == NULL) {
    // obj->set_varbinary("");
    datum->set_null();
  } else {
    obj->set_varbinary(s);
    datum->set_string(s);
  }
  obj->set_collation_type(CS_TYPE_BINARY);
}
void ZypRow::add_longtext(ObObj* obj, ObDatum* datum, const ObString&s) {
  if(s.ptr() == NULL) {
    // obj->set_string(oceanbase::ObLongTextType, "");
    datum->set_null();
  } else {
    obj->set_string(oceanbase::ObLongTextType, s);
    datum->set_string(s);
  }
  obj->set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
}
void ZypRow::add_bigint(ObObj* obj, ObDatum* datum, int64_t v) {
  obj->set_int(v);
  datum->set_int(v);
}
void ZypRow::add_tinyint(ObObj* obj, ObDatum* datum, int8_t v) {
  obj->set_tinyint(v);
  datum->set_int(v);
}
void ZypRow::add_bigunsigned(ObObj* obj, ObDatum* datum, uint64_t v) {
  obj->set_uint64(v);
  datum->set_uint(v);
}
void ZypRow::add_null(ObObj* obj, ObDatum* datum) {
  obj->set_null();
  datum->set_null();
}
void ZypRow::add_timestamp(ObObj* obj, ObDatum* datum, int64_t timestamp) {
  obj->set_timestamp(timestamp);
  datum->set_timestamp(timestamp);
}
ObNewRow ZypRow::new_row() { init_objs(); return ObNewRow(get_cells(), get_cells_cnt()); }

ObArray<ZypRow*> ZypInsertInfo::get_row(int64_t count) {
  ObArray<ZypRow*> ret;
  ret.prepare_allocate(count);
  int64_t size;
  queue_.multi_pop((void**)ret.get_data(), count, size);
  while(count>size) count--, ret.pop_back();
  for(int i=0;i<ret.count();i++)ret[i]->init_objs();
  return ret;
}
