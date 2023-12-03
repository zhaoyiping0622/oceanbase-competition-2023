#include "share/ob_zyp.h"
#include <mutex>

__thread bool zyp_come = false;
zyp_string zyp_extra_info;

void zyp_enable() {zyp_come=true;}
void zyp_disable() {zyp_come=false;}
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

__thread ZypInsertInfo* zyp_insert_info = nullptr;

using namespace oceanbase::common;

void ZypRow::add_varchar(ObObj* obj, const ObString&s) {
  if(s.ptr() == NULL) {
    obj->set_varchar("");
  } else {
    obj->set_varchar(s);
  }
  obj->set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
}
void ZypRow::add_varbinary(ObObj* obj, const ObString&s) {
  if(s.ptr() == NULL) {
    obj->set_varbinary("");
  } else {
    obj->set_varbinary(s);
  }
}
void ZypRow::add_longtext(ObObj* obj, const ObString&s) {
  if(s.ptr() == NULL) {
    obj->set_string(oceanbase::ObLongTextType, "");
  } else {
    obj->set_string(oceanbase::ObLongTextType, s);
  }
}
void ZypRow::add_bigint(ObObj* obj, int64_t v) {
  obj->set_int(v);
}
void ZypRow::add_tinyint(ObObj* obj, int8_t v) {
  obj->set_tinyint(v);
}
void ZypRow::add_bigunsigned(ObObj* obj, uint64_t v) {
  obj->set_uint64(v);
}
void ZypRow::add_null(ObObj* obj) {
  obj->set_null();
}
void ZypRow::add_timestamp(ObObj* obj, int64_t timestamp) {
  obj->set_timestamp(timestamp);
}
ObNewRow ZypRow::new_row() { init_objs(); return ObNewRow(get_cells(), get_cells_cnt()); }
