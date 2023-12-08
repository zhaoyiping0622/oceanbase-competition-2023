#include "share/ob_zyp.h"
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/datum/ob_datum.h"
#include <mutex>
#include <thread>
#include <dlfcn.h>
#include "rootserver/ob_ddl_service.h"
#include "share/schema/ob_table_schema.h"

__thread bool zyp_come = false;
zyp_string zyp_extra_info;
thread_local ZypAllocator* local_allocator = nullptr;
oceanbase::LightyQueue local_allocator_gc_;

void zyp_enable() { zyp_come=true; zyp_inited=false; }
void zyp_disable() { zyp_come=false; zyp_inited=false; }
void zyp_set_extra(const zyp_string&s) {zyp_extra_info=s;}

int zyp_fd=-1;

std::mutex mutex;
void zyp_unlimit_log(const char* buf, size_t size) {
  if(zyp_fd==-1)return;
  std::unique_lock<std::mutex>unique_lock(mutex);
  write(zyp_fd, buf, size);
  write(zyp_fd, "\n", 1);
}

bool zyp_enabled(){return zyp_come;}

thread_local ZypInsertInfo* zyp_insert_info = nullptr;
thread_local ZypRow** zyp_current_row = nullptr;
thread_local ZypRow** zyp_row_head = nullptr;
thread_local ZypRow** zyp_row_tail = nullptr;
thread_local bool zyp_inited = false;

using namespace oceanbase::common;

void ZypRow::add_varchar(ObDatum* datum, const ObString&s) {
  if(s.ptr() == NULL) {
    // obj->set_varchar("");
    datum->set_null();
  } else {
    datum->set_string(s);
  }
}
void ZypRow::add_varbinary(ObDatum* datum, const ObString&s) {
  if(s.ptr() == NULL) {
    // obj->set_varbinary("");
    datum->set_null();
  } else {
    datum->set_string(s);
  }
}
void ZypRow::add_longtext(ObDatum* datum, const ObString&s) {
  if(s.ptr() == NULL) {
    // obj->set_string(oceanbase::ObLongTextType, "");
    datum->set_null();
  } else {
    datum->set_string(s);
  }
}
void ZypRow::add_bigint(ObDatum* datum, int64_t v) {
  datum->set_int(v);
}
void ZypRow::add_tinyint(ObDatum* datum, int8_t v) {
  datum->set_int(v);
}
void ZypRow::add_bigunsigned(ObDatum* datum, uint64_t v) {
  datum->set_uint(v);
}
void ZypRow::add_null(ObDatum* datum) {
  datum->set_null();
}
void ZypRow::add_timestamp(ObDatum* datum, int64_t timestamp) {
  datum->set_timestamp(timestamp);
}

ObArray<ZypRow*> ZypInsertInfo::get_row(int64_t count) {
  ObArray<ZypRow*> ret;
  ret.prepare_allocate(count);
  int64_t size;
  queue_.multi_pop((void**)ret.get_data(), count, size);
  while(count>size) count--, ret.pop_back();
  for(int i=0;i<ret.count();i++)ret[i]->init_datums();
  return ret;
}

void zyp_real_sleep(int seconds) {
  static void* libc_hdl = dlopen("libc.so.6", RTLD_LAZY | RTLD_NOLOAD);
  static unsigned int (*glibc_sleep)(unsigned int) = (decltype(glibc_sleep))dlsym(libc_hdl, "sleep");
  glibc_sleep(seconds);
}

void zyp_real_usleep(int useconds) {
  static void* libc_hdl = dlopen("libc.so.6", RTLD_LAZY | RTLD_NOLOAD);
  static unsigned int (*glibc_usleep)(unsigned int) = (decltype(glibc_usleep))dlsym(libc_hdl, "usleep");
  glibc_usleep(useconds);
}

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::share::schema;

schema_create_func not_import_schemas [] = {
#include "share/not-import-tables"
,NULL
};

schema_create_func import_schemas [] = {
#include "share/import-tables"
,NULL
};

void zyp_create_table_async(ObDDLService* ddl_service, const int64_t tenant_id, ObArray<ObTableSchema> tables) {
//   zyp_real_sleep(10);
//   int ret = OB_SUCCESS;
//   oceanbase::lib::set_thread_name("not_key_schema_thread");
//   ObDDLSQLTransaction* sql_client = OB_NEW(ObDDLSQLTransaction, "create_table", &ddl_service->get_schema_service(), true, true, false, false);
//   const int64_t refreshed_schema_version = 0;
//   if(OB_FAIL(sql_client->start(&ddl_service->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
//     LOG_INFO("failed to start sql_client", KR(ret));
//   }
//   ObDDLOperator ddl_operator(ddl_service->get_schema_service(),
//       ddl_service->get_sql_proxy());
//   // ddl_service->create_table_batch(ddl_operator, tables);
//   for(int i=0;i<tables.count();i++) {
//     ddl_operator.create_table(tables.at(i), *sql_client);
//   }
//   if(OB_FAIL(sql_client->end(true))){
//     LOG_WARN("sql_clients end failed");
//   }
//   OB_DELETE(ObDDLSQLTransaction, "create_table", sql_client);
}

std::set<void*>* import_schemas_set;
std::set<void*>* not_import_schemas_set;

__attribute__((constructor)) void init() {
  import_schemas_set = new std::set<void*>;
  not_import_schemas_set = new std::set<void*>;
  zyp_fd = open("/home/zhaoyiping/logs/zyp_log", O_CREAT|O_WRONLY|O_TRUNC, 0644);
  std::thread tmp_thread([](){
    for(schema_create_func* tmp=not_import_schemas;*tmp;tmp++) {
      not_import_schemas_set->insert((void*)*tmp);
    }
    for(schema_create_func* tmp=import_schemas;*tmp;tmp++) {
      import_schemas_set->insert((void*)*tmp);
    }
  });
  tmp_thread.detach();
}
__attribute__((destructor)) void fini() {
  if(zyp_fd!=-1) close(zyp_fd);
}
