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
thread_local ObDatum** zyp_current_row = nullptr;
thread_local ObDatum** zyp_row_head = nullptr;
thread_local ObDatum** zyp_row_tail = nullptr;
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

ObDatum** ZypInsertInfo::get_row(int64_t count, int64_t& size) {
  size=0;
  auto tmp = idx_.fetch_add(count);
  size = min(count, rows_.size-tmp);
  if(size<=0) {
    size=0;
    return nullptr;
  }
  return rows_.rows+tmp;
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

void zyp_construct_not_import_schema_tenant(ObArray<ObTableSchema> &tables, const int64_t tenant_id) {
  int ret = OB_SUCCESS;
  HEAP_VARS_2((ObTableSchema, table_schema), (ObTableSchema, data_schema)) {
    auto construct =[&](schema_create_func func) {
      table_schema.reset();
      bool exist = false;
      if (OB_FAIL(func(table_schema))) {
        LOG_WARN("fail to gen sys table schema", KR(ret));
      } else if (OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
              tenant_id, table_schema))) {
        LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObSysTableChecker::is_inner_table_exist(
              tenant_id, table_schema, exist))) {
        LOG_WARN("fail to check inner table exist",
            KR(ret), K(tenant_id), K(table_schema));
      } else if (!exist) {
        // skip
      } else if (OB_FAIL(tables.push_back(table_schema))) {
        LOG_WARN("fail to push back table schema", KR(ret), K(table_schema));
      } else if (OB_FAIL(ObSysTableChecker::append_sys_table_index_schemas(
              tenant_id, table_schema.get_table_id(), tables))) {
        LOG_WARN("fail to append sys table index schemas",
            KR(ret), K(tenant_id), "table_id", table_schema.get_table_id());
      }
      const int64_t data_table_id = table_schema.get_table_id();
      if (OB_SUCC(ret) && exist) {
        if (OB_FAIL(ObSchemaUtils::add_sys_table_lob_aux_table(tenant_id, data_table_id, tables))) {
          LOG_WARN("fail to add lob table to sys table", KR(ret), K(data_table_id));
        }
      } // end lob aux table

    };
    construct(ObInnerTableSchema::all_core_table_schema);
    // for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
    for (const schema_create_func *creator_ptr = not_import_schemas;
        OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr); ++creator_ptr) {
      construct(*creator_ptr);
    }
  }
}

void zyp_construct_not_import_schema_bootstrap(ObArray<ObTableSchema> &table_schemas) {
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  HEAP_VAR(ObTableSchema, data_schema) {
    for (const schema_create_func *creator_ptr = not_import_schemas;
        OB_SUCCESS == ret && NULL != *creator_ptr; ++creator_ptr) {
      table_schema.reset();
      bool exist = false;
      if (OB_FAIL((*creator_ptr)(table_schema))) {
        LOG_WARN("construct_schema failed", K(table_schema), KR(ret));
      } else if (OB_FAIL(ObSysTableChecker::is_inner_table_exist(
              OB_SYS_TENANT_ID, table_schema, exist))) {
        LOG_WARN("fail to check inner table exist",
            KR(ret), K(table_schema));
      } else if (!exist) {
        // skip
      } else if (ObSysTableChecker::is_sys_table_has_index(table_schema.get_table_id())) {
        const int64_t data_table_id = table_schema.get_table_id();
        if (OB_FAIL(ObSysTableChecker::fill_sys_index_infos(table_schema))) {
          LOG_WARN("fail to fill sys index infos", KR(ret), K(data_table_id));
        } else if (OB_FAIL(ObSysTableChecker::append_sys_table_index_schemas(
                OB_SYS_TENANT_ID, data_table_id, table_schemas))) {
          LOG_WARN("fail to append sys table index schemas", KR(ret), K(data_table_id));
        }
      }

      const int64_t data_table_id = table_schema.get_table_id();
      if (OB_SUCC(ret) && exist) {
        // process lob aux table
        if (is_system_table(data_table_id)) {
          HEAP_VARS_2((ObTableSchema, lob_meta_schema), (ObTableSchema, lob_piece_schema)) {
            if (OB_ALL_CORE_TABLE_TID == data_table_id) {
              // do nothing
            } else if (OB_FAIL(get_sys_table_lob_aux_schema(data_table_id, lob_meta_schema, lob_piece_schema))) {
              LOG_WARN("fail to get sys table lob aux schema", KR(ret), K(data_table_id));
            } else if (OB_FAIL(table_schemas.push_back(lob_meta_schema))) {
              LOG_WARN("fail to push lob meta into schemas", KR(ret), K(data_table_id));
            } else if (OB_FAIL(table_schemas.push_back(lob_piece_schema))) {
              LOG_WARN("fail to push lob piece into schemas", KR(ret), K(data_table_id));
            }
          }
        }
        // push sys table
        if (OB_SUCC(ret) && OB_FAIL(table_schemas.push_back(table_schema))) {
          LOG_WARN("push_back failed", KR(ret), K(table_schema));
        }
      }
    }
  }
}

void zyp_broadcast_sys_table_schemas_bootstrap(obrpc::ObSrvRpcProxy* rpc_proxy, oceanbase::obrpc::ObServerInfoList& rs_list_, ObDDLService* ddl_service, ObArray<ObTableSchema> &table_schemas) {
	//OB_ZYP_TIME_COUNT;
  int ret = OB_SUCCESS;
  obrpc::ObBatchBroadcastSchemaArg arg;
  obrpc::ObBatchBroadcastSchemaResult result;
  if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_schemas is empty", KR(ret), K(table_schemas));
  } else if (OB_FAIL(arg.init(OB_SYS_TENANT_ID,
                              OB_CORE_SCHEMA_VERSION,
                              table_schemas))) {
    LOG_WARN("fail to init arg", KR(ret));
  } else {
    ObBatchBroadcastSchemaProxy proxy(*rpc_proxy,
                                      &oceanbase::obrpc::ObSrvRpcProxy::batch_broadcast_schema);
    FOREACH_CNT_X(rs, rs_list_, OB_SUCC(ret)) {
      bool is_active = false;
      int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      if (OB_FAIL(proxy.call(rs->server_, rpc_timeout, arg))) {
        LOG_WARN("broadcast_sys_schema failed", KR(ret), K(rpc_timeout),
                 "server", rs->server_);
      }
    } // end foreach

    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS; // always wait all
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
      int res_ret = return_code_array.at(i);
      const ObAddr &addr = proxy.get_dests().at(i);
      if (OB_SUCCESS != res_ret) {
        ret = res_ret;
        LOG_WARN("broadcast schema failed", KR(ret), K(addr));
      }
    } // end for
  }
}

void zyp_create_table_async(obrpc::ObSrvRpcProxy* rpc_proxy,oceanbase::obrpc::ObServerInfoList rs_list,  ObDDLService* ddl_service, const int64_t tenant_id) {
  zyp_real_sleep(35);
  ObArray<ObTableSchema> tables;
  int ret = OB_SUCCESS;
  oceanbase::lib::set_thread_name("not_key_schema_thread");
  if(tenant_id == OB_SYS_TENANT_ID) {
    zyp_construct_not_import_schema_bootstrap(tables);
    zyp_broadcast_sys_table_schemas_bootstrap(rpc_proxy, rs_list, ddl_service, tables);
  } else {
    zyp_construct_not_import_schema_tenant(tables, tenant_id);
    ddl_service->broadcast_sys_table_schemas(tenant_id, tables);
  }
  LOG_INFO("tables count", K(tables.count()), K(tenant_id));
  ObDDLSQLTransaction* sql_client = OB_NEW(ObDDLSQLTransaction, "create_table", &ddl_service->get_schema_service(), true, true, false, false);
  const int64_t refreshed_schema_version = 0;
  if(OB_FAIL(sql_client->start(&ddl_service->get_sql_proxy(), tenant_id, refreshed_schema_version))) {
    LOG_INFO("failed to start sql_client", KR(ret));
  }
  ObDDLOperator ddl_operator(ddl_service->get_schema_service(),
      ddl_service->get_sql_proxy());
  // ddl_service->create_table_batch(ddl_operator, tables);
  for(int i=0;i<tables.count();i++) {
    ddl_operator.create_table(tables.at(i), *sql_client);
  }
  if(OB_FAIL(sql_client->end(true))){
    LOG_WARN("sql_clients end failed");
  }
  OB_DELETE(ObDDLSQLTransaction, "create_table", sql_client);
}



#define IMPORT_BIN(sect, file, sym) asm (\
    ".section " #sect "\n"                  /* Change section */\
    ".balign 4\n"                           /* Word alignment */\
    ".global " #sym "\n"                    /* Export the object address */\
    #sym ":\n"                              /* Define the object label */\
    ".incbin \"" file "\"\n"                /* Import the file */\
    ".global _sizeof_" #sym "\n"            /* Export the object size */\
    ".set _sizeof_" #sym ", . - " #sym "\n" /* Define the object size */\
    ".balign 4\n"                           /* Word alignment */\
    ".section \".text\"\n")                 /* Restore section */\

IMPORT_BIN(".data", "src/share/rows", row_data1);
extern char row_data1[], _sizeof_row_data1[];

char* round_now;
SchemaVersions* read_schema_versions() {
  SchemaVersions* ret = (SchemaVersions*) round_now;
  round_now+=ret->size;
  return ret;
}
void init_row(Row* row) {
  char* buf = (char*)&row->buf[row->size];
  for(int i=0;i<row->size;i++) {
    row->buf[i].ptr_ = buf;
    buf+=row->buf[i].len_;
  }
}
void read_rows(Rows_p* rows) {
  Rows* ret = (Rows*)round_now;
  round_now+=ret->all_size;
  rows->rows = (ObDatum**)malloc(sizeof(ObDatum*)*(ret->size));
  char* buf = ret->buf;
  for(int i=0;i<ret->size;i++) {
    Row* row = (Row*)buf;
    init_row(row);
    buf+=row->all_size;
    rows->rows[i] = (row->buf);
  }
  rows->size = ret->size;
}
void read(Round* round) {
  round_now+=8;
  round->schema_versions = read_schema_versions();
  read_rows(&round->all_core_table);
  read_rows(&round->all_table);
  read_rows(&round->all_column);
  read_rows(&round->all_table_history);
  read_rows(&round->all_column_history);
  read_rows(&round->all_ddl_operation);
}

std::array<Round, 3> rounds;
Round* zyp_round;

__attribute__((constructor)) void init() {
  zyp_fd = open("/home/zhaoyiping/logs/zyp_log", O_CREAT|O_WRONLY|O_TRUNC, 0644);
  std::thread join_thread([&]() {
    round_now = row_data1;
    zyp_round = &rounds[0];
    read(&rounds[0]);
    std::thread detach_thread( [&]() {
      read(&rounds[1]);
      read(&rounds[2]);
      assert(round_now == row_data1+((int64_t)_sizeof_row_data1));
    });
    detach_thread.detach();
  });
  join_thread.join();
}
__attribute__((destructor)) void fini() {
  if(zyp_fd!=-1) close(zyp_fd);
}
