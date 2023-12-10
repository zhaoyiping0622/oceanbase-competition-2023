#include "lib/utility/ob_macro_utils.h"
#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_table_batch_create.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_store_format.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_partition_sql_helper.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_time_zone_info_manager.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_trigger_sql_service.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_create_schema_parallel.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "sql/ob_sql_utils.h"
#include "lib/time/ob_time_count.h"
#include "rootserver/ob_ddl_service.h"
#include "share/ob_zyp.h"
#include "share/schema/ob_table_sql_service.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {

ObCoreTableProxyBatch::ObCoreTableProxyBatch(const char* table, ObISQLClient& sql_client, uint64_t tenant_id, std::atomic_long& row_id): table_name_(table), kv_(table, sql_client, tenant_id), row_id_(row_id), tenant_id_(tenant_id) {}

int ObCoreTableProxyBatch::AddDMLSqlSplicer(ObDMLSqlSplicer& raw_dml, int64_t limit) {
  int ret = OB_SUCCESS;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  if(OB_FAIL(raw_dml.splice_core_cells(kv_, cells))) {
    LOG_WARN("failed to splice_core_cells", KR(ret));
  } else {
    long now = row_id_++;
    FOREACH_X(uc, cells, ret==OB_SUCCESS && limit--) {
      if(OB_FAIL(dml_.add_pk_column("table_name", table_name_))) {
        LOG_WARN("failed to add table_name", KR(ret));
      } else if(OB_FAIL(dml_.add_pk_column("row_id", now))) {
        LOG_WARN("failed to add row_id", KR(ret));
      } else if(OB_FAIL(dml_.add_pk_column("column_name", uc->cell_.name_))) {
        LOG_WARN("failed to add column_name", KR(ret));
      } else if(OB_FAIL(dml_.add_gmt_create())) {
        LOG_WARN("failed to add gmt_create", KR(ret));
      } else if(dml_.add_gmt_modified()) {
        LOG_WARN("failed to add gmt_modified", KR(ret));
      } else if(uc->cell_.value_.ptr() == NULL) {
        if(OB_FAIL(dml_.add_column(true, "column_value"))) {
          LOG_WARN("failed to add column_value", KR(ret));
        }
      } else if(uc->cell_.is_hex_value_) {
        if(FALSE_IT(dml_.change_mode(ObDMLSqlSplicer::NAKED_VALUE_MODE))){
        } if(OB_FAIL(dml_.add_column("column_value", uc->cell_.value_))) {
          LOG_WARN("failed to add column_value", KR(ret));
        } else if(FALSE_IT(dml_.change_mode(ObDMLSqlSplicer::QUOTE_STRING_MODE))){
        }
      } else {
        if(OB_FAIL(dml_.add_column("column_value", uc->cell_.value_.ptr()))) {
          LOG_WARN("failed to add column_value", KR(ret));
        }
      }
      if(OB_SUCC(ret)) {
        if(OB_FAIL(dml_.finish_row())) {
          LOG_WARN("failed to finish_row", KR(ret));
        }
      }
    }
  }
  return ret;
}
ObDMLSqlSplicer& ObCoreTableProxyBatch::getDML() {
  return dml_;
}

namespace schema {

TableBatchCreateByPass::TableBatchCreateByPass(common::ObIArray<ObTableSchema>& tables, StartFunc start, EndFunc end): client_start_(start), client_end_(end), tables_(tables) {
  if(tables_.count() == 0) {
    LOG_INFO("tables is empty");
    return;
  }
  tenant_id_ = tables_.at(0).get_tenant_id();
  exec_tenant_id_ = ObSchemaUtils::get_exec_tenant_id(tenant_id_);
  global_client_ = client_start_();
  now_ = ObTimeUtility::current_time();
  LOG_INFO("zyp_round", K(zyp_round));
  all_core_table_rows_ = &zyp_round->all_core_table;
  all_table_rows_ = &zyp_round->all_table;
  all_column_rows_ = &zyp_round->all_column;
  all_table_history_rows_ = &zyp_round->all_table_history;
  all_column_history_rows_ = &zyp_round->all_column_history;
  all_ddl_operation_rows_ = &zyp_round->all_ddl_operation;
  LOG_INFO("row size",
      K(all_core_table_rows_->size),
      K(all_table_rows_->size),
      K(all_column_rows_->size),
      K(all_table_history_rows_->size),
      K(all_column_history_rows_->size),
      K(all_ddl_operation_rows_->size)
  );
}

TableBatchCreateByPass::~TableBatchCreateByPass() {
  if(tables_.count() == 0) {
    LOG_INFO("tables is empty");
    return;
  }
  client_end_(global_client_);
}
int TableBatchCreateByPass::run() {
  int ret = OB_SUCCESS;
  LOG_INFO("TableBatchCreateByPass run begin");
  DEFER({LOG_INFO("TableBatchCreateByPass run end");});

  if(tables_.count() == 0) {
    LOG_INFO("tables is empty");
    return OB_SUCCESS;
  }

  std::vector<ZypInsertInfo*> insert_info={
    OB_NEW(ZypInsertInfo, "insert_info", *all_core_table_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", *all_table_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", *all_column_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", *all_ddl_operation_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", *all_table_history_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", *all_column_history_rows_),
  };

  std::vector<std::function<int()>> run_insert = {
    [&](){ return run_insert_all_core_table(); },
    [&](){ return run_insert_all_table(); },
    [&](){ return run_insert_all_column(); },
    [&](){ return run_insert_all_ddl_operation(); },
    [&](){ return run_insert_all_table_history(); },
    [&](){ return run_insert_all_column_history(); },
  };

  std::vector<std::atomic<int64_t>> size(run_insert.size());
  size[0] = all_core_table_rows_->size;
  size[1] = all_table_rows_->size;
  size[2] = all_column_rows_->size;
  size[3] = all_ddl_operation_rows_->size;
  size[4] = all_table_history_rows_->size;
  size[5] = all_column_history_rows_->size;

  DEFER({for(auto x:insert_info)OB_DELETE(ZypInsertInfo, "insert_info", x);});

  auto find_max_idx=[&]() {
    int ret = -1;
    int64_t max_count=-1;
    for(int i=0;i<insert_info.size();i++) {
      int64_t t = size[i];
      if(t>0&&t>max_count)max_count=t, ret=i;
    }
    LOG_INFO("find_max_idx", K(ret), K(max_count));
    return ret;
  };

  auto run=[&](int idx) {
    int ret = OB_SUCCESS;
    zyp_enable();
    DEFER({zyp_disable();});
    zyp_insert_info = insert_info[idx];
    const int batch_size = 512;
    auto now = size[idx].fetch_sub(batch_size);
    if(now<0) return;
    int64_t size;
    ObDatum** rows = zyp_insert_info->get_row(batch_size, size);
    if(size == 0) return;
    LOG_INFO("::zyp_insert_info", K(::zyp_insert_info));
    for(int i=0;i<3;i++) {
      zyp_row_head = rows;
      zyp_current_row = zyp_row_head;
      zyp_row_tail = zyp_row_head + size;
      if(OB_FAIL(run_insert[idx]())) {
        LOG_INFO("insert failed sleep", K(ret), K(idx), K(i));
        usleep(100);
      } else {
        LOG_INFO("insert done", K(idx));
        return;
      }
    }
    LOG_INFO("insert failed", K(ret), K(idx));
  };
    
  std::atomic_int idx{0};

  ParallelRunner runner;
  runner.run_parallel([&]() {
    for(int i=0;i<run_insert.size();i++) {
      while(size[i]>0) {
        run(i);
      }
    }
  }, [&]() { return find_max_idx() != -1; });

  return ret;
}
int TableBatchCreateByPass::run_insert_all_core_table() { 
  auto* sql_client = client_start_();
  DEFER({client_end_(sql_client);});
  int ret = OB_SUCCESS;
  int64_t affected_rows;
  const char* sql = "INSERT INTO __all_core_table (table_name, row_id, column_name, column_value) VALUES  ('__all_zyp_info', 1, 'tenant_id', '1')";
  if(OB_FAIL(sql_client->write(exec_tenant_id_, sql, affected_rows))) {
      LOG_INFO("run_insert_all_core_table failed", KR(ret), K(affected_rows));
  } else {
      LOG_INFO("run_insert_all_core_table succeeded", K(affected_rows));
  }
  return ret;
}

int TableBatchCreateByPass::run_insert_all_table_history() { 
  auto* sql_client = client_start_();
  DEFER({client_end_(sql_client);});
  int ret = OB_SUCCESS;
  int64_t affected_rows;
  const char* sql = "INSERT INTO __all_table_history (tenant_id, table_id, table_name, database_id, table_type, load_type, def_type, rowkey_column_num, index_column_num, max_used_column_id, session_id, sess_active_time, tablet_size, pctfree, autoinc_column_id, auto_increment, read_only, rowkey_split_pos, compress_func_name, expire_condition, is_use_bloomfilter, index_attributes_set, comment, block_size, collation_type, data_table_id, index_status, tablegroup_id, progressive_merge_num, index_type, index_using_type, part_level, part_func_type, part_func_expr, part_num, sub_part_func_type, sub_part_func_expr, sub_part_num, schema_version, view_definition, view_check_option, view_is_updatable, parser_name, gmt_create, gmt_modified, partition_status, partition_schema_version, pk_comment, row_store_type, store_format, duplicate_scope, progressive_merge_round, storage_format_version, table_mode, encryption, tablespace_id, sub_part_template_flags, dop, character_set_client, collation_connection, auto_part, auto_part_size, association_table_id, define_user_id, max_dependency_version, tablet_id, object_status, table_flags, truncate_version, external_file_location, external_file_location_access_info, external_file_format, external_file_pattern, ttl_definition, kv_attributes, name_generated_type, is_deleted) VALUES (0, 114514, X'5F5F6964785F3130325F6964785F75725F6E616D65', 201001, 5, 0, 0, 3, 1, 20, 0, 0, 134217728, 10, 0, 1, 0, 0, X'6E6F6E65', X'', 0, 0, X'', 16384, 45, 102, 2, 202001, 0, 1, 0, 0, 0, X'', 1, 0, X'', 0, 1701765983004840, X'', 0, 0, NULL, now(6), now(6), 0, 0, X'', X'656E636F64696E675F726F775F73746F7265', X'44594E414D4943', 0, 1, 3, 0, X'', -1, 0, 1, 0, 0, 0, 0, -1, -1, -1, 101005, 1, 0, -1, NULL, NULL, NULL, NULL, X'', X'', 0, 0)";
  if(OB_FAIL(sql_client->write(exec_tenant_id_, sql, affected_rows))) {
      LOG_INFO("run_insert_all_table_history failed", KR(ret), K(affected_rows));
  } else {
      LOG_INFO("run_insert_all_table_history succeeded", K(affected_rows));
  }
  return ret;
}

int TableBatchCreateByPass::run_insert_all_table() { 
  auto* sql_client = client_start_();
  DEFER({client_end_(sql_client);});
  int ret = OB_SUCCESS;
  int64_t affected_rows;
  const char* sql = "INSERT INTO __all_table (tenant_id, table_id, table_name, database_id, table_type, load_type, def_type, rowkey_column_num, index_column_num, max_used_column_id, session_id, sess_active_time, tablet_size, pctfree, autoinc_column_id, auto_increment, read_only, rowkey_split_pos, compress_func_name, expire_condition, is_use_bloomfilter, index_attributes_set, comment, block_size, collation_type, data_table_id, index_status, tablegroup_id, progressive_merge_num, index_type, index_using_type, part_level, part_func_type, part_func_expr, part_num, sub_part_func_type, sub_part_func_expr, sub_part_num, schema_version, view_definition, view_check_option, view_is_updatable, parser_name, gmt_create, gmt_modified, partition_status, partition_schema_version, pk_comment, row_store_type, store_format, duplicate_scope, progressive_merge_round, storage_format_version, table_mode, encryption, tablespace_id, sub_part_template_flags, dop, character_set_client, collation_connection, auto_part, auto_part_size, association_table_id, define_user_id, max_dependency_version, tablet_id, object_status, table_flags, truncate_version, external_file_location, external_file_location_access_info, external_file_format, external_file_pattern, ttl_definition, kv_attributes, name_generated_type) VALUES (0, 114514, X'5F5F6964785F3130325F6964785F75725F6E616D65', 201001, 5, 0, 0, 3, 1, 20, 0, 0, 134217728, 10, 0, 1, 0, 0, X'6E6F6E65', X'', 0, 0, X'', 16384, 45, 102, 2, 202001, 0, 1, 0, 0, 0, X'', 1, 0, X'', 0, 1701765983004840, X'', 0, 0, NULL, now(6), now(6), 0, 0, X'', X'656E636F64696E675F726F775F73746F7265', X'44594E414D4943', 0, 1, 3, 0, X'', -1, 0, 1, 0, 0, 0, 0, -1, -1, -1, 101005, 1, 0, -1, NULL, NULL, NULL, NULL, X'', X'', 0)";
  if(OB_FAIL(sql_client->write(exec_tenant_id_, sql, affected_rows))) {
      LOG_INFO("run_insert_all_table failed", KR(ret), K(affected_rows));
  } else {
      LOG_INFO("run_insert_all_table succeeded", K(affected_rows));
  }
  return ret;
}

int TableBatchCreateByPass::run_insert_all_column() { 
  auto* sql_client = client_start_();
  DEFER({client_end_(sql_client);});
  int ret = OB_SUCCESS;
  int64_t affected_rows;
  const char* sql = "INSERT INTO __all_column (tenant_id, table_id, column_id, column_name, rowkey_position, index_position, partition_key_position, data_type, data_length, data_precision, data_scale, zero_fill, nullable, autoincrement, is_hidden, on_update_current_timestamp, orig_default_value_v2, cur_default_value_v2, cur_default_value, order_in_rowkey, collation_type, comment, schema_version, column_flags, extended_type_info, prev_column_id, srs_id, udt_set_id, sub_data_type, gmt_create, gmt_modified) VALUES (0,114514,16,X'676D745F637265617465',0,0,0,18,0,25,6,0,1,0,0,0,NULL,X'43555252454E545F54494D455354414D50',NULL,0,63,X'',0,0,NULL,0,-32,0,0,now(6),now(6))";
  if(OB_FAIL(sql_client->write(exec_tenant_id_, sql, affected_rows))) {
      LOG_INFO("run_insert_all_column failed", KR(ret), K(affected_rows));
  } else {
      LOG_INFO("run_insert_all_column succeeded", K(affected_rows));
  }
  return ret;
}

int TableBatchCreateByPass::run_insert_all_column_history() { 
  auto* sql_client = client_start_();
  DEFER({client_end_(sql_client);});
  int ret = OB_SUCCESS;
  int64_t affected_rows;
  const char* sql = "INSERT INTO __all_column_history (tenant_id, table_id, column_id, column_name, rowkey_position, index_position, partition_key_position, data_type, data_length, data_precision, data_scale, zero_fill, nullable, autoincrement, is_hidden, on_update_current_timestamp, orig_default_value_v2, cur_default_value_v2, cur_default_value, order_in_rowkey, collation_type, comment, schema_version, column_flags, extended_type_info, prev_column_id, srs_id, udt_set_id, sub_data_type, gmt_create, gmt_modified, is_deleted) VALUES (0,114514,16,X'676D745F637265617465',0,0,0,18,0,25,6,0,1,0,0,0,NULL,X'43555252454E545F54494D455354414D50',NULL,0,63,X'',0,0,NULL,0,-32,0,0,now(6),now(6), 0)";
  if(OB_FAIL(sql_client->write(exec_tenant_id_, sql, affected_rows))) {
      LOG_INFO("run_insert_all_column_history failed", KR(ret), K(affected_rows));
  } else {
      LOG_INFO("run_insert_all_column_history succeeded", K(affected_rows));
  }
  return ret;
}
int TableBatchCreateByPass::run_insert_all_ddl_operation() { 
  auto* sql_client = client_start_();
  DEFER({client_end_(sql_client);});
  int ret = OB_SUCCESS;
  int64_t affected_rows;
  const char* sql = "INSERT INTO __all_ddl_operation (SCHEMA_VERSION, TENANT_ID, EXEC_TENANT_ID, USER_ID, DATABASE_ID, DATABASE_NAME, TABLEGROUP_ID, TABLE_ID, TABLE_NAME, OPERATION_TYPE, DDL_STMT_STR, gmt_modified) values (1701765982879712, 0, 1, 0, 201001, '', 202001, 1, '', 4, X'', now(6))";
  if(OB_FAIL(sql_client->write(exec_tenant_id_, sql, affected_rows))) {
      LOG_INFO("run_insert_all_ddl_operation failed", KR(ret), K(affected_rows));
  } else {
      LOG_INFO("run_insert_all_ddl_operation succeeded", K(affected_rows));
  }
  return ret;
};

void TableBatchCreateNormal::prepare() {
  LOG_INFO("TableBatchCreateNormal prepare begin");
  DEFER({LOG_INFO("TableBatchCreateNormal prepare end");});
  if(tables_.count() == 0) return;
  ParallelRunner runner;
  runner.run_parallel_range(0, (int)tables_.count(), [&](int i) {
    auto &table = tables_.at(i);
    ObDMLSqlSplicer dml;
    sql_service_->gen_table_dml(exec_tenant_id_, table, false, dml);
    ObSqlString sql;
    dml.splice_insert_sql(OB_ALL_TABLE_TNAME, sql);
    char* buf = new char[sql.length()+1];
    memcpy(buf, sql.ptr(), sql.length());
    buf[sql.length()] = 0;
    queue_.push(buf);
    dml.add_column("is_deleted", 0);
    sql.reuse();
    dml.splice_insert_sql(OB_ALL_TABLE_HISTORY_TNAME, sql);
    buf = new char[sql.length()+1];
    memcpy(buf, sql.ptr(), sql.length());
    buf[sql.length()] = 0;
    queue_.push(buf);
  });
}
void TableBatchCreateNormal::run() {
  LOG_INFO("TableBatchCreateNormal run begin");
  DEFER({LOG_INFO("TableBatchCreateNormal run end");});
  if(tables_.count() == 0) {
    LOG_INFO("tables is empty");
    return;
  }
  ParallelRunner runner;
  runner.run_parallel([&](){
    void* sql;
    if(OB_SUCCESS == queue_.pop(sql)) {
      auto* client = client_start_();
      DEFER({client_end_(client);});
      int64_t affected_rows;
      client->write(tenant_id_, (char*)sql, affected_rows);
    }
  }, [&](){return queue_.size()!=0;});
}

TableBatchCreateNormal::TableBatchCreateNormal(ObIArray<oceanbase::share::schema::ObTableSchema>& tables, StartFunc start, EndFunc end, ObTableSqlService* sql_service):
  client_start_(start), client_end_(end), tables_(tables), sql_service_(sql_service) {
  if(tables_.count() == 0) {
    LOG_INFO("tables is empty");
    return;
  }
  tenant_id_ = tables.at(0).get_tenant_id();
  exec_tenant_id_ = ObSchemaUtils::get_exec_tenant_id(tenant_id_);
  queue_.init(tables.count()*2);
}

}

}
