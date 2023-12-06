#pragma once

#include "lib/mysqlclient/ob_isql_client.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/datum/ob_datum.h"
#include "lib/allocator/ob_safe_arena.h"
#include "share/ob_zyp.h"
#include "lib/queue/ob_lighty_queue.h"
#include "share/schema/ob_table_schema.h"
#include <mutex>

namespace oceanbase {

class ObCoreTableProxyBatch {
  public:
    ObCoreTableProxyBatch(const char* table, ObISQLClient& sql_client, uint64_t tenant_id, std::atomic_long& row_id);
    share::ObDMLSqlSplicer& getDML();
    int AddDMLSqlSplicer(share::ObDMLSqlSplicer& raw_dml, int64_t limit = INT64_MAX);
private:
    const char* table_name_;
    share::ObCoreTableProxy kv_;
    share::ObDMLSqlSplicer dml_;
    std::atomic_long& row_id_;
    uint64_t tenant_id_;
};

#define ZYP_ALL_CORE_TABLE_OP(ZYP_OP,...)\
  ZYP_OP(table_name,varchar,0,__VA_ARGS__)\
  ZYP_OP(row_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(column_name,varchar,0,__VA_ARGS__)\
  ZYP_OP(gmt_create,timestamp,1,__VA_ARGS__)\
  ZYP_OP(gmt_modified,timestamp,1,__VA_ARGS__)\
  ZYP_OP(column_value,varchar,1,__VA_ARGS__)\

#define ZYP_ALL_TABLE_OP(ZYP_OP,...)\
  ZYP_OP(tenant_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(gmt_create,timestamp,1,__VA_ARGS__)\
  ZYP_OP(gmt_modified,timestamp,1,__VA_ARGS__)\
  ZYP_OP(table_name,varchar,0,__VA_ARGS__)\
  ZYP_OP(database_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(load_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(def_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(rowkey_column_num,bigint,0,__VA_ARGS__)\
  ZYP_OP(index_column_num,bigint,0,__VA_ARGS__)\
  ZYP_OP(max_used_column_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(autoinc_column_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(auto_increment,bigunsigned,1,__VA_ARGS__)\
  ZYP_OP(read_only,bigint,0,__VA_ARGS__)\
  ZYP_OP(rowkey_split_pos,bigint,0,__VA_ARGS__)\
  ZYP_OP(compress_func_name,varchar,0,__VA_ARGS__)\
  ZYP_OP(expire_condition,varchar,0,__VA_ARGS__)\
  ZYP_OP(is_use_bloomfilter,bigint,0,__VA_ARGS__)\
  ZYP_OP(comment,varchar,0,__VA_ARGS__)\
  ZYP_OP(block_size,bigint,0,__VA_ARGS__)\
  ZYP_OP(collation_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(data_table_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(index_status,bigint,0,__VA_ARGS__)\
  ZYP_OP(tablegroup_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(progressive_merge_num,bigint,0,__VA_ARGS__)\
  ZYP_OP(index_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(part_level,bigint,0,__VA_ARGS__)\
  ZYP_OP(part_func_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(part_func_expr,varchar,0,__VA_ARGS__)\
  ZYP_OP(part_num,bigint,0,__VA_ARGS__)\
  ZYP_OP(sub_part_func_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(sub_part_func_expr,varchar,0,__VA_ARGS__)\
  ZYP_OP(sub_part_num,bigint,0,__VA_ARGS__)\
  ZYP_OP(schema_version,bigint,0,__VA_ARGS__)\
  ZYP_OP(view_definition,longtext,0,__VA_ARGS__)\
  ZYP_OP(view_check_option,bigint,0,__VA_ARGS__)\
  ZYP_OP(view_is_updatable,bigint,0,__VA_ARGS__)\
  ZYP_OP(index_using_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(parser_name,varchar,1,__VA_ARGS__)\
  ZYP_OP(index_attributes_set,bigint,1,__VA_ARGS__)\
  ZYP_OP(tablet_size,bigint,0,__VA_ARGS__)\
  ZYP_OP(pctfree,bigint,0,__VA_ARGS__)\
  ZYP_OP(partition_status,bigint,1,__VA_ARGS__)\
  ZYP_OP(partition_schema_version,bigint,1,__VA_ARGS__)\
  ZYP_OP(session_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(pk_comment,varchar,0,__VA_ARGS__)\
  ZYP_OP(sess_active_time,bigint,1,__VA_ARGS__)\
  ZYP_OP(row_store_type,varchar,1,__VA_ARGS__)\
  ZYP_OP(store_format,varchar,1,__VA_ARGS__)\
  ZYP_OP(duplicate_scope,bigint,1,__VA_ARGS__)\
  ZYP_OP(progressive_merge_round,bigint,1,__VA_ARGS__)\
  ZYP_OP(storage_format_version,bigint,1,__VA_ARGS__)\
  ZYP_OP(table_mode,bigint,0,__VA_ARGS__)\
  ZYP_OP(encryption,varchar,1,__VA_ARGS__)\
  ZYP_OP(tablespace_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(sub_part_template_flags,bigint,0,__VA_ARGS__)\
  ZYP_OP(dop,bigint,0,__VA_ARGS__)\
  ZYP_OP(character_set_client,bigint,0,__VA_ARGS__)\
  ZYP_OP(collation_connection,bigint,0,__VA_ARGS__)\
  ZYP_OP(auto_part_size,bigint,0,__VA_ARGS__)\
  ZYP_OP(auto_part,tinyint,0,__VA_ARGS__)\
  ZYP_OP(association_table_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(tablet_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(max_dependency_version,bigint,0,__VA_ARGS__)\
  ZYP_OP(define_user_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(transition_point,varchar,1,__VA_ARGS__)\
  ZYP_OP(b_transition_point,varchar,1,__VA_ARGS__)\
  ZYP_OP(interval_range,varchar,1,__VA_ARGS__)\
  ZYP_OP(b_interval_range,varchar,1,__VA_ARGS__)\
  ZYP_OP(object_status,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_flags,bigint,0,__VA_ARGS__)\
  ZYP_OP(truncate_version,bigint,0,__VA_ARGS__)\
  ZYP_OP(external_file_location,varbinary,1,__VA_ARGS__)\
  ZYP_OP(external_file_location_access_info,varbinary,1,__VA_ARGS__)\
  ZYP_OP(external_file_format,varbinary,1,__VA_ARGS__)\
  ZYP_OP(external_file_pattern,varbinary,1,__VA_ARGS__)\
  ZYP_OP(ttl_definition,varchar,0,__VA_ARGS__)\
  ZYP_OP(kv_attributes,varchar,0,__VA_ARGS__)\
  ZYP_OP(name_generated_type,bigint,0,__VA_ARGS__)\

#define ZYP_ALL_COLUMN_OP(ZYP_OP,...)\
  ZYP_OP(tenant_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(column_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(gmt_create,timestamp,1,__VA_ARGS__)\
  ZYP_OP(gmt_modified,timestamp,1,__VA_ARGS__)\
  ZYP_OP(column_name,varchar,0,__VA_ARGS__)\
  ZYP_OP(rowkey_position,bigint,0,__VA_ARGS__)\
  ZYP_OP(index_position,bigint,0,__VA_ARGS__)\
  ZYP_OP(order_in_rowkey,bigint,0,__VA_ARGS__)\
  ZYP_OP(partition_key_position,bigint,0,__VA_ARGS__)\
  ZYP_OP(data_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(data_length,bigint,0,__VA_ARGS__)\
  ZYP_OP(data_precision,bigint,1,__VA_ARGS__)\
  ZYP_OP(data_scale,bigint,1,__VA_ARGS__)\
  ZYP_OP(zero_fill,bigint,0,__VA_ARGS__)\
  ZYP_OP(nullable,bigint,0,__VA_ARGS__)\
  ZYP_OP(on_update_current_timestamp,bigint,0,__VA_ARGS__)\
  ZYP_OP(autoincrement,bigint,0,__VA_ARGS__)\
  ZYP_OP(is_hidden,bigint,0,__VA_ARGS__)\
  ZYP_OP(collation_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(orig_default_value,varchar,1,__VA_ARGS__)\
  ZYP_OP(cur_default_value,varchar,1,__VA_ARGS__)\
  ZYP_OP(comment,longtext,1,__VA_ARGS__)\
  ZYP_OP(schema_version,bigint,0,__VA_ARGS__)\
  ZYP_OP(column_flags,bigint,0,__VA_ARGS__)\
  ZYP_OP(prev_column_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(extended_type_info,varbinary,1,__VA_ARGS__)\
  ZYP_OP(orig_default_value_v2,varbinary,1,__VA_ARGS__)\
  ZYP_OP(cur_default_value_v2,varbinary,1,__VA_ARGS__)\
  ZYP_OP(srs_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(udt_set_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(sub_data_type,bigint,0,__VA_ARGS__)\

#define ZYP_ALL_TABLE_HISTORY_OP(ZYP_OP,...)\
  ZYP_OP(tenant_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(schema_version,bigint,0,__VA_ARGS__)\
  ZYP_OP(gmt_create,timestamp,1,__VA_ARGS__)\
  ZYP_OP(gmt_modified,timestamp,1,__VA_ARGS__)\
  ZYP_OP(is_deleted,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_name,varchar,1,__VA_ARGS__)\
  ZYP_OP(database_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(table_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(load_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(def_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(rowkey_column_num,bigint,1,__VA_ARGS__)\
  ZYP_OP(index_column_num,bigint,1,__VA_ARGS__)\
  ZYP_OP(max_used_column_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(autoinc_column_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(auto_increment,bigunsigned,1,__VA_ARGS__)\
  ZYP_OP(read_only,bigint,1,__VA_ARGS__)\
  ZYP_OP(rowkey_split_pos,bigint,1,__VA_ARGS__)\
  ZYP_OP(compress_func_name,varchar,1,__VA_ARGS__)\
  ZYP_OP(expire_condition,varchar,1,__VA_ARGS__)\
  ZYP_OP(is_use_bloomfilter,bigint,1,__VA_ARGS__)\
  ZYP_OP(comment,varchar,1,__VA_ARGS__)\
  ZYP_OP(block_size,bigint,1,__VA_ARGS__)\
  ZYP_OP(collation_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(data_table_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(index_status,bigint,1,__VA_ARGS__)\
  ZYP_OP(tablegroup_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(progressive_merge_num,bigint,1,__VA_ARGS__)\
  ZYP_OP(index_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(part_level,bigint,1,__VA_ARGS__)\
  ZYP_OP(part_func_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(part_func_expr,varchar,1,__VA_ARGS__)\
  ZYP_OP(part_num,bigint,1,__VA_ARGS__)\
  ZYP_OP(sub_part_func_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(sub_part_func_expr,varchar,1,__VA_ARGS__)\
  ZYP_OP(sub_part_num,bigint,1,__VA_ARGS__)\
  ZYP_OP(view_definition,longtext,1,__VA_ARGS__)\
  ZYP_OP(view_check_option,bigint,1,__VA_ARGS__)\
  ZYP_OP(view_is_updatable,bigint,1,__VA_ARGS__)\
  ZYP_OP(index_using_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(parser_name,varchar,1,__VA_ARGS__)\
  ZYP_OP(index_attributes_set,bigint,1,__VA_ARGS__)\
  ZYP_OP(tablet_size,bigint,1,__VA_ARGS__)\
  ZYP_OP(pctfree,bigint,1,__VA_ARGS__)\
  ZYP_OP(partition_status,bigint,1,__VA_ARGS__)\
  ZYP_OP(partition_schema_version,bigint,1,__VA_ARGS__)\
  ZYP_OP(session_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(pk_comment,varchar,1,__VA_ARGS__)\
  ZYP_OP(sess_active_time,bigint,1,__VA_ARGS__)\
  ZYP_OP(row_store_type,varchar,1,__VA_ARGS__)\
  ZYP_OP(store_format,varchar,1,__VA_ARGS__)\
  ZYP_OP(duplicate_scope,bigint,1,__VA_ARGS__)\
  ZYP_OP(progressive_merge_round,bigint,1,__VA_ARGS__)\
  ZYP_OP(storage_format_version,bigint,1,__VA_ARGS__)\
  ZYP_OP(table_mode,bigint,1,__VA_ARGS__)\
  ZYP_OP(encryption,varchar,1,__VA_ARGS__)\
  ZYP_OP(tablespace_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(sub_part_template_flags,bigint,1,__VA_ARGS__)\
  ZYP_OP(dop,bigint,1,__VA_ARGS__)\
  ZYP_OP(character_set_client,bigint,1,__VA_ARGS__)\
  ZYP_OP(collation_connection,bigint,1,__VA_ARGS__)\
  ZYP_OP(auto_part_size,bigint,1,__VA_ARGS__)\
  ZYP_OP(auto_part,tinyint,1,__VA_ARGS__)\
  ZYP_OP(association_table_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(tablet_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(max_dependency_version,bigint,1,__VA_ARGS__)\
  ZYP_OP(define_user_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(transition_point,varchar,1,__VA_ARGS__)\
  ZYP_OP(b_transition_point,varchar,1,__VA_ARGS__)\
  ZYP_OP(interval_range,varchar,1,__VA_ARGS__)\
  ZYP_OP(b_interval_range,varchar,1,__VA_ARGS__)\
  ZYP_OP(object_status,bigint,1,__VA_ARGS__)\
  ZYP_OP(table_flags,bigint,1,__VA_ARGS__)\
  ZYP_OP(truncate_version,bigint,1,__VA_ARGS__)\
  ZYP_OP(external_file_location,varbinary,1,__VA_ARGS__)\
  ZYP_OP(external_file_location_access_info,varbinary,1,__VA_ARGS__)\
  ZYP_OP(external_file_format,varbinary,1,__VA_ARGS__)\
  ZYP_OP(external_file_pattern,varbinary,1,__VA_ARGS__)\
  ZYP_OP(ttl_definition,varchar,1,__VA_ARGS__)\
  ZYP_OP(kv_attributes,varchar,1,__VA_ARGS__)\
  ZYP_OP(name_generated_type,bigint,1,__VA_ARGS__)\

#define ZYP_ALL_COLUMN_HISTORY_OP(ZYP_OP,...)\
  ZYP_OP(tenant_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(column_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(schema_version,bigint,0,__VA_ARGS__)\
  ZYP_OP(gmt_create,timestamp,1,__VA_ARGS__)\
  ZYP_OP(gmt_modified,timestamp,1,__VA_ARGS__)\
  ZYP_OP(is_deleted,bigint,0,__VA_ARGS__)\
  ZYP_OP(column_name,varchar,1,__VA_ARGS__)\
  ZYP_OP(rowkey_position,bigint,1,__VA_ARGS__)\
  ZYP_OP(index_position,bigint,1,__VA_ARGS__)\
  ZYP_OP(order_in_rowkey,bigint,1,__VA_ARGS__)\
  ZYP_OP(partition_key_position,bigint,1,__VA_ARGS__)\
  ZYP_OP(data_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(data_length,bigint,1,__VA_ARGS__)\
  ZYP_OP(data_precision,bigint,1,__VA_ARGS__)\
  ZYP_OP(data_scale,bigint,1,__VA_ARGS__)\
  ZYP_OP(zero_fill,bigint,1,__VA_ARGS__)\
  ZYP_OP(nullable,bigint,1,__VA_ARGS__)\
  ZYP_OP(on_update_current_timestamp,bigint,1,__VA_ARGS__)\
  ZYP_OP(autoincrement,bigint,1,__VA_ARGS__)\
  ZYP_OP(is_hidden,bigint,1,__VA_ARGS__)\
  ZYP_OP(collation_type,bigint,1,__VA_ARGS__)\
  ZYP_OP(orig_default_value,varchar,1,__VA_ARGS__)\
  ZYP_OP(cur_default_value,varchar,1,__VA_ARGS__)\
  ZYP_OP(comment,longtext,1,__VA_ARGS__)\
  ZYP_OP(column_flags,bigint,1,__VA_ARGS__)\
  ZYP_OP(prev_column_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(extended_type_info,varbinary,1,__VA_ARGS__)\
  ZYP_OP(orig_default_value_v2,varbinary,1,__VA_ARGS__)\
  ZYP_OP(cur_default_value_v2,varbinary,1,__VA_ARGS__)\
  ZYP_OP(srs_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(udt_set_id,bigint,1,__VA_ARGS__)\
  ZYP_OP(sub_data_type,bigint,1,__VA_ARGS__)\

#define ZYP_ALL_DDL_OPERATION_OP(ZYP_OP,...)\
  ZYP_OP(schema_version,bigint,0,__VA_ARGS__)\
  ZYP_OP(gmt_create,timestamp,1,__VA_ARGS__)\
  ZYP_OP(gmt_modified,timestamp,1,__VA_ARGS__)\
  ZYP_OP(tenant_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(user_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(database_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(database_name,varchar,0,__VA_ARGS__)\
  ZYP_OP(tablegroup_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_id,bigint,0,__VA_ARGS__)\
  ZYP_OP(table_name,varchar,0,__VA_ARGS__)\
  ZYP_OP(operation_type,bigint,0,__VA_ARGS__)\
  ZYP_OP(ddl_stmt_str,longtext,0,__VA_ARGS__)\
  ZYP_OP(exec_tenant_id,bigint,0,__VA_ARGS__)\

#define timestamp int64_t
#define bigint int64_t
#define varchar ObString
#define longtext ObString
#define tinyint int8_t
#define varbinary ObString
#define bigunsigned uint64_t

static ObString ZypToString(int8_t v) {
  char buf[32];
  int nr = sprintf(buf, "%u", v);
  char* ret = ZYP_LOCAL_ALLOC(char, nr+1);
  ret[nr]=0;
  memcpy(ret, buf, nr);
  return ObString(nr, ret);
} 

static ObString ZypToString(uint64_t v) {
  char buf[32];
  int nr = sprintf(buf, "%lu", v);
  char* ret = ZYP_LOCAL_ALLOC(char, nr+1);
  ret[nr]=0;
  memcpy(ret, buf, nr);
  return ObString(nr, ret);
} 

static ObString ZypToString(int64_t v) {
  char buf[32];
  int nr = sprintf(buf, "%ld", v);
  char* ret = ZYP_LOCAL_ALLOC(char, nr+1);
  ret[nr]=0;
  memcpy(ret, buf, nr);
  return ObString(nr, ret);
} 
static ObString ZypToString(const ObString& v) {
  return v;
}

static ObString ZypCopyString(const ObString& v) {
  auto nr = v.length();
  char* ret = ZYP_LOCAL_ALLOC(char, nr+1);
  ret[nr]=0;
  memcpy(ret, v.ptr(), nr);
  return ObString(nr, ret);
} 

#define row_cnt(...) +1

#define member_name(a) a##_member_
#define member_datum_name(a) a##_member_datum_
#define member_is_null_name(a) a##_member_is_null_
#define member_is_nullable_name(a) a##_member_is_nullable_
#define set_member_name(a) set_##a
#define get_member_name(a) get_##a
#define set_member_null_name(a) set_##a##_null
#define init_datum_name(a) init_datum_##a

#define to_core_row(x,y,...) \
  tmp=ZYP_LOCAL_NEW(ZypAllCoreTableRow, "core_row");\
  tmp->set_gmt_create(now);\
  tmp->set_gmt_modified(now);\
  tmp->set_table_name(_table_name_);\
  tmp->set_row_id(row);\
  tmp->set_column_name(#x);\
  if(strcmp(#x, "gmt_create")==0||strcmp(#x, "gmt_modified")==0) \
    tmp->set_column_value("now(6)"); \
  else if(member_is_null_name(x)) tmp->set_member_null_name(column_value)();\
  else tmp->set_member_name(column_value)(ZypToString(member_name(x)));\
  ret[cnt++]=tmp;

#define member_null(a,...) \
  bool member_is_null_name(a):1;

// 之后加一个no copy的接口
#define member(a,b,...) \
  private:\
    b member_name(a);\
    static const bool member_is_nullable_name(a);\
  public:\
    int set_member_name(a)(const b& tmp) {\
      if(member_is_nullable_name(a) && need_set_null(tmp)) { \
        set_member_null_name(a)();\
      } else { \
        member_name(a) = copy(tmp);\
        member_is_null_name(a)=false; \
      }\
      return OB_SUCCESS; \
    }\
    int set_member_null_name(a)() { member_is_null_name(a)=true; return OB_SUCCESS; }\
    b& get_member_name(a)() { return member_name(a); }

#define member_static_null(a,b,c,d,...) \
  const bool d::member_is_nullable_name(a)=c;

#define member_datum(a,b,...) \
  private: \
    ObDatum *member_datum_name(a);\
    void init_datum_name(a)() { \
      if(member_is_null_name(a)) { \
        add_null(member_datum_name(a)); \
      } else { \
        add_##b(member_datum_name(a), member_name(a)); \
      } \
    }

#define init_datum(a,...) init_datum_name(a)();

#define member_datum_point_init(a,...) \
  member_datum_name(a)=&datums.at(now);now++;

#define null_class_name(class_name) class_name##_null

#define TABLE_CLASS_DECLARE(class_name, table_name, table_rows) \
  struct null_class_name(class_name) { \
    table_rows(member_null);\
    OB_INLINE null_class_name(class_name)() { memset(this, 0xff, sizeof(*this)); }\
  }; \
  class class_name : public ZypRow, public null_class_name(class_name) { \
    static const char* _table_name_; \
    table_rows(member);\
    table_rows(member_datum);\
    std::array<uint64_t, table_rows(row_cnt)> datum_buf; \
    std::array<ObDatum, table_rows(row_cnt)> datums; \
  public:\
    OB_INLINE class_name() { \
      for(int i=0;i<cells_cnt;i++){ \
        datums[i].ptr_ = (char*)&datum_buf[i]; \
      }\
      int now=0;\
      table_rows(member_datum_point_init);\
    } \
    virtual ~class_name() { \
    } \
    virtual void init_datums() { \
      table_rows(init_datum); \
    } \
    virtual ObDatum* get_datums() { return datums.data(); } \
    static const int cells_cnt = table_rows(row_cnt); \
    virtual size_t get_cells_cnt() const { return cells_cnt; }\
    template<typename T>\
    static bool need_set_null(const T&) { return false; }\
    static bool need_set_null(const ObString&t) { return t.ptr() == nullptr; }\
    template<typename T> \
    static T copy(const T&t) { return t; }  \
    static ObString copy(const ObString&t) { return ZypCopyString(t); } \
    virtual ObArray<ZypRow*> gen_core_rows(std::atomic_long& row_id) {\
      auto now = ObTimeUtility::fast_current_time();\
      int64_t row = row_id++;\
      ZypAllCoreTableRow* tmp;\
      ObArray<ZypRow*> ret;\
      ret.prepare_allocate(get_cells_cnt());\
      int cnt = 0;\
      table_rows(to_core_row);\
      return ret;\
    }\
  }; \
  const char* class_name::_table_name_ = table_name; \
  table_rows(member_static_null, class_name);

TABLE_CLASS_DECLARE(ZypAllCoreTableRow, "__all_core_table", ZYP_ALL_CORE_TABLE_OP);
TABLE_CLASS_DECLARE(ZypAllTableRow, "__all_table", ZYP_ALL_TABLE_OP);
TABLE_CLASS_DECLARE(ZypAllColumnRow, "__all_column", ZYP_ALL_COLUMN_OP);
TABLE_CLASS_DECLARE(ZypAllTableHistoryRow, "__all_table_history", ZYP_ALL_TABLE_HISTORY_OP);
TABLE_CLASS_DECLARE(ZypAllColumnHistoryRow, "__all_column_history", ZYP_ALL_COLUMN_HISTORY_OP);
TABLE_CLASS_DECLARE(ZypAllDDLOperationRow, "__all_ddl_operation", ZYP_ALL_DDL_OPERATION_OP);

#undef member
#undef row_cnt
#undef member_name
#undef member_datum_name
#undef member_is_null_name
#undef member_is_nullable_name
#undef set_member_name
#undef get_member_name
#undef set_member_null_name
#undef init_datum_name
#undef to_core_row
#undef member_static_null
#undef member_datum
#undef init_datum
#undef member_datum_point_init
#undef TABLE_CLASS_DECLARE

#undef timestamp
#undef bigint 
#undef varchar
#undef longtext
#undef tinyint
#undef varbinary
#undef bigunsigned

#undef ZYP_ALL_CORE_TABLE_OP
#undef ZYP_ALL_TABLE_OP
#undef ZYP_ALL_COLUMN_OP
#undef ZYP_ALL_TABLE_HISTORY_OP
#undef ZYP_ALL_COLUMN_HISTORY_OP
#undef ZYP_ALL_DDL_OPERATION_OP

namespace schema {

class TableBatchCreateByPass {
public:
  using StartFunc = std::function<ObISQLClient*()>;
  using EndFunc = std::function<void(ObISQLClient*)>;
  TableBatchCreateByPass(common::ObIArray<share::schema::ObTableSchema>& tables, StartFunc start, EndFunc end);
  ~TableBatchCreateByPass();

  void prepare_not_core();

  void prepare_core();

  int run();

  void init_core_all_table_idx() ;

  void init_core_all_column_idx();

  std::atomic_long& get_core_all_table_idx();

  std::atomic_long& get_core_all_column_idx();

  void gen_all_core_table(share::schema::ObTableSchema& table);

  void gen_all_table(share::schema::ObTableSchema& table);

  void gen_all_table_history(share::schema::ObTableSchema& table);

  void gen_all_column(share::schema::ObTableSchema& table);

  void gen_all_column_history(share::schema::ObTableSchema& table);

  void gen_all_ddl_operation(share::schema::ObTableSchema& table);

  void run_insert_all_core_table();

  void run_insert_all_table_history();

  void run_insert_all_table();

  void run_insert_all_column();

  void run_insert_all_column_history();

  void run_insert_all_ddl_operation();


private:
  StartFunc client_start_;
  EndFunc  client_end_;
  common::ObIArray<share::schema::ObTableSchema>& tables_;
  uint64_t exec_tenant_id_;
  uint64_t tenant_id_;
  std::atomic_long core_all_table_idx_{-1};
  std::atomic_long core_all_column_idx_{-1};
  ObISQLClient* global_client_;
  std::once_flag table_flag_;
  std::once_flag column_flag_;
  int64_t now_;

  LightyQueue all_core_table_rows_;
  LightyQueue all_table_rows_;
  LightyQueue all_column_rows_;
  LightyQueue all_table_history_rows_;
  LightyQueue all_column_history_rows_;
  LightyQueue all_ddl_operation_rows_;
};
}
namespace share {
namespace schema {
class ObTableSqlService;
}
}

namespace schema {
// 只针对view
class TableBatchCreateNormal {
public:
  using StartFunc = std::function<ObISQLClient*()>;
  using EndFunc = std::function<void(ObISQLClient*)>;
  using ObTableSqlService = share::schema::ObTableSqlService;

  TableBatchCreateNormal(ObIArray<oceanbase::share::schema::ObTableSchema>& tables, StartFunc start, EndFunc end, ObTableSqlService* sql_service);
  StartFunc client_start_;
  EndFunc  client_end_;
  ObIArray<oceanbase::share::schema::ObTableSchema>& tables_;
  void prepare();
  void run();
  LightyQueue queue_;
  ObTableSqlService* sql_service_;
  int64_t tenant_id_;
  int64_t exec_tenant_id_;
};
}

}
