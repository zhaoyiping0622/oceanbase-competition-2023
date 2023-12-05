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

namespace oceanbase {

using namespace share;

template <typename T>
int gen_table_dml(
    const uint64_t exec_tenant_id,
    const ObTableSchema &table,
    const bool update_object_status_ignore_version,
    T& t)
{

  auto now = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  uint64_t data_version = DATA_VERSION_4_2_1_0;
  if (OB_SUCC(ret)) {
    const ObPartitionOption &part_option = table.get_part_option();
    const ObPartitionOption &sub_part_option = table.get_sub_part_option();
    const char *expire_info = table.get_expire_info().length() <= 0 ?
        "" : table.get_expire_info().ptr();
    const char *part_func_expr = part_option.get_part_func_expr_str().length() <= 0 ?
        "" : part_option.get_part_func_expr_str().ptr();
    const char *sub_part_func_expr = sub_part_option.get_part_func_expr_str().length() <= 0 ?
        "" : sub_part_option.get_part_func_expr_str().ptr();
    const char *encryption = table.get_encryption_str().empty() ?
        "" : table.get_encryption_str().ptr();
    const int64_t INVALID_REPLICA_NUM = -1;
    const int64_t part_num = part_option.get_part_num();
    const int64_t sub_part_num = PARTITION_LEVEL_TWO == table.get_part_level()
                                 && table.has_sub_part_template_def() ?
                                 sub_part_option.get_part_num() : 0;
    const char *ttl_definition = table.get_ttl_definition().empty() ?
        "" : table.get_ttl_definition().ptr();
    const char *kv_attributes = table.get_kv_attributes().empty() ?
        "" : table.get_kv_attributes().ptr();
    if (OB_FAIL(t.set_tenant_id(ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table.get_tenant_id())))
        || OB_FAIL(t.set_table_id(ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, table.get_table_id())))
        || OB_FAIL(t.set_table_name(table.get_table_name()))
        || OB_FAIL(t.set_database_id(ObSchemaUtils::get_extract_schema_id(
                                                 exec_tenant_id, table.get_database_id())))
        || OB_FAIL(t.set_table_type(table.get_table_type()))
        || OB_FAIL(t.set_load_type(table.get_load_type()))
        || OB_FAIL(t.set_def_type(table.get_def_type()))
        || OB_FAIL(t.set_rowkey_column_num(table.get_rowkey_column_num()))
        || OB_FAIL(t.set_index_column_num(table.get_index_column_num()))
        || OB_FAIL(t.set_max_used_column_id(table.get_max_used_column_id()))
        || OB_FAIL(t.set_session_id(table.get_session_id()))
        || OB_FAIL(t.set_sess_active_time(table.get_sess_active_time()))
        //|| OB_FAIL(t.set_create_host(table.get_create_host()))
        || OB_FAIL(t.set_tablet_size(table.get_tablet_size()))
        || OB_FAIL(t.set_pctfree(table.get_pctfree()))
        || OB_FAIL(t.set_autoinc_column_id(table.get_autoinc_column_id()))
        || OB_FAIL(t.set_auto_increment(share::ObRealUInt64(table.get_auto_increment()).value()))
        || OB_FAIL(t.set_read_only(table.is_read_only()))
        || OB_FAIL(t.set_rowkey_split_pos(table.get_rowkey_split_pos()))
        || OB_FAIL(t.set_compress_func_name(table.get_compress_func_name()))
        || OB_FAIL(t.set_expire_condition(expire_info))
        || OB_FAIL(t.set_is_use_bloomfilter(table.is_use_bloomfilter()))
        || OB_FAIL(t.set_index_attributes_set(table.get_index_attributes_set()))
        || OB_FAIL(t.set_comment(table.get_comment()))
        || OB_FAIL(t.set_block_size(table.get_block_size()))
        || OB_FAIL(t.set_collation_type(table.get_collation_type()))
        || OB_FAIL(t.set_data_table_id(ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, table.get_data_table_id())))
        || OB_FAIL(t.set_index_status(table.get_index_status()))
        || OB_FAIL(t.set_tablegroup_id(ObSchemaUtils::get_extract_schema_id(
                                                   exec_tenant_id, table.get_tablegroup_id())))
        || OB_FAIL(t.set_progressive_merge_num(table.get_progressive_merge_num()))
        || OB_FAIL(t.set_index_type(table.get_index_type()))
        || OB_FAIL(t.set_index_using_type(table.get_index_using_type()))
        || OB_FAIL(t.set_part_level(table.get_part_level()))
        || OB_FAIL(t.set_part_func_type(part_option.get_part_func_type()))
        || OB_FAIL(t.set_part_func_expr(part_func_expr))
        || OB_FAIL(t.set_part_num(part_num))
        || OB_FAIL(t.set_sub_part_func_type(sub_part_option.get_part_func_type()))
        || OB_FAIL(t.set_sub_part_func_expr(sub_part_func_expr))
        || OB_FAIL(t.set_sub_part_num(sub_part_num))
        || OB_FAIL(t.set_schema_version(table.get_schema_version()))
        || OB_FAIL(t.set_view_definition(table.get_view_schema().get_view_definition()))
        || OB_FAIL(t.set_view_check_option(table.get_view_schema().get_view_check_option()))
        || OB_FAIL(t.set_view_is_updatable(table.get_view_schema().get_view_is_updatable()))
        || OB_FAIL(t.set_parser_name(table.get_parser_name_str()))
        || OB_FAIL(t.set_gmt_create(now))
        || OB_FAIL(t.set_gmt_modified(now))
        || OB_FAIL(t.set_partition_status(table.get_partition_status()))
        || OB_FAIL(t.set_partition_schema_version(table.get_partition_schema_version()))
        || OB_FAIL(t.set_pk_comment(table.get_pk_comment()))
        || OB_FAIL(t.set_row_store_type(
                    ObStoreFormat::get_row_store_name(table.get_row_store_type())))
        || OB_FAIL(t.set_store_format(
                    ObStoreFormat::get_store_format_name(table.get_store_format())))
        || OB_FAIL(t.set_duplicate_scope(int64_t(table.get_duplicate_scope())))
        || OB_FAIL(t.set_progressive_merge_round(table.get_progressive_merge_round()))
        || OB_FAIL(t.set_storage_format_version(table.get_storage_format_version()))
        || OB_FAIL(t.set_table_mode(table.get_table_mode()))
        || OB_FAIL(t.set_encryption(encryption))
        || OB_FAIL(t.set_tablespace_id(ObSchemaUtils::get_extract_schema_id(
                                  exec_tenant_id, table.get_tablespace_id())))
        // To avoid compatibility problems (such as error while upgrade virtual schema) in upgrade post stage,
        // cluster version judgemenet is needed if columns are added in upgrade post stage.
        || OB_FAIL(t.set_sub_part_template_flags(table.get_sub_part_template_flags()))
        || OB_FAIL(t.set_dop(table.get_dop()))
        || OB_FAIL(t.set_character_set_client(table.get_view_schema().get_character_set_client()))
        || OB_FAIL(t.set_collation_connection(table.get_view_schema().get_collation_connection()))
        || OB_FAIL(t.set_auto_part(table.get_part_option().is_auto_range_part()))
        || OB_FAIL(t.set_auto_part_size(table.get_part_option().get_auto_part_size()))
        || OB_FAIL(t.set_association_table_id(ObSchemaUtils::get_extract_schema_id(
                                      exec_tenant_id, table.get_association_table_id())))
        || OB_FAIL(t.set_define_user_id(ObSchemaUtils::get_extract_schema_id(
                                      exec_tenant_id, table.get_define_user_id())))
        || OB_FAIL(t.set_max_dependency_version(table.get_max_dependency_version()))
        || (OB_FAIL(t.set_tablet_id(table.get_tablet_id().id())))
        || ((data_version >= DATA_VERSION_4_1_0_0 || update_object_status_ignore_version)
            && OB_FAIL(t.set_object_status(static_cast<int64_t> (table.get_object_status()))))
        || (data_version >= DATA_VERSION_4_1_0_0
            && OB_FAIL(t.set_table_flags(table.get_table_flags())))
        || (data_version >= DATA_VERSION_4_1_0_0
            && OB_FAIL(t.set_truncate_version(table.get_truncate_version())))
        || (data_version >= DATA_VERSION_4_2_0_0
            && OB_FAIL(t.set_external_file_location(table.get_external_file_location())))
        || (data_version >= DATA_VERSION_4_2_0_0
            && OB_FAIL(t.set_external_file_location_access_info(table.get_external_file_location_access_info())))
        || (data_version >= DATA_VERSION_4_2_0_0
            && OB_FAIL(t.set_external_file_format(table.get_external_file_format())))
        || (data_version >= DATA_VERSION_4_2_0_0
            && OB_FAIL(t.set_external_file_pattern(table.get_external_file_pattern())))
        || (data_version >= DATA_VERSION_4_2_1_0
            && OB_FAIL(t.set_ttl_definition(ttl_definition)))
        || (data_version >= DATA_VERSION_4_2_1_0
            && OB_FAIL(t.set_kv_attributes(kv_attributes)))
        || (data_version >= DATA_VERSION_4_2_1_0
            && OB_FAIL(t.set_name_generated_type(table.get_name_generated_type())))
        ) {
      LOG_WARN("add column failed", K(ret));
    }
  }
  return ret;
}
template<typename T>
int gen_column_dml(
    const uint64_t exec_tenant_id,
    const ObColumnSchemaV2 &column, T& t)
{
  int ret = OB_SUCCESS;
  ObString orig_default_value;
  ObString cur_default_value;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA);
  char *extended_type_info_buf = NULL;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(exec_tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_2_0_0 &&
             (column.is_xmltype() || column.get_udt_set_id() != 0 || column.get_sub_data_type() != 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.2, xmltype type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2, xmltype");
  } else if (tenant_data_version < DATA_VERSION_4_1_0_0 && ob_is_json(column.get_data_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.1, json type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, json type");
  } else if (tenant_data_version < DATA_VERSION_4_1_0_0 &&
             (ob_is_geometry(column.get_data_type()) ||
             column.get_srs_id() != OB_DEFAULT_COLUMN_SRS_ID ||
             column.is_spatial_generated_column())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.1, geometry type is not supported", K(ret), K(tenant_data_version), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.1, geometry type");
  } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(column.get_charset_type(),
                                                                    exec_tenant_id))) {
    LOG_WARN("failed to check charset data version valid", K(ret));
  } else if (column.is_generated_column() ||
      column.is_identity_column() ||
      ob_is_string_type(column.get_data_type()) ||
      ob_is_json(column.get_data_type()) ||
      ob_is_geometry(column.get_data_type())) {
    //The default value of the generated column is the expression definition of the generated column
    ObString orig_default_value_str = column.get_orig_default_value().get_string();
    ObString cur_default_value_str = column.get_cur_default_value().get_string();
    orig_default_value.assign_ptr(orig_default_value_str.ptr(), orig_default_value_str.length());
    cur_default_value.assign_ptr(cur_default_value_str.ptr(), cur_default_value_str.length());
    if (!column.get_orig_default_value().is_null() && OB_ISNULL(orig_default_value.ptr())) {
      orig_default_value.assign_ptr("", 0);
    }
    if (!column.get_cur_default_value().is_null() && OB_ISNULL(cur_default_value.ptr())) {
      cur_default_value.assign_ptr("", 0);
    }
  } else {
    const int64_t value_buf_len = 2 * OB_MAX_DEFAULT_VALUE_LENGTH + 3;
    char *orig_default_value_buf = NULL;
    char *cur_default_value_buf = NULL;
    orig_default_value_buf = static_cast<char *>(allocator.alloc(value_buf_len));
    cur_default_value_buf = static_cast<char *>(allocator.alloc(value_buf_len));
    extended_type_info_buf = static_cast<char *>(allocator.alloc(OB_MAX_VARBINARY_LENGTH));
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    if (OB_ISNULL(orig_default_value_buf)
        || OB_ISNULL(cur_default_value_buf)
        || OB_ISNULL(extended_type_info_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for default value buffer failed");
    } else if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(
               column.get_tenant_id(), column.get_table_id(), compat_mode))) {
      LOG_WARN("fail to get tenant mode", K(ret), K(column));
    } else {
      MEMSET(orig_default_value_buf, 0, value_buf_len);
      MEMSET(cur_default_value_buf, 0, value_buf_len);
      MEMSET(extended_type_info_buf, 0, OB_MAX_VARBINARY_LENGTH);

      int64_t orig_default_value_len = 0;
      int64_t cur_default_value_len = 0;
      lib::CompatModeGuard compat_mode_guard(compat_mode);
      ObTimeZoneInfo tz_info;
      if (OB_FAIL(OTTZ_MGR.get_tenant_tz(exec_tenant_id, tz_info.get_tz_map_wrap()))) {
        LOG_WARN("get tenant timezone failed", K(ret));
      } else if (OB_FAIL(column.get_orig_default_value().print_plain_str_literal(
                      orig_default_value_buf, value_buf_len, orig_default_value_len, &tz_info))) {
        LOG_WARN("failed to print orig default value", K(ret),
                 K(value_buf_len), K(orig_default_value_len));
      } else if (OB_FAIL(column.get_cur_default_value().print_plain_str_literal(
                             cur_default_value_buf, value_buf_len, cur_default_value_len, &tz_info))) {
        LOG_WARN("failed to print cur default value",
                 K(ret), K(value_buf_len), K(cur_default_value_len));
      } else {
        orig_default_value.assign_ptr(orig_default_value_buf, static_cast<int32_t>(orig_default_value_len));
        cur_default_value.assign_ptr(cur_default_value_buf, static_cast<int32_t>(cur_default_value_len));
      }
      LOG_TRACE("begin gen_column_dml", K(ret), K(compat_mode), K(orig_default_value), K(cur_default_value),  K(orig_default_value_len), K(cur_default_value_len));
    }
  }
  if(strcmp(column.get_column_name(), "auto_increment") == 0 && column.get_table_id() == 3) {
    LOG_INFO("zyp target", K(orig_default_value.ptr()==nullptr), K(orig_default_value), K(cur_default_value), K(cur_default_value.ptr()==nullptr));
  }
  LOG_TRACE("begin gen_column_dml", K(ret), K(orig_default_value), K(cur_default_value), K(column));
  if (OB_SUCC(ret)) {
    ObString cur_default_value_v1;
    if (column.get_orig_default_value().is_null()) {
      orig_default_value.reset();
    }
    if (column.get_cur_default_value().is_null()) {
      cur_default_value.reset();
    }
    ObString bin_extended_type_info;
    if (OB_SUCC(ret) && column.is_enum_or_set()) {
      int64_t pos = 0;
      extended_type_info_buf = static_cast<char *>(allocator.alloc(OB_MAX_VARBINARY_LENGTH));
      if (OB_ISNULL(extended_type_info_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for default value buffer failed");
      } else if (OB_FAIL(column.serialize_extended_type_info(extended_type_info_buf, OB_MAX_VARBINARY_LENGTH, pos))) {
        LOG_WARN("fail to serialize_extended_type_info", K(ret));
      } else {
        bin_extended_type_info.assign_ptr(extended_type_info_buf, static_cast<int32_t>(pos));
      }
    }
    auto now = ObTimeUtility::fast_current_time();
    if (OB_SUCC(ret) && (OB_FAIL(t.set_tenant_id(ObSchemaUtils::get_extract_tenant_id(
                                                    exec_tenant_id, column.get_tenant_id())))
                         || OB_FAIL(t.set_table_id(ObSchemaUtils::get_extract_schema_id(
                                                    exec_tenant_id, column.get_table_id())))
                         || OB_FAIL(t.set_column_id(column.get_column_id()))
                         || OB_FAIL(t.set_column_name(column.get_column_name()))
                         || OB_FAIL(t.set_rowkey_position(column.get_rowkey_position()))
                         || OB_FAIL(t.set_index_position(column.get_index_position()))
                         || OB_FAIL(t.set_partition_key_position(column.get_tbl_part_key_pos()))
                         || OB_FAIL(t.set_data_type(column.get_data_type()))
                         || OB_FAIL(t.set_data_length(column.get_data_length()))
                         || OB_FAIL(t.set_data_precision(column.get_data_precision()))
                         || OB_FAIL(t.set_data_scale(column.get_data_scale()))
                         || OB_FAIL(t.set_zero_fill(column.is_zero_fill()))
                         || OB_FAIL(t.set_nullable(column.is_nullable()))
                         || OB_FAIL(t.set_autoincrement(column.is_autoincrement()))
                         || OB_FAIL(t.set_is_hidden(column.is_hidden()))
                         || OB_FAIL(t.set_on_update_current_timestamp(column.is_on_update_current_timestamp()))
                         || OB_FAIL(t.set_orig_default_value_v2(orig_default_value))
                         || OB_FAIL(t.set_cur_default_value_v2(cur_default_value))
                         || OB_FAIL(t.set_cur_default_value(cur_default_value_v1))
                         || OB_FAIL(t.set_order_in_rowkey(column.get_order_in_rowkey()))
                         || OB_FAIL(t.set_collation_type(column.get_collation_type()))
                         || OB_FAIL(t.set_comment(column.get_comment()))
                         || OB_FAIL(t.set_schema_version(column.get_schema_version()))
                         || OB_FAIL(t.set_column_flags(column.get_stored_column_flags()))
                         || OB_FAIL(t.set_extended_type_info(bin_extended_type_info))
                         || OB_FAIL(t.set_prev_column_id(column.get_prev_column_id()))
                         || (tenant_data_version >= DATA_VERSION_4_1_0_0 && OB_FAIL(t.set_srs_id(column.get_srs_id())))
                            // todo : tenant_data_version >= DATA_VERSION_4_2_0_0
                         || (tenant_data_version >= DATA_VERSION_4_2_0_0 && OB_FAIL(t.set_udt_set_id(column.get_udt_set_id())))
                         || (tenant_data_version >= DATA_VERSION_4_2_0_0 &&OB_FAIL(t.set_sub_data_type(column.get_sub_data_type())))
                         || OB_FAIL(t.set_gmt_create(now))
                         || OB_FAIL(t.set_gmt_modified(now)))) {
      LOG_WARN("dml add column failed", K(ret));
    }
  }
  LOG_DEBUG("gen column dml", K(exec_tenant_id), K(column.get_tenant_id()), K(column.get_table_id()), K(column.get_column_id()),
            K(column.is_nullable()), K(column.get_stored_column_flags()), K(column.get_column_flags()));
  return ret;
}


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
  tenant_id_ = tables_.at(0).get_tenant_id();
  exec_tenant_id_ = ObSchemaUtils::get_exec_tenant_id(tenant_id_);
  global_client_ = client_start_();
  now_ = ObTimeUtility::current_time();
  all_core_table_rows_.init(10000);  
  all_table_rows_.init(tables.count());  
  all_column_rows_.init(20000);  
  all_table_history_rows_.init(tables.count());  
  all_column_history_rows_.init(20000);  
  all_ddl_operation_rows_.init(tables.count());
}

TableBatchCreateByPass::~TableBatchCreateByPass() {
  client_end_(global_client_);
  // auto free_all=[&](ObArray<ZypRow*>&tmp) { for(int i=0;i<tmp.count();i++)OB_DELETE(ZypRow, "create_table", tmp.at(i)); };
  // free_all(all_core_table_rows_);
  // free_all(all_table_rows_);
  // free_all(all_column_rows_);
  // free_all(all_table_history_rows_);
  // free_all(all_column_history_rows_);
  // free_all(all_ddl_operation_rows_);
}

void TableBatchCreateByPass::run_parallel(ParallelRunner func, std::function<bool()> run) {
  int ret = OB_SUCCESS;
  auto trace_id = ObCurTraceId::get_trace_id();
  auto runner = [&]() {
    if(trace_id!=nullptr) {
      ObCurTraceId::set(*trace_id);
    }
    while(run()) func(); 
  };
  OBCreateSchemaParallel csp(runner);
  if(OB_FAIL(csp.init())){
    LOG_WARN("failed to init csp", K(ret));
  } else if(OB_FAIL(csp.start())) {
    LOG_WARN("failed to start csp", K(ret));
  } else {
    LOG_INFO("csp wait begin");
    csp.wait();
    LOG_INFO("csp wait done");
    csp.destroy();
    LOG_INFO("csp destroy");
  }
}

void TableBatchCreateByPass::run_parallel(std::vector<ParallelRunner> &funcs) {
  int ret = OB_SUCCESS;
  std::atomic_int idx{0};
  run_parallel([&]() {
    int now = idx++;
    if(now<funcs.size()) funcs[now]();
  }, [&](){return idx<funcs.size();});
}

void TableBatchCreateByPass::run_parallel_range(int beg, int end, std::function<void(int)> func) {
  std::atomic_int idx{beg};
  run_parallel([&]() { int now = idx++; if(now<end) func(now); }, [&](){return idx<end;});
}

void TableBatchCreateByPass::prepare_not_core() {
  auto base_func = [&](int i) {
    auto& table = tables_.at(i);
    auto table_id = table.get_table_id();
    if(!is_core_table(table_id)) {
      gen_all_table(table);
      gen_all_column(table);
      gen_all_table_history(table);
      gen_all_column_history(table);
      gen_all_ddl_operation(table);
    }
  };
  run_parallel_range(0, (int)tables_.count(), base_func);
  LOG_INFO("prepare_not_core", K(tables_.count()), K(all_table_rows_.size()), K(all_column_rows_.size()), K(all_table_history_rows_.size()), K(all_column_history_rows_.size()), K(all_ddl_operation_rows_.size()));
}

void TableBatchCreateByPass::prepare_core() {
  run_parallel_range(0, (int)tables_.count(), [&](int i) {
    auto& table = tables_.at(i);
    auto table_id = table.get_table_id();
    if(is_core_table(table_id)) {
      gen_all_core_table(table);
    }
  });
  LOG_INFO("prepare_core", K(tables_.count()), K(all_core_table_rows_.size()));
}
int TableBatchCreateByPass::run() {
  int ret = OB_SUCCESS;
  LOG_INFO("TableBatchCreateByPass run begin");
  DEFER({LOG_INFO("TableBatchCreateByPass run end");});

  std::vector<ZypInsertInfo*> insert_info={
    OB_NEW(ZypInsertInfo, "insert_info", all_core_table_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", all_table_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", all_column_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", all_table_history_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", all_column_history_rows_),
    OB_NEW(ZypInsertInfo, "insert_info", all_ddl_operation_rows_),
  };

  std::vector<std::function<void()>> run_insert = {
    [&](){ run_insert_all_core_table(); },
    [&](){ run_insert_all_table(); },
    [&](){ run_insert_all_column(); },
    [&](){ run_insert_all_table_history(); },
    [&](){ run_insert_all_column_history(); },
    [&](){ run_insert_all_ddl_operation(); },
  };

  DEFER({for(auto x:insert_info)OB_DELETE(ZypInsertInfo, "insert_info", x);});

  auto find_max_idx=[&]() {
    int ret = -1;
    long max_count=-1;
    for(int i=0;i<insert_info.size();i++) {
      auto t = insert_info[i]->count();
      if(t>0&&t>max_count)max_count=t, ret=i;
    }
    LOG_INFO("find_max_idx", K(ret), K(max_count));
    return ret;
  };

  auto run=[&](int idx) {
    zyp_enable();
    DEFER({zyp_disable();});
    ::zyp_insert_info = insert_info[idx];
    LOG_INFO("::zyp_insert_info", K(::zyp_insert_info));
    run_insert[idx]();
    LOG_INFO("insert done", K(idx));
  };
    
  std::atomic_int idx{0};

  run_parallel([&]() {
    int a = idx++;
    while (a<run_insert.size() && insert_info[a]->count()) {
      run(a);
    }
    a = find_max_idx();
    if(a!=-1) run(a);
  }, [&]() { return find_max_idx() != -1; });

  return ret;
}

void TableBatchCreateByPass::init_core_all_table_idx() {
  ObCoreTableProxy kv(OB_ALL_TABLE_TNAME, *global_client_, tenant_id_);
  kv.load();
  core_all_table_idx_ = kv.row_count()+1;
}

void TableBatchCreateByPass::init_core_all_column_idx() {
  ObCoreTableProxy kv(OB_ALL_COLUMN_TNAME, *global_client_, tenant_id_);
  kv.load();
  core_all_column_idx_ = kv.row_count()+1;
}

std::atomic_long& TableBatchCreateByPass::get_core_all_table_idx() {
  if(OB_UNLIKELY(core_all_table_idx_ == -1)) {
    std::call_once(table_flag_, [&]() {init_core_all_table_idx();});
  }
  return core_all_table_idx_;
}

std::atomic_long& TableBatchCreateByPass::get_core_all_column_idx() {
  if(OB_UNLIKELY(core_all_column_idx_ == -1)) {
    std::call_once(column_flag_, [&]() {init_core_all_column_idx();});
  }
  return core_all_column_idx_;
}

void TableBatchCreateByPass::gen_all_core_table(ObTableSchema& table) {
  auto table_id = table.get_table_id();
  int ret = OB_SUCCESS;
  if(is_core_table(table_id)) {
    ZypAllTableRow* row = OB_NEW(ZypAllTableRow, "create_table");
    oceanbase::gen_table_dml(exec_tenant_id_, table, false, *row);
    auto tmp = row->gen_core_rows(get_core_all_table_idx());
    for(int i=0;i<tmp.count();i++)all_core_table_rows_.push(tmp.at(i));
    OB_DELETE(ZypAllTableRow, "create_table", row);
    if (!table.is_view_table() || table.view_column_filled()) {
      for(auto it = table.column_begin();OB_SUCC(ret)&&it!=table.column_end();it++) {
        ObColumnSchemaV2 column = **it;
        ZypAllColumnRow* row = OB_NEW(ZypAllColumnRow, "create_table");
        if(OB_FAIL(oceanbase::gen_column_dml(exec_tenant_id_, column, *row))) {
          LOG_WARN("fail to gen_column_dml", KR(ret));
        } else {
          auto tmp=row->gen_core_rows(get_core_all_column_idx());
          for(int i=0;i<tmp.count();i++)all_core_table_rows_.push(tmp.at(i));
          OB_DELETE(ZypAllColumnRow, "create_table", row);
        }
      }
    }
  }
}

void TableBatchCreateByPass::gen_all_table(ObTableSchema& table) {
  if(!is_core_table(table.get_table_id())&&!table.is_view_table()) {
    ZypAllTableRow* row = OB_NEW(ZypAllTableRow, "create_table");
    oceanbase::gen_table_dml(exec_tenant_id_, table, false, *row);
    all_table_rows_.push(row);
  }
}

void TableBatchCreateByPass::gen_all_table_history(ObTableSchema& table) {
  if(table.is_view_table()) return;
  ZypAllTableHistoryRow* row = OB_NEW(ZypAllTableHistoryRow, "create_table");
  oceanbase::gen_table_dml(exec_tenant_id_, table, false, *row);
  row->set_is_deleted(0);
  all_table_history_rows_.push(row);
}

void TableBatchCreateByPass::gen_all_column(ObTableSchema& table) {
  int ret = OB_SUCCESS;
  auto table_id = table.get_table_id();
  if(table.is_view_table()) return;
  if (!table.is_view_table() || table.view_column_filled()) {
    for(auto it = table.column_begin();OB_SUCC(ret)&&it!=table.column_end();it++) {
      ObColumnSchemaV2 column = **it;
      ZypAllColumnRow* row = OB_NEW(ZypAllColumnRow, "create_table");
      if(OB_FAIL(oceanbase::gen_column_dml(exec_tenant_id_, column, *row))) {
        LOG_WARN("fail to gen_column_dml", KR(ret));
      } else {
        all_column_rows_.push(row);
      }
    }
  }
}

void TableBatchCreateByPass::gen_all_column_history(ObTableSchema& table) {
  int ret = OB_SUCCESS;
  auto table_id = table.get_table_id();
  if (!table.is_view_table() || table.view_column_filled()) {
    for(auto it = table.column_begin();OB_SUCC(ret)&&it!=table.column_end();it++) {
      ObColumnSchemaV2 column = **it;
      ZypAllColumnHistoryRow* row = OB_NEW(ZypAllColumnHistoryRow, "create_table");
      row->set_is_deleted(0);
      if(OB_FAIL(oceanbase::gen_column_dml(exec_tenant_id_, column, *row))) {
        LOG_WARN("fail to gen_column_dml", KR(ret));
      } else {
        all_column_history_rows_.push(row);
      }
    }
  }
}

void TableBatchCreateByPass::gen_all_ddl_operation(ObTableSchema& table) {
  int ret = OB_SUCCESS;
  auto table_id = table.get_table_id();
  ZypAllDDLOperationRow* row = OB_NEW(ZypAllDDLOperationRow, "create_table");
  ObSchemaOperation opt;
  opt.tenant_id_ = tenant_id_;
  opt.database_id_ = table.get_database_id();
  opt.tablegroup_id_ = table.get_tablegroup_id();
  opt.table_id_ = table.get_table_id();
  if (table.is_index_table()) {
    opt.op_type_ = table.is_global_index_table() ? OB_DDL_CREATE_GLOBAL_INDEX : OB_DDL_CREATE_INDEX;
  } else if (table.is_view_table()){
    opt.op_type_ = OB_DDL_CREATE_VIEW;
  } else {
    opt.op_type_ = OB_DDL_CREATE_TABLE;
  }
  opt.schema_version_ = table.get_schema_version();
  opt.ddl_stmt_str_ = ObString();
  row->set_schema_version(opt.schema_version_);
  row->set_tenant_id(is_tenant_operation(opt.op_type_)?opt.tenant_id_:OB_INVALID_TENANT_ID);
  row->set_exec_tenant_id(exec_tenant_id_);
  row->set_user_id(opt.user_id_);
  row->set_database_id(opt.database_id_);
  row->set_database_name(opt.database_name_);
  row->set_tablegroup_id(opt.tablegroup_id_);
  row->set_table_id(opt.table_id_);
  row->set_table_name(opt.table_name_);
  row->set_operation_type(opt.op_type_);
  row->set_ddl_stmt_str("");
  row->set_gmt_modified(now_);
  row->set_gmt_create(now_);
  all_ddl_operation_rows_.push(row);
}

void TableBatchCreateByPass::run_insert_all_core_table() { 
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
}

void TableBatchCreateByPass::run_insert_all_table_history() { 
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
}

void TableBatchCreateByPass::run_insert_all_table() { 
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
}

void TableBatchCreateByPass::run_insert_all_column() { 
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
}

void TableBatchCreateByPass::run_insert_all_column_history() { 
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
}
void TableBatchCreateByPass::run_insert_all_ddl_operation() { 
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
};


}

}
