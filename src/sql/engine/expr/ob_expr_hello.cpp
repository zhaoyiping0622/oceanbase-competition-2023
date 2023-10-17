/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_hello.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

////////////////////////////////////////////////////////////////
const char ObExprHello::hello_world[] = "Hello, OceanBase Competition!";
const int64_t ObExprHello::hello_len = sizeof(hello_world) - 1;
ObExprHello::ObExprHello(common::ObIAllocator& alloc)
    :ObStringExprOperator(alloc, T_FUN_SYS_HELLO, "hello", 0, NOT_VALID_FOR_GENERATED_COL)
{
}

int ObExprHello::calc_result_type0(ObExprResType& res_type, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  res_type.set_varchar();
  res_type.set_default_collation_type();
  res_type.set_collation_level(CS_LEVEL_SYSCONST);
  res_type.set_length(static_cast<ObLength>(hello_len));
  return ret;
}

int ObExprHello::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprHello::eval;
  return OB_SUCCESS;
}

int ObExprHello::eval(const ObExpr& expr, ObEvalCtx &ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    expr_datum.set_string(hello_world, sizeof(hello_world) - 1);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
