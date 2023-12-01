#include "ob_create_schema_parallel.h"

#include <functional>

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/signal/ob_signal_utils.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_thread_mgr.h"

#define USING_LOG_PREFIX BOOTSTRAP

namespace oceanbase {
const int CreateSchemaParallelCnt = 8;
OBCreateSchemaParallel::OBCreateSchemaParallel(std::function<void()> func)
    : func_(func) {}
OBCreateSchemaParallel::~OBCreateSchemaParallel() {}
int OBCreateSchemaParallel::init() {
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(
                 TG_CREATE_TENANT(lib::TGDefIDs::ZYPCreateSchema, tg_id_))) {
    LOG_WARN("create thread for OBCreateSchemaParallel failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}
int OBCreateSchemaParallel::start() {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("OBCreateSchemaParallel not init", K(ret), K(this));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    LOG_WARN("start thread failed", K(ret));
  }
  return ret;
}
int OBCreateSchemaParallel::stop() {
  TG_STOP(tg_id_);
  return OB_SUCCESS;
}
void OBCreateSchemaParallel::wait() { TG_WAIT(tg_id_); }
void OBCreateSchemaParallel::destroy() { TG_DESTROY(tg_id_); }
void OBCreateSchemaParallel::run1() {
  if(cnt_++>=CreateSchemaParallelCnt){
    --cnt_;
    return;
  }
  DEFER({--cnt_;});
  lib::set_thread_name("OBCreateSchemaParallel");
  func_();
}
}  // namespace oceanbase
