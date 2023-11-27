#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_SCHEMA_PARALLEL_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_SCHEMA_PARALLEL_H_
#include "lib/thread/thread_mgr_interface.h"
namespace oceanbase {
class OBCreateSchemaParallel : public lib::TGRunnable {
 public:
  OBCreateSchemaParallel();
  virtual ~OBCreateSchemaParallel();
  int init();
  int start();
  int stop();
  void wait();
  void destroy();

  void run1();

 private:
  bool is_inited_{false};
  int tg_id_;
};
}  // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_CREATE_SCHEMA_PARALLEL_H_
