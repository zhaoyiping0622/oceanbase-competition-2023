#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_SCHEMA_PARALLEL_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_SCHEMA_PARALLEL_H_
#include "lib/thread/thread_mgr_interface.h"
#include <vector>
namespace oceanbase {
const int CreateSchemaParallelCnt = 8;
class OBCreateSchemaParallel : public lib::TGRunnable {
 public:
  OBCreateSchemaParallel(std::function<void()> func);
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
  std::function<void()> func_;
  std::atomic_int cnt_{0};
};

class ParallelRunner {
public:
  using ParallelRunnerFunc = std::function<void()>;

  void run_parallel(ParallelRunnerFunc func, std::function<bool()> end);

  void run_parallel(std::vector<ParallelRunnerFunc> &funcs);

  void run_parallel_range(int beg, int end, std::function<void(int)> func);
};

}  // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_CREATE_SCHEMA_PARALLEL_H_
