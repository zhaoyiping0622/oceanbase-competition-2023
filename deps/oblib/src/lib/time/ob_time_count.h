#ifndef _OCEANBASE_COMMON_OB_TIME_COUNT_H_
#define _OCEANBASE_COMMON_OB_TIME_COUNT_H_
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
namespace oceanbase {
namespace common {
class FuncTimer {
 public:
  FuncTimer(ObString name, int line) : name_(name), line_(line) {
    // LOG_INFO("FuncTimer func begin, name ", K(name_), K(line_));
    begin_ = ObTimeUtility::current_time_ns();
  }

  ~FuncTimer() {
    auto delta_time = (ObTimeUtility::current_time_ns() - begin_) / 1000.;
    // LOG_INFO("FuncTimer func end, ", K(name_), K(line_), K(delta_time));
  }

 private:
  ObString name_;
  int line_;
  int64_t begin_;
};
}  // namespace common
}  // namespace oceanbase

#define OB_ZYP_TIME_COUNT FuncTimer ___(ObString(__func__), __LINE__)
#define OB_ZYP_TIME_COUNT_BEGIN(x) auto* zyp_counter##x = new FuncTimer(ObString(__func__), __LINE__)
#define OB_ZYP_TIME_COUNT_END(x) delete zyp_counter##x

#endif  // _OCEANBASE_COMMON_OB_TIME_COUNT_H_
