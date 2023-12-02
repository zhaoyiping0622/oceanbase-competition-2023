#include<lib/string/ob_string.h>

extern __thread bool zyp_come;
using zyp_string = oceanbase::common::ObString;
extern zyp_string zyp_extra_info;

#define ZYP_LOG_INFO(...) if(zyp_come) LOG_INFO(__VA_ARGS__)

void zyp_enable();
bool zyp_enabled();
void zyp_disable();
void zyp_set_extra(const zyp_string&s);

void zyp_unlimit_log(char* buf, size_t size);
