#include "share/ob_zyp.h"
#include <mutex>

__thread bool zyp_come = false;
zyp_string zyp_extra_info;

void zyp_enable() {zyp_come=true;}
void zyp_disable() {zyp_come=false;}
void zyp_set_extra(const zyp_string&s) {zyp_extra_info=s;}

int zyp_fd=-1;
__attribute__((constructor)) void init() {
  zyp_fd = open("/home/zhaoyiping/logs/zyp_log", O_CREAT|O_WRONLY|O_TRUNC, 0644);
}
__attribute__((destructor)) void fini() {
  if(zyp_fd!=-1) close(zyp_fd);
}

std::mutex mutex;
void zyp_unlimit_log(char* buf, size_t size) {
  if(zyp_fd==-1)return;
  mutex.lock();
  write(zyp_fd, buf, size);
  mutex.unlock();
}

bool zyp_enabled(){return zyp_come;}
