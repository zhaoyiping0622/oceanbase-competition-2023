#include"lib/string/ob_string.h"
#include"lib/container/ob_array.h"
#include"common/object/ob_object.h"
#include"common/row/ob_row.h"

extern __thread bool zyp_come;
using zyp_string = oceanbase::common::ObString;
extern zyp_string zyp_extra_info;

#define ZYP_LOG_INFO(...) if(zyp_come) LOG_INFO(__VA_ARGS__)

void zyp_enable();
bool zyp_enabled();
void zyp_disable();
void zyp_set_extra(const zyp_string&s);

void zyp_unlimit_log(const char* buf, size_t size);

class ZypRow {
  public:
    using ObObj = oceanbase::common::ObObj;
    using ObNewRow = oceanbase::common::ObNewRow;
    template<typename T>
    using ObArray = oceanbase::common::ObArray<T>;
    using ObString = oceanbase::common::ObString;
    virtual void init_objs() = 0;
    virtual ObObj* get_cells() = 0;
    virtual size_t get_cells_cnt() = 0;
    oceanbase::common::ObNewRow new_row();
    virtual ObArray<ZypRow*> gen_core_rows(std::atomic_long &row_id) = 0;
    virtual ~ZypRow() {}
    void add_varchar(ObObj* obj, const ObString&s);
    void add_varbinary(ObObj* obj, const ObString&s);
    void add_longtext(ObObj* obj, const ObString&s);
    void add_bigint(ObObj* obj, int64_t v);
    void add_tinyint(ObObj* obj, int8_t v);
    void add_bigunsigned(ObObj* obj, uint64_t v);
    void add_null(ObObj* obj);
    void add_timestamp(ObObj* obj, int64_t timestamp);
    size_t to_string(const char* buf, size_t size) const {return 0;}
  private:
    ObArray<ObObj> cells_;
};

class ZypInsertInfo {
public:
  template<typename T>
  using ObArray = oceanbase::common::ObArray<T>;
  ZypInsertInfo(ObArray<ZypRow*>& array):array_(array),idx_(0) {}
  ZypRow* get_row() {
    long now = idx_++;
    if(now<array_.count()) {
      return array_.at(now);
    }
    return nullptr;
  }
  long count() {
    long ret = array_.count()-idx_;
    if(ret<0)return 0;
    return ret;
  }
private:
  ObArray<ZypRow*>& array_;
  std::atomic_long idx_;
};

extern __thread ZypInsertInfo* zyp_insert_info;
