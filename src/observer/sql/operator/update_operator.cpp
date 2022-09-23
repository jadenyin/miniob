#include "common/log/log.h"
#include "sql/operator/update_operator.h"
#include "storage/common/record.h"
#include "storage/common/table.h"
#include "sql/stmt/update_stmt.h"

RC UpdateOperator::open()
{
  if (children_.size() != 1) {
    LOG_WARN("delete operator must has 1 child");
    return RC::INTERNAL;
  }

  Operator *child = children_[0];
  RC rc = child->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  Table *table = update_stmt_->table();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }
    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &record = row_tuple->record();
    // rc = table->delete_record(nullptr, &record);
    // if (rc != RC::SUCCESS) {
    //   LOG_WARN("failed to update record: %s", strrc(rc));
    //   return rc;
    // }
    const TableMeta table_meta=table->table_meta();
    const FieldMeta *field_meta=table_meta.field(update_stmt_->attribute_name());
    const int field_len=field_meta->len();
    const int field_offset=field_meta->offset();

    char *data=record.data();
    memcpy(data+field_offset,update_stmt_->values().data,field_len);
    //rc = table->insert_record(nullptr, &record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }
  return RC::SUCCESS;
}

RC UpdateOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdateOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}
