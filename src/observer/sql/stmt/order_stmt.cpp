#include "order_stmt.h"
#include <unordered_map>
#include "common/log/log.h"
#include "common/lang/string.h"
#include "common/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"


RC OrderStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const std::vector<OrderSqlNode> &orders, std::vector<std::unique_ptr<Expression>> &&exprs, OrderStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt = nullptr;
  if (orders.empty()) return rc;
  OrderStmt *tmp_stmt = new OrderStmt();
  for (int i = 0; i < orders.size(); i++) {
    std::unique_ptr<OrderUnit> order_unit = nullptr;

    rc = create_order_unit(db, default_table, tables, orders[i], std::move(order_unit));
    if (rc != RC::SUCCESS) {
      delete tmp_stmt;
      LOG_WARN("failed to create order unit. condition index=%d", i);
      return rc;
    }
    tmp_stmt->order_units_.push_back(std::move(order_unit));
  }
  tmp_stmt->exprs_ = std::move(exprs);
  stmt = tmp_stmt;
  return rc;
}



RC OrderStmt::create_order_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const OrderSqlNode &order, std::unique_ptr<OrderUnit> &&order_unit)
{
  RC rc = RC::SUCCESS;



  Table           *table = nullptr;
  const FieldMeta *field = nullptr;
  rc                     = get_table_and_field(db, default_table, tables, order.attr, table, field);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot find attr");
    return rc;
  }
  order_unit = std::make_unique<OrderUnit>(std::make_unique<FieldExpr>(table, field), order.isAsc);

  return rc;
}
