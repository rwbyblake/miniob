#pragma once

#include "sql/expr/expression.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"
#include <unordered_map>
#include <vector>

class Db;
class Table;

// 排序单元
class OrderUnit {
public:
  OrderUnit(std::unique_ptr<Expression> expr , bool is_asc):is_asc_(is_asc),expr_(std::move(expr))
  {}

  ~OrderUnit() = default;

  void set_sort_type(bool sort_type)
  {
    is_asc_ = sort_type;
  }

  bool sort_type() const
  {
    return is_asc_;
  }

  std::unique_ptr<Expression>& expr()
  {
    return expr_;
  }

private:
  // sort type : true is asc
  bool is_asc_ = true;
  std::unique_ptr<Expression> expr_;
};

/**
 * @brief Order/排序语句
 * @ingroup Statement
 */
class OrderStmt
{
public:
  OrderStmt() = default;
  ~OrderStmt() = default;

public:
  std::vector<std::unique_ptr<OrderUnit>> &order_units(){ return order_units_; }
  std::vector<std::unique_ptr<Expression>>& get_exprs() { return exprs_; }

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const std::vector<OrderSqlNode> &orders, std::vector<std::unique_ptr<Expression>> &&exprs, OrderStmt *&stmt);

  static RC create_order_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      const OrderSqlNode &order, std::unique_ptr<OrderUnit> &&order_unit);


private:
  std::vector<std::unique_ptr<OrderUnit>> order_units_; // 存储所有的排序单元
  std::vector<std::unique_ptr<Expression>> exprs_; //存储所有的字段表达式
};
