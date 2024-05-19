/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "common/lang/string.h"
#include <regex>

using namespace std;

RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }

  switch (cast_type_) {
    case BOOLEANS: {
      bool val = value.get_boolean();
      cast_value.set_boolean(val);
    } break;
    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported convert from type %d to %d", child_->value_type(), cast_type_);
    }
  }
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &cell) const
{
  RC rc = child_->get_value(tuple, cell);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(cell, cell);
}

RC CastExpr::try_get_value(Value &value) const
{
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, value);
}

////////////////////////////////////////////////////////////////////////////////
static void replace_all(std::string &str, const std::string &from, const std::string &to)
{
  if (from.empty()) {
    return;
  }
  size_t pos = 0;
  while (std::string::npos != (pos = str.find(from, pos))) {
    str.replace(pos, from.length(), to);
    pos += to.length();  // in case 'to' contains 'from'
  }
}
static bool str_like(const Value &left, const Value &right)
{
  std::string raw_reg(right.data());
  replace_all(raw_reg, "_", "[^']");
  replace_all(raw_reg, "%", "[^']*");
  std::regex reg(raw_reg.c_str(), std::regex_constants::ECMAScript | std::regex_constants::icase);
  bool res = std::regex_match(left.data(), reg);
  return res;
}


ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{}

ComparisonExpr::ComparisonExpr(CompOp comp, Expression* left, Expression* right)
    : comp_(comp), left_(left), right_(right)
{}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  RC  rc         = RC::SUCCESS;
  int cmp_result = left.compare(right);
  result         = false;

  if (comp_ == LIKE_OP || comp_ == NOT_LIKE_OP) {
    ASSERT(left.is_string() && right.is_string(), "[NOT_]LIKE_OP lhs or rhs NOT STRING!");
    result = comp_ == LIKE_OP ? str_like(left, right) : !str_like(left, right);
    return rc;
  }

  switch (comp_) {
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    case IS_NULL: {
      AttrType left_type = left.attr_type();
      AttrType right_type = right.attr_type();
      result = left_type == right_type && left_type == NULLS;
    }break;
    case IS_NOT_NULL: {
      AttrType left_type = left.attr_type();
      AttrType right_type = right.attr_type();
      result = (left_type == NULLS || right_type == NULLS) && (left_type != NULLS || right_type != NULLS);
    }break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr   *left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr   *right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();
    const Value &right_cell       = right_value_expr->get_value();

    bool value = false;
    RC   rc    = compare_value(left_cell, right_cell, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    } else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;
  Value right_value;

  RC rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }

  bool bool_value = false;

  rc = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> children)
    : conjunction_type_(type), children_(std::move(children))
{}
ConjunctionExpr::ConjunctionExpr(Type type, Expression* left, Expression* right)
    : conjunction_type_(type)
{
  children_.emplace_back(left);
  children_.emplace_back(right);
}
RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == Type::AND);
  value.set_boolean(default_value);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();

  switch (arithmetic_type_) {
    case Type::ADD: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() + right_value.get_int());
      } else {
        value.set_float(left_value.get_float() + right_value.get_float());
      }
    } break;

    case Type::SUB: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() - right_value.get_int());
      } else {
        value.set_float(left_value.get_float() - right_value.get_float());
      }
    } break;

    case Type::MUL: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() * right_value.get_int());
      } else {
        value.set_float(left_value.get_float() * right_value.get_float());
      }
    } break;

    case Type::DIV: {
      if (target_type == AttrType::INTS) {
        if (right_value.get_int() == 0) {
          // NOTE:
          // 设置为整数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为整数最大值。
          value.set_int(numeric_limits<int>::max());
        } else {
          value.set_int(left_value.get_int() / right_value.get_int());
        }
      } else {
        if (right_value.get_float() > -EPSILON && right_value.get_float() < EPSILON) {
          // NOTE:
          // 设置为浮点数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为浮点数最大值。
          value.set_float(numeric_limits<float>::max());
        } else {
          value.set_float(left_value.get_float() / right_value.get_float());
        }
      }
    } break;

    case Type::NEGATIVE: {
      if (target_type == AttrType::INTS) {
        value.set_int(-left_value.get_int());
      } else {
        value.set_float(-left_value.get_float());
      }
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

// table_map 有表名检查表名(可能是别名) 没表名只能有一个 table 或者用 default table 检查列名
// table_alias_map 是为了设置 name alias 的时候用
// NOTE: 是针对 projects 中的 FieldExpr 写的 conditions 中的也可以用 但是处理之后的 name alias 是无用的
// RC FieldExpr::check_field(const std::unordered_map<std::string, Table *> &table_map,
//   const std::vector<Table *> &tables, Table* default_table,
//   const std::unordered_map<std::string, std::string> & table_alias_map)
// {
//   ASSERT(field_name_ != "*", "ERROR!");
//   const char* table_name = table_name_.c_str();
//   const char* field_name = field_name_.c_str();
//   Table * table = nullptr;
//   if(!common::is_blank(table_name)) { //表名不为空
//     // check table
//     auto iter = table_map.find(table_name);
//     if (iter == table_map.end()) {
//       LOG_WARN("no such table in from list: %s", table_name);
//       return RC::SCHEMA_FIELD_MISSING;
//     }
//     table = iter->second;
//   } else { // 表名为空，只有列名
//     if (tables.size() != 1 && default_table == nullptr) {
//       LOG_WARN("invalid. I do not know the attr's table. attr=%s", this->get_field_name().c_str());
//       return RC::SCHEMA_FIELD_MISSING;
//     }
//     table = default_table ? default_table : tables[0];
//   }
//   ASSERT(nullptr != table, "ERROR!");
//   // set table_name
//   table_name = table->name();
//   // check field
//   const FieldMeta *field_meta = table->table_meta().field(field_name);
//   if (nullptr == field_meta) {
//     LOG_WARN("no such field. field=%s.%s", table->name(), field_name);
//     return RC::SCHEMA_FIELD_MISSING;
//   }
//   // set field_
//   field_ = Field(table, field_meta);
//   // set name 没用了 暂时保留它
//   bool is_single_table = (tables.size() == 1);
//   if(is_single_table) {
//     set_name(field_name_);
//   } else {
//     set_name(table_name_ + "." + field_name_);
//   }
//   // set alias
//   if (alias().empty()) {
//     if (is_single_table) {
//       set_alias(field_name_);
//     } else {
//       auto iter = table_alias_map.find(table_name_);
//       if (iter != table_alias_map.end()) {
//         set_alias(iter->second + "." + field_name_);
//       } else {
//         set_alias(table_name_ + "." + field_name_);
//       }
//     }
//   }
//   return RC::SUCCESS;
// }

AggrFuncExpr::AggrFuncExpr(AggrFuncType type, Expression *param)
    : AggrFuncExpr(type, std::unique_ptr<Expression>(param))
{}
AggrFuncExpr::AggrFuncExpr(AggrFuncType type, unique_ptr<Expression> param)
    : type_(type), param_(std::move(param))
{
  //
  auto check_is_constexpr = [](const Expression* expr) -> RC {
    if (expr->type() == ExprType::FIELD) {
      return RC::INTERNAL;
    }
    return RC::SUCCESS;
  };
  if (RC::SUCCESS == param_->traverse_check(check_is_constexpr)) {
    param_is_constexpr_ = true;
  }
}

std::string AggrFuncExpr::get_func_name() const
{
  switch (type_) {
    case AggrFuncType::AGG_MAX:
      return "max";
    case AggrFuncType::AGG_MIN:
      return "min";
    case AggrFuncType::AGG_SUM:
      return "sum";
    case AggrFuncType::AGG_AVG:
      return "avg";
    case AggrFuncType::AGG_COUNT:
      return "count";
    default:
      break;
  }
  return "unknown_aggr_fun";
}

AttrType AggrFuncExpr::value_type() const
{
  switch (type_) {
    case AggrFuncType::AGG_MAX:
    case AggrFuncType::AGG_MIN:
    case AggrFuncType::AGG_SUM:
      return param_->value_type();
      break;
    case AggrFuncType::AGG_AVG:
      return FLOATS;
      break;
    case AggrFuncType::AGG_COUNT:
      return INTS;
      break;
    default:
      return UNDEFINED;
      break;
  }
  return UNDEFINED;
}

//Project 算子的cell_at 会调用该函数取得聚集函数最后计算的结果,传入的Tuple 就是gropuby 中的 grouptuple
RC AggrFuncExpr::get_value(const Tuple &tuple, Value &cell) const
{
  TupleCellSpec spec(name().c_str());
  //int index = 0;
  // spec.set_agg_type(get_aggr_func_type());
  // if(is_first_)
  // {
  //   bool & is_first_ref = const_cast<bool&>(is_first_);
  //   is_first_ref = false;
    
  // }
  // else
  // {
  //   return tuple.cell_at(index_, cell);
  // }
  return tuple.find_cell(spec,cell);
}
