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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include <memory>
#include <vector>

#include "common/rc.h"
#include "sql/stmt/stmt.h"
#include "sql/expr/expression.h"
#include "storage/field/field.h"

class FieldMeta;
class FilterStmt;
class Db;
class Table;

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class SelectStmt : public Stmt
{
public:
  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override { return StmtType::SELECT; }

public:
  static RC create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt);

public:
  const std::vector<Table *> &tables() const { return tables_; }
  const std::vector<Field>   &query_fields() const { return query_fields_; }
  FilterStmt                 *filter_stmt() const { return filter_stmt_; }

  std::vector<std::unique_ptr<AggrFuncExpr>> &get_aggr_exprs()
  {
    return aggr_exprs_;
  }

  void set_aggr_exprs(std::vector<std::unique_ptr<AggrFuncExpr>> &&aggr_exprs)
  {
    aggr_exprs_ = std::move(aggr_exprs);
  }

private:
  std::vector<Field>   query_fields_;
  std::vector<Table *> tables_;
  FilterStmt          *filter_stmt_ = nullptr;
  std::vector<std::unique_ptr<AggrFuncExpr>> aggr_exprs_; // 聚集函数表达式
};
