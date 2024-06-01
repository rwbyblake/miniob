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
// Created by Wangyunlai on 2023/7/12.
//

#pragma once

#include <string>

#include "sql/stmt/stmt.h"

class Table;

class AnalyzeStmt : public Stmt
{
public:
  // 构造函数接收一个表指针和一个要分析的列名
  AnalyzeStmt(Table *table, const std::vector<std::string> &columnName) : table_(table), columnName_(columnName) {}
  virtual ~AnalyzeStmt() = default;

  // 实现 Stmt 类中的虚函数，返回语句类型
  StmtType type() const override { return StmtType::ANALYZE; }

  // 访问器方法，获取表对象的指针
  Table *table() const { return table_; }

  // 访问器方法，获取列名
  const std::vector<std::string> &columnName() const { return columnName_; }

  // 静态方法，用于创建 AnalyzeStmt 对象
  static RC create(Db *db, const AnalyzeSqlNode &analyze_data, Stmt *&stmt);

private:
  Table *table_ = nullptr;  // 指向分析的表对象的指针
  std::vector<std::string> columnName_;  // 要分析的列名
};

