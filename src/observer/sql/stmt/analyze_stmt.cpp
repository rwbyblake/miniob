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
#include "sql/stmt/analyze_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include <errno.h>
#include <string.h>
#include <vector>

using namespace common;

RC AnalyzeStmt::create(Db *db, const AnalyzeSqlNode &analyze_data, Stmt *&stmt)
{
  RC rc = RC::SUCCESS;

  const char *table_name = analyze_data.table_name.c_str();
  const std::vector<std::string> column_names = analyze_data.column_names;

  if (is_blank(table_name)) {
    LOG_WARN("invalid argument. db=%p, table_name=%p",
             db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // Check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // // Check if the column exists in the table
  // if (!(table->has_column(column_name))) {
  //   LOG_WARN("no such column. table_name=%s, column_name=%s", table_name, column_name);
  //   return RC::SCHEMA_FIELD_NOT_EXIST;
  // }

  stmt = new AnalyzeStmt(table, column_names);
  return rc;
}
