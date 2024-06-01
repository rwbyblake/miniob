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
// Created by Longda on 2021/4/13.
//

#include <string.h>
#include <string>
#include <utility>

#include "optimize_stage.h"

#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/operator/logical_operator.h"
#include "sql/stmt/stmt.h"
#include "session/session.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "storage/record/record_manager.h"
#include "storage/trx/trx.h"
#include "sql/expr/tuple.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/join_logical_operator.h"

using namespace std;
using namespace common;
std::string toString(LogicalOperatorType type) {
  switch (type) {
    case LogicalOperatorType::CALC: return "CALC";
    case LogicalOperatorType::TABLE_GET: return "TABLE_GET";
    case LogicalOperatorType::PREDICATE: return "PREDICATE";
    case LogicalOperatorType::PROJECTION: return "PROJECTION";
    case LogicalOperatorType::JOIN: return "JOIN";
    case LogicalOperatorType::INSERT: return "INSERT";
    case LogicalOperatorType::UPDATE: return "UPDATE";
    case LogicalOperatorType::DELETE: return "DELETE";
    case LogicalOperatorType::EXPLAIN: return "EXPLAIN";
    case LogicalOperatorType::AGGR: return "AGGR";
    case LogicalOperatorType::ORDER: return "ORDER";
    default: return "Unknown Type";
  }
}

void printTree(unique_ptr<LogicalOperator> &oper, int level = 0) {
  if (!oper) return;
  std::string indent(level * 4, ' ');  // 根据层级增加缩进
  std::cout << indent << toString(oper->type()) << std::endl;
  for (unique_ptr<LogicalOperator>& child : oper->children()) {
    printTree(child, level + 1);
  }
}
RC findTableGetOper(unique_ptr<LogicalOperator> &oper, vector<unique_ptr<LogicalOperator>> &table_get_opers) {
  if (!oper) return RC::SUCCESS;

  if (oper->type() == LogicalOperatorType::TABLE_GET) {
    table_get_opers.push_back(std::move(oper));
    return RC::SUCCESS;
  }
  for (unique_ptr<LogicalOperator>& child: oper->children()) {
    findTableGetOper(child, table_get_opers);
  }
  return RC::SUCCESS;
}

RC OptimizeStage::find_histogram(const string& table_name, string column_name, analyse &ana) {
  for (auto it: analyses) {
    if (it.table_name == table_name && it.column_name == column_name) {
      ana = it;
      return RC::SUCCESS;
    }
  }
  return RC::NOTFOUND;
}

void OptimizeStage::generate_all_set(set<int> S) {
  int length = S.size();
  set<int> A;
  set<set<int>> tmp;
  tmp.insert(A);
  all_length_set.push_back(tmp);
  for (int i = 1; i <= length; i++) {
    tmp.clear();
    for (auto a: all_length_set[i - 1]) {
      for (int j = 0; j < length; j++) {
        A = a;
        if (A.count(j))
          continue;
        A.insert(j);
        tmp.insert(A);
      }
    }
    all_length_set.push_back(tmp);
  }
}


RC OptimizeStage::release_optimize_data() {
  RC rc = RC::SUCCESS;
  analyses.clear();
  min_set_scan.clear();
  rel_record_num.clear();
  merge_cost.clear();
  min_cost.clear();
  all_length_set.clear();
  ratio_map.clear();
  record_num_map.clear();
  table_get_opers.clear();
  return rc;
}

double OptimizeStage::minimal_cost(set<int> S, unique_ptr<LogicalOperator> &oper) {
//  if (dp.count(S)) return dp[S];
  min_cost[S] = 1e9;
  if (S.size() == 1) {
    int idx = *S.begin();
    TableGetLogicalOperator *table_oper = dynamic_cast<TableGetLogicalOperator*>(table_get_opers[idx].get());

    auto table_get_oper = new TableGetLogicalOperator(table_oper->table(), table_oper->fields(), true /*readonly*/);
    vector<unique_ptr<Expression>> &child_exprs = table_oper->predicates();
    vector<unique_ptr<Expression>> predicates;
    for (auto iter = child_exprs.begin(); iter != child_exprs.end(); iter++) predicates.push_back((iter->get()->deep_copy()));
    table_get_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<LogicalOperator>(std::move(table_get_oper));
    min_set_scan[S] = record_num_map[idx];
    rel_record_num[S] = max(min_set_scan[S] * ratio_map[idx], 1.0);
    merge_cost[S] = 0;
    min_cost[S] = min_set_scan[S] * (cpu_tuple_cost + cpu_operator_cost);
    return min_cost[S];
  }
  for (int i = 1; i < S.size(); i++) {
    for (set<int> A: all_length_set[i]) {
      if (includes(S.begin(), S.end(), A.begin(), A.end())) {
        set<int> S_A;
        set_difference(S.begin(), S.end(), A.begin(), A.end(),
            inserter(S_A, S_A.begin()));
        unique_ptr<LogicalOperator> table_oper_left(nullptr);
        unique_ptr<LogicalOperator> table_oper_right(nullptr);
        minimal_cost(A, table_oper_left);
        minimal_cost(S_A, table_oper_right);
        double A_scan_cost = min_set_scan[A];
        double S_A_scan_cost = min_set_scan[S_A];
        double cost = A_scan_cost + S_A_scan_cost * rel_record_num[A] +
                      merge_cost[A] + merge_cost[S_A] +
                      rel_record_num[A] * rel_record_num[S_A] * cpu_tuple_cost;
        if (min_cost[S] > cost) {
          min_set_scan[S] = A_scan_cost + S_A_scan_cost * rel_record_num[A];
          rel_record_num[S] = rel_record_num[A] * rel_record_num[S_A];
          merge_cost[S] = merge_cost[A] + merge_cost[S_A] + rel_record_num[S] * cpu_tuple_cost;
          auto *join_oper = new JoinLogicalOperator;
          join_oper->add_child(std::move(table_oper_left));
          join_oper->add_child(std::move(table_oper_right));
          oper = unique_ptr<LogicalOperator>(join_oper);
          min_cost[S] = cost;
        }
      }
    }
  }
  return min_cost[S];
}


RC OptimizeStage::optimize_join_oper(unique_ptr<LogicalOperator> &oper) {
  for (unique_ptr<LogicalOperator>& child: oper->children()) {
    if (child->type() == LogicalOperatorType::JOIN) {
//      vector<unique_ptr<LogicalOperator>> table_get_opers;
      RC rc = findTableGetOper(child, table_get_opers);
      // 计算每个table_get即扫描表算子的花费
      for (int i = 0; i < table_get_opers.size(); i++) {
        TableGetLogicalOperator *table = dynamic_cast<TableGetLogicalOperator*>(table_get_opers[i].get());

        std::vector<std::unique_ptr<Expression>> &child_exprs = table->predicates();
        double ratio = 0;
        analyse ana;
        for (auto iter = child_exprs.begin(); iter != child_exprs.end(); iter++) {
          Value value;
//          bool less_or_great = false;
          auto comparison_expr = static_cast<ComparisonExpr *>(iter->get());
//          CompOp comp = comparison_expr->comp();
          std::unique_ptr<Expression> &left_expr = comparison_expr->left();
          std::unique_ptr<Expression> &right_expr = comparison_expr->right();
          string table_name;
          string field_name;
          if (left_expr->type() == ExprType::FIELD && right_expr->type() == ExprType::VALUE) {
            auto field_expr = static_cast<FieldExpr *>(left_expr.get());
            table_name = field_expr->get_table_name();
            field_name = field_expr->get_field_name();
            right_expr->try_get_value(value);
            rc = find_histogram(table_name, field_name, ana);
            if (rc == RC::NOTFOUND) ana.record_num = -1;
          }
          ratio = ana.count_ratio(value.get_int());
          LOG_INFO("ratio for %s = %lf", table_name.c_str(), ratio);
        }
        // 计算代价
        ratio_map[i] = ratio;
        if (ana.record_num != -1)    record_num_map[i] = ana.record_num;
        else {
//          double avg = 0;
//          for (int j = 0; j < i; j++) avg += record_num_map[j];
//          avg /= i - 1;
//          record_num_map[i] = int(avg);
          record_num_map[i] = 15000;
        }
      }

      // 开始dp计算最优组合
      set<int> S;
      for (int i = 0; i < table_get_opers.size(); i++) S.insert(i);
      generate_all_set(S);
      double min_ = minimal_cost(S, child);
      LOG_INFO("min join cost = %lf", min_);
      return RC::SUCCESS;
    } else {
      return optimize_join_oper(child);
    }
  }
  return RC::EMPTY;
}

RC OptimizeStage::handle_request(SQLStageEvent *sql_event)
{
  unique_ptr<LogicalOperator> logical_operator;

  RC                          rc = create_logical_plan(sql_event, logical_operator);
  if (rc != RC::SUCCESS) {
    if (rc != RC::UNIMPLENMENT) {
      LOG_WARN("failed to create logical plan. rc=%s", strrc(rc));
    }
    return rc;
  }

  rc = rewrite(logical_operator);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to rewrite plan. rc=%s", strrc(rc));
    return rc;
  }
  printTree(logical_operator, 0);

  rc = optimize(logical_operator, sql_event);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to optimize plan. rc=%s", strrc(rc));
    return rc;
  }

  unique_ptr<PhysicalOperator> physical_operator;
  rc = generate_physical_plan(logical_operator, physical_operator);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to generate physical plan. rc=%s", strrc(rc));
    return rc;
  }

  sql_event->set_operator(std::move(physical_operator));

  return rc;
}


RC OptimizeStage::optimize(unique_ptr<LogicalOperator> &oper, SQLStageEvent *sql_event)
{
//  return RC::SUCCESS;
  RC            rc            = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  SqlResult    *sql_result    = session_event->sql_result();

  // 获取数据库
  Db *db = session_event->session()->get_current_db();
  if (nullptr == db) {
    LOG_ERROR("cannot find current db");
    rc = RC::SCHEMA_DB_NOT_EXIST;
    sql_result->set_return_code(rc);
    sql_result->set_state_string("no db selected");
    return rc;
  }
  // 获取统计表
  const char *table_name = "relstatistics";
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 生成recorde_scanner进行遍历，寻找当前表对应的
  RecordFileScanner record_scanner;
  Record            current_record;
  RowTuple          tuple;
  Trx *trx = session_event->session()->current_trx();
  trx->start_if_need();
  rc = table->get_record_scanner(record_scanner, trx, true);
  if (rc == RC::SUCCESS) {
    tuple.set_schema(table, table->table_meta().field_metas());
  }
  while (record_scanner.has_next()) {
    rc = record_scanner.next(current_record);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    tuple.set_record(&current_record);
    int cell_num = tuple.cell_num();
    string tableName;
    string columnName;
    string histogram;
    int record_num;
    string column_name;
    for (int i = 0; i < cell_num; i++) {
      Value value;
      rc = tuple.cell_at(i, value);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      rc = tuple.column_at(i, column_name);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      if (column_name == "table_name") tableName = value.get_string();
      else if(column_name == "column_name") columnName = value.get_string();
      else if(column_name == "histogtram") histogram = value.get_string();
      else if(column_name == "record_num") record_num = value.get_int();
    }
    analyses.push_back(analyse(tableName, columnName, histogram, record_num));
  }
  if (analyses.empty()) return RC::SUCCESS;
  optimize_join_oper(oper);
  printTree(oper);
  rc = release_optimize_data();
  return rc;
}


RC OptimizeStage::generate_physical_plan(
    unique_ptr<LogicalOperator> &logical_operator, unique_ptr<PhysicalOperator> &physical_operator)
{
  RC rc = RC::SUCCESS;
  rc    = physical_plan_generator_.create(*logical_operator, physical_operator);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
  }
  return rc;
}

RC OptimizeStage::rewrite(unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;

  bool change_made = false;
  do {
    change_made = false;
    rc          = rewriter_.rewrite(logical_operator, change_made);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to do expression rewrite on logical plan. rc=%s", strrc(rc));
      return rc;
    }
  } while (change_made);

  return rc;
}

RC OptimizeStage::create_logical_plan(SQLStageEvent *sql_event, unique_ptr<LogicalOperator> &logical_operator)
{
  Stmt *stmt = sql_event->stmt();
  if (nullptr == stmt) {
    return RC::UNIMPLENMENT;
  }

  return logical_plan_generator_.create(stmt, logical_operator);
}
