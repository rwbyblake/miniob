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
// Created by WangYunlai on 2022/6/27.
//

#include "common/log/log.h"
#include "sql/operator/aggr_physical_operator.h"
#include "storage/record/record.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/field/field.h"
// #include <log.h>

AggrPhysicalOperator::AggrPhysicalOperator(std::vector<std::unique_ptr<AggrFuncExpr>> &&aggr_exprs)
{
  tuple_.init(std::move(aggr_exprs));
}

RC AggrPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  if (children_.size() != 1) {
    LOG_WARN("AggrPhysicalOperator must has one child");
    return RC::INTERNAL;
  }
  if (RC::SUCCESS != (rc = children_[0]->open(trx))) {
    rc = RC::INTERNAL;
    LOG_WARN("AggrOperater child open failed!");
  }
  tuple_.reset();
  tuple_.set_tuple(children_[0] -> current_tuple());
  is_record_eof_ = false;
  is_first_ = true;
  is_new_aggr_ = true;
  return rc;
}

RC AggrPhysicalOperator::next()
{
  if (is_record_eof_) {
    return RC::RECORD_EOF;
  }
  RC rc = RC::SUCCESS;
  if (is_first_) {
    rc = children_[0]->next();
    // maybe empty. count(x) -> 0
    if (RC::SUCCESS != rc) {
      if (RC::RECORD_EOF == rc) {
        is_record_eof_ = true;
      }
      return rc;
    }
    is_first_ = false;
    is_new_aggr_ = true;
    LOG_INFO("AggrOperator set first success!");
  }

  while (true) {
    // 0. if the last row is new aggr, do aggregate first
    if (is_new_aggr_) {
      tuple_.do_aggregate_first();
      is_new_aggr_ = false;
    }
    // 3. if new aggr, should return a row
    if (is_new_aggr_) {
      tuple_.do_aggregate_done();
      return rc;
    }
    // 4. if not new aggr, execute aggregate function and update result
    tuple_.do_aggregate();

    if (RC::SUCCESS != (rc = children_[0]->next())) {
      break;
    }
  } //end while

  if (RC::RECORD_EOF == rc) {
    is_record_eof_ = true;
    tuple_.do_aggregate_done();
    return RC::SUCCESS;
  }
  return rc;
}

RC AggrPhysicalOperator::close()
{
  return children_[0]->close();
}

Tuple *AggrPhysicalOperator::current_tuple()
{
  return &tuple_;
}
