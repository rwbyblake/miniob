/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <memory>
#include "sql/operator/physical_operator.h"
#include "sql/expr/expression.h"

/**
 * @brief 物理算子
 * @ingroup PhysicalOperator
 */
class AggrPhysicalOperator : public PhysicalOperator
{
public:
  explicit AggrPhysicalOperator(std::vector<std::unique_ptr<AggrFuncExpr>> &&aggr_exprs);

  ~AggrPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::AGGR;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  bool is_first_ = true;
  bool is_new_aggr_ = true;
  bool is_record_eof_ = false;
  std::vector<Value> pre_values_;  // its size equal to aggr_units.size

  AggrTuple tuple_;
};
