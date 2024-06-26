#pragma once

#ifndef MINIOB_SQL_OPERATOR_UPDATE_LOGICAL_OPERATOR_H
#define MINIOB_SQL_OPERATOR_UPDATE_LOGICAL_OPERATOR_H

#include "sql/operator/logical_operator.h"
// #include "sql/parser/parse_defs.h"
#include <unordered_map>
#include <cstring>
/**
 * @brief 逻辑算子，用于执行delete语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator {

public:
    UpdateLogicalOperator(Table *table, Value value, const char *field_name);

    ~UpdateLogicalOperator() override;

    LogicalOperatorType type() const override {
        return LogicalOperatorType::UPDATE;
    }

    inline Table * table() const {
        return table_;
    }

    inline Value& value() {
        return value_;
    }

    inline char * field_name() const {
        return field_name_;
    }
    // const Value* find_value_by_field(const FieldMeta& field_meta) {
    //     return &update_map_->at(field_meta);
    // }

private:
    Table * table_ = nullptr;
    // std::unordered_map<FieldMeta, Value> *update_map_ = nullptr; // TODO 多字段更新
    Value value_;
    char * field_name_ = nullptr;
};

#endif