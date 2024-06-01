#pragma once

#include "common/rc.h"
#include <vector>
#include <string>
#include "storage/trx/trx.h"

class SQLStageEvent;
class Table;
class SqlResult;

/**
 * @brief 数据分析的执行器
 * @ingroup Executor
 */
class AnalyzeExecutor
{
public:
    AnalyzeExecutor() = default;                     // 默认构造函数
    virtual ~AnalyzeExecutor() = default;            // 虚析构函数

    RC execute(SQLStageEvent *sql_event);            // 执行分析的公共接口

private:
    // void analyze_table(Table *table, SqlResult *sql_result); // 执行表分析
    void build_histogram(std::vector<int> &samples, std::vector<std::pair<int, int>> &histogram);
};
