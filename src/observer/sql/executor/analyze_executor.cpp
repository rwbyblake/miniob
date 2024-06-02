#include "analyze_executor.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/stmt/analyze_stmt.h"
#include "sql/executor/sql_result.h"
#include "sql/executor/analyze_executor.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "storage/record/record.h"
#include "storage/record/record_manager.h"
#include "storage/db/db.h"
#include <random>
#include <algorithm>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include "session/session.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/expr/tuple.h"

using namespace common;

bool column_exist(vector<string> col, string column) {
  for (string c: col) {
    if (column == c) return true;
  }
  return false;
} 

RC insert_record_from_string(
    Table *table, std::vector<std::string> &file_values, std::vector<Value> &record_values, std::stringstream &errmsg)
{

  const int field_num     = record_values.size();
  const int sys_field_num = table->table_meta().sys_field_num();

  if (file_values.size() < record_values.size()) {
    return RC::SCHEMA_FIELD_MISSING;
  }

  RC rc = RC::SUCCESS;

  std::stringstream deserialize_stream;
  for (int i = 0; i < field_num && RC::SUCCESS == rc; i++) {
    const FieldMeta *field = table->table_meta().field(i + sys_field_num);

    std::string &file_value = file_values[i];
    if (field->type() != CHARS) {
      common::strip(file_value);
    }

    switch (field->type()) {
      case INTS: {
        deserialize_stream.clear();  // 清理stream的状态，防止多次解析出现异常
        deserialize_stream.str(file_value);

        int int_value;
        deserialize_stream >> int_value;
        if (!deserialize_stream || !deserialize_stream.eof()) {
          errmsg << "need an integer but got '" << file_values[i] << "' (field index:" << i << ")";

          rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
        } else {
          record_values[i].set_int(int_value);
        }
      }

      break;
      case FLOATS: {
        deserialize_stream.clear();
        deserialize_stream.str(file_value);

        float float_value;
        deserialize_stream >> float_value;
        if (!deserialize_stream || !deserialize_stream.eof()) {
          errmsg << "need a float number but got '" << file_values[i] << "'(field index:" << i << ")";
          rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
        } else {
          record_values[i].set_float(float_value);
        }
      } break;
      case CHARS: {
        record_values[i].set_string(file_value.c_str());
      } break;
      default: {
        errmsg << "Unsupported field type to loading: " << field->type();
        rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
      } break;
    }
  }

  if (RC::SUCCESS == rc) {
    Record record;
    rc = table->make_record(field_num, record_values.data(), record);
    if (rc != RC::SUCCESS) {
      errmsg << "insert failed.";
    } else if (RC::SUCCESS != (rc = table->insert_record(record))) {
      errmsg << "insert failed.";
    }
  }
  return rc;
}

RC AnalyzeExecutor::execute(SQLStageEvent *sql_event)
{
    RC            rc            = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  SqlResult    *sql_result    = session_event->sql_result();
  AnalyzeStmt *astmt = static_cast<AnalyzeStmt *>(sql_event->stmt());
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
  // const char *table_name = astmt;
  Table *table = astmt->table();
  string origin_table_name = table->name();
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table->name());
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


  set<int> index_to_cal;
  TableMeta tablemeta = table->table_meta();
  const vector<FieldMeta> *fields = tablemeta.field_metas();
  for (int i = 0; i < (int)fields->size(); i++) {
    if (column_exist(astmt->columnName(), fields->at(i).name())) {
      index_to_cal.insert(i);
    }
  }


  int record_num = 0;
  int sampleSize = 10;
  double k_percent = 1;
  int seen = 0;
  std::default_random_engine generator; // 随机数生成器
  unordered_map<int, vector<int>> samples;
  for (int idx: index_to_cal) samples[idx] = vector<int>(0);
  while (record_scanner.has_next()) {
    record_num ++;
    rc = record_scanner.next(current_record);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    tuple.set_record(&current_record);
    // int cell_num = tuple.cell_num();
    if (seen * k_percent > sampleSize) sampleSize = int(seen * k_percent);
    for (int i: index_to_cal) {
      if (!index_to_cal.count(i)) continue;
      Value value;
      std::string column_name;
      rc = tuple.cell_at(i, value);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      rc = tuple.column_at(i, column_name);
      if (rc != RC::SUCCESS) {
        return rc;
      }

      if (samples[i].size() < sampleSize) {
            samples[i].push_back(value.get_int()); // 如果见过的记录少于sampleSize，直接添加
        } else {
            // 随机决定是否替换现有样本
            std::uniform_int_distribution<int> distribution(0, sampleSize - 1);
            int pos = distribution(generator);
            if (pos < sampleSize) {
                samples[i][pos] = value.get_int(); // 替换位置pos的样本
            }
        }
        
    }
    seen++; // 更新见过的记录数
  }
  const char *table_name = "relstatistics";
  table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table->name());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  std::stringstream errmsg;
  for (int idx: index_to_cal) {
    vector<Value> record_value(5);
    std::vector<std::pair<int, int>> histogram;
    build_histogram(samples[idx], histogram);
    string column_name = fields->at(idx).name();
    stringstream deserialize_stream;
    for (int i = 0; i < (int)histogram.size(); i++) {
      deserialize_stream << '(' << histogram[i].first << ',' << histogram[i].second << ')';
      if (i != (int)histogram.size() - 1) deserialize_stream << ',';
    }
    vector<string> record_string(5);
    record_string[0] = origin_table_name;
    record_string[1] = column_name;
    record_string[2] = to_string(histogram.size());
    record_string[3] = deserialize_stream.str();
    record_string[4] = to_string(record_num);
    insert_record_from_string(table, record_string, record_value, errmsg);
  }

  sql_result->set_return_code(RC::SUCCESS);
  sql_result->set_state_string("Histogram analysis completed successfully.");
  return RC::SUCCESS;
}



void AnalyzeExecutor::build_histogram(std::vector<int> &samples, std::vector<std::pair<int, int>> &histogram) {
    if (samples.empty()) return;

    // 排序样本数据以方便构建直方图
    std::sort(samples.begin(), samples.end());

    // 定义直方图的桶数量
    int bucket_num = 20;  // 你可以根据需要调整桶的数量

    // 计算每个桶的范围
    int min_value = samples.front();
    int max_value = samples.back();
    int range = (max_value - min_value + 1);
    int bucket_size = range / bucket_num + (range % bucket_num != 0 ? 1 : 0);

    // 初始化直方图桶
    histogram.resize(bucket_num, {1e9, 0});

    // 分配样本到桶中
    for (int value : samples) {
        int bucket_index = (value - min_value) / bucket_size;
        if (bucket_index >= bucket_num) bucket_index = bucket_num - 1; // 防止边界问题
        histogram[bucket_index].first = std::min(histogram[bucket_index].first, value);
        histogram[bucket_index].second = std::max(histogram[bucket_index].second, value);
    }
}

