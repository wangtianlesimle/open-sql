import random

import torch
import sys
from transformers import  LlamaTokenizer
import json, os
import re


PREDICT_SQL_PROMPT = """### Here are SQLite SQL tables that will be used, with their properties:
{table_info}
### Question: {question}
### Note that: {note}
Please generate the SQL script STEP BY STEP.
Given the tables and columns used in the SQL query: 
{used_tables_and_columns},
### Complete sqlite SQL query based on the given tables and columns
SELECT
"""

if torch.cuda.is_available():
    device = torch.device(0)
else:
    device = torch.device('cpu')
model_name_or_path = "/public14_data/wtl/model/CodeLlama-7b-hf/"
tokenizer = LlamaTokenizer.from_pretrained(model_name_or_path)
space_id = tokenizer.encode("\n", add_special_tokens=False)[0]
space_tensor = torch.LongTensor([[space_id]]).to(device)


def get_all_columns(db_name, table_data):
    columns = []
    for table in table_data[db_name]:
        if table == "tables":
            continue
        columns.extend(table_data[db_name][table])
    return columns

def separate_tables(sql_tokens, tables):
    used_tables = set()
    for table in tables:
        table_without_space = table.replace(" ", "")
        table_without_space = table_without_space.lower()
        for new_tok in sql_tokens:
            new_tok = new_tok.lower()
            new_tok_without_space = new_tok.replace(" ", "")
            if table_without_space == new_tok_without_space:
                used_tables.add(table)
    return list(used_tables)

def separate_columns(sql_tokens, columns):
    used_columns = set()
    sql_tokens_without_alias = []
    # 去除别名
    for idx, tok in enumerate(sql_tokens):
        if "." in tok:
            match =  re.search(r'\.(.*)', tok)
            sql_tokens_without_alias.append(match.group(1).strip())
        else:
            sql_tokens_without_alias.append(tok)
    for column in columns:
        column_without_space = column.replace(" ", "")
        column_without_space = column_without_space.lower()
        for tok in sql_tokens_without_alias:
            tok = tok.lower()
            tok = tok.replace(" ", "")
            tok = tok.strip("`")
            if column_without_space == tok:
                used_columns.add(column)
    return list(used_columns)

def generate_deleted_schema_A(schema, skip_tables=None, skip_columns=None):
    if skip_columns is None:
        skip_columns = []
    if skip_tables is None:
        skip_tables = []
    schema_A_str = ""
    foreign_keys = schema["foreign_keys"]
    for table_name, table_info in schema.items():
        if table_name == "foreign_keys" or table_name in skip_tables:
            continue
        schema_A_str += table_name + " (\n"
        for col, col_info in table_info.items():
            if col in skip_columns:
                continue
            schema_A_str += col + ": " + col_info + "\n"
        schema_A_str.rstrip("\n")
        schema_A_str += ")\n"
    foreign_keys_info = "FOREIGN KEYS:\n"
    for primary_and_foreign_key in foreign_keys:
        primary_key, foreign_key = primary_and_foreign_key.split("=")
        table_of_primary_key = primary_key.split(".")[0]
        table_of_foreign_key = foreign_key.split(".")[0]
        column_of_primary_key = primary_key.split(".")[1]
        column_of_foreign_key = foreign_key.split(".")[1]
        if table_of_primary_key  in skip_tables or table_of_foreign_key  in skip_tables or \
            column_of_primary_key  in skip_columns or column_of_foreign_key  in skip_columns:
            continue
        else:
            foreign_keys_info += primary_and_foreign_key + "\n"
    schema_info_str = schema_A_str + foreign_keys_info.rstrip("\n")
    return schema_info_str

def get_used_tables_label(used_tables):
    tables_label = ""
    for used_table in used_tables:
        tables_label += used_table + "\n"
    return tables_label.rstrip("\n")

def get_used_columns_label(used_tables, used_columns, table_column):
    columns_label = ""
    for used_table in used_tables:
        columns_label += used_table + " (\n"
        for used_column in used_columns:
            if used_column in table_column[used_table]:
                columns_label += used_column + "\n"
        columns_label = columns_label.rstrip("\n")
        columns_label += "\n)\n"
    return columns_label.rstrip("\n")

def count_tokens(inputs):
    return len(tokenizer.encode(inputs, add_special_tokens=False))

def is_primary_or_foreign_key(table_info, table, column):
    primary_keys = table_info["primary_keys"]
    foreign_keys = table_info["foreign_keys"]
    # 找到正在删除的表对应的索引
    index_of_table = table_info["table_names_original"].index(table)
    index_of_column = -1
    for index, column_index in enumerate(table_info["column_names_original"]):
        if column == column_index[1] and index_of_table == column_index[0]:
            index_of_column = index
            break
    assert index_of_column > -1
    for primary_key in primary_keys:
        if type(primary_key) == list:
            if index_of_column in primary_key:
                return True
        elif primary_key == index_of_column:
            return True
    for foreign_key in foreign_keys:
        if index_of_column in foreign_key:
            return True
    return False

def get_predict_sql_full_of_dev_data_based_on_true_label():
    schema_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/schema_A.json"
    schema_info = json.load(open(schema_info_file, "r"))
    dev_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev.json"
    dev_data = json.load(open(dev_file, "r"))
    table_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/table_info.json"
    table_data = json.load(open(table_file, "r"))
    dev_tables_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev_tables.json"
    dev_tables_info_data = json.load(open(dev_tables_info_file, "r"))
    f = open("/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/sft/cot/dev_cot_predict_sql_full_schema_A_based_on_true_label.json", "w")
    predict_sql_dev_data = {}
    for idx, data in enumerate(dev_data):
        print(idx)
        db_name = data["db_id"]
        question = data["question"]
        note = data["evidence"]
        sql_tokens = data["SQL_toks"]
        sql = data["SQL"]
        predict_sql_dev_data["id"] = idx
        predict_sql_dev_data["db_id"] = db_name

        tables = table_data[db_name]["tables"]
        columns = get_all_columns(db_name, table_data)
        columns = list(set(columns))
        used_tables = separate_tables(sql_tokens, tables)
        used_columns = separate_columns(sql_tokens, columns)
        can_delete_tables = list(set(tables) - set(used_tables))
        can_delete_columns_list = list(set(columns) - set(used_columns))
        columns_of_tables = {}
        for table_name, table_info in table_data[db_name].items():
            if table_name == "tables":
                continue
            columns_of_tables[table_name] = table_info.copy()
        table_info = {}
        for table_info_data in dev_tables_info_data:
            if db_name == table_info_data["db_id"]:
                table_info = table_info_data.copy()
                break

        schema_A = generate_deleted_schema_A(schema_info[db_name])
        input_str = PREDICT_SQL_PROMPT.format(table_info=schema_A, note=note, question=question,
                                              used_tables_and_columns=get_used_columns_label(used_tables, used_columns, table_data[db_name]))
        all_tokens_len = count_tokens(input_str)
        delete_tables = []
        delete_columns = []
        while all_tokens_len >= 2048:
            # 先删除不需要使用到的表中的列
            if len(can_delete_tables) > 0:
                # 本次需要删除的列名
                this_delete_column = ""
                # 本次需要删除的列对应的表名
                delete_columns_of_table = can_delete_tables[0]
                # 正在删除的列对应的表名
                deleting_table = delete_columns_of_table
                this_can_delete_columns_list = columns_of_tables[delete_columns_of_table]
                # 遍历可以删除的列，先删除不是主外键的列
                for column in this_can_delete_columns_list:
                    if is_primary_or_foreign_key(table_info, delete_columns_of_table, column):
                        continue
                    else:
                        this_delete_column = column
                        delete_columns.append(column)
                        columns_of_tables[delete_columns_of_table].remove(column)
                        # 如果只剩下一个列，删除之后表就不存在了，也需要删除
                        if len(this_can_delete_columns_list) <= 1:
                            delete_tables.append(delete_columns_of_table)
                            can_delete_tables.remove(delete_columns_of_table)
                            del columns_of_tables[delete_columns_of_table]
                        break
                # 如果只剩主外键，则先删除整个表
                if this_delete_column == "":
                    delete_tables.append(delete_columns_of_table)
                    delete_columns.extend(this_can_delete_columns_list)
                    del columns_of_tables[delete_columns_of_table]
                    can_delete_tables.remove(delete_columns_of_table)
            else:
                delete_tables_index = random.sample(range(len(used_tables)), 1)[0]
                delete_columns_of_table = used_tables[delete_tables_index]
                deleting_table = delete_columns_of_table
                this_can_delete_columns_list = columns_of_tables[delete_columns_of_table]
                for column in this_can_delete_columns_list:
                    if is_primary_or_foreign_key(table_info, delete_columns_of_table, column):
                        continue
                    else:
                        delete_columns.append(column)
                        columns_of_tables[delete_columns_of_table].remove(column)
                        break
            schema_A = generate_deleted_schema_A(schema_info[db_name], skip_tables=delete_tables, skip_columns=delete_columns)
            input_str = PREDICT_SQL_PROMPT.format(table_info=schema_A, note=note, question=question,
                                                  used_tables_and_columns=get_used_columns_label(used_tables,
                                                                                                 used_columns,
                                                                                                 table_data[db_name]))
            all_tokens_len = count_tokens(input_str)
        predict_sql_dev_data["conversations"] = [
            {"from": "human",
             "value": input_str},
            {"from": "assistant",
             "value": sql}
        ]
        f.write(json.dumps(predict_sql_dev_data, ensure_ascii=False) + "\n")

def get_predict_columns(predict_columns_str):
    predict_tables, predict_columns = [], []
    predict_list = predict_columns_str.split("\n")
    for predict in predict_list:
        if predict.strip().endswith("("):
            predict_tables.append(predict.strip(" )"))
        elif predict.strip() == ")":
            continue
        else:
            predict_columns.append(predict.strip())
    return  list(set(predict_columns))

def get_predict_sql_full_of_dev_data_based_on_pred_label():
    schema_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/schema_A.json"
    schema_info = json.load(open(schema_info_file, "r"))
    dev_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev.json"
    dev_data = json.load(open(dev_file, "r"))
    table_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/table_info.json"
    table_data = json.load(open(table_file, "r"))
    dev_tables_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev_tables.json"
    dev_tables_info_data = json.load(open(dev_tables_info_file, "r"))
    predict_tables_file = "/public14_data/wtl/text2sql/open_sql_v2/evaluation/codellama/sft/cot/result/result_cot_predict_tables_schema_D.json"
    predict_tables_data = json.load(open(predict_tables_file, "r"))
    predict_columns_file = "/public14_data/wtl/text2sql/open_sql_v2/evaluation/codellama/sft/cot/result/result_cot_predict_columns_pred_schema_A_based_on_predict_tables_schema_D.json"
    predict_columns_data = json.load(open(predict_columns_file, "r"))
    f = open("/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/sft/cot/dev_cot_predict_sql_full_schema_A_based_on_predict_label.json", "w")
    predict_sql_dev_data = {}
    for idx, data in enumerate(dev_data):
        print(idx)
        db_name = data["db_id"]
        question = data["question"]
        note = data["evidence"]
        sql_tokens = data["SQL_toks"]
        sql = data["SQL"]
        predict_sql_dev_data["id"] = idx
        predict_sql_dev_data["db_id"] = db_name

        tables = table_data[db_name]["tables"]
        columns = get_all_columns(db_name, table_data)
        columns = list(set(columns))
        # used_tables = separate_tables(sql_tokens, tables)
        # used_columns = separate_columns(sql_tokens, columns)
        predict_tables_str = predict_tables_data[idx]["predict_tables"]
        predict_columns_str = predict_columns_data[idx]["predict_columns"]
        predict_tables = [predict_table for predict_table in predict_tables_str.split("\n")]
        predict_columns = get_predict_columns(predict_columns_str)
        can_delete_tables = list(set(tables) - set(predict_tables))
        can_delete_columns_list = list(set(columns) - set(predict_columns))
        columns_of_tables = {}
        for table_name, table_info in table_data[db_name].items():
            if table_name == "tables":
                continue
            columns_of_tables[table_name] = table_info.copy()
        table_info = {}
        for table_info_data in dev_tables_info_data:
            if db_name == table_info_data["db_id"]:
                table_info = table_info_data.copy()
                break

        schema_A = generate_deleted_schema_A(schema_info[db_name])
        input_str = PREDICT_SQL_PROMPT.format(table_info=schema_A, note=note, question=question,
                                              used_tables_and_columns=get_used_columns_label(predict_tables, predict_columns, table_data[db_name]))
        all_tokens_len = count_tokens(input_str)
        delete_tables = []
        delete_columns = []
        while all_tokens_len >= 2048:
            # 先删除不需要使用到的表中的列
            if len(can_delete_tables) > 0:
                # 本次需要删除的列名
                this_delete_column = ""
                # 本次需要删除的列对应的表名
                delete_columns_of_table = can_delete_tables[0]
                # 正在删除的列对应的表名
                deleting_table = delete_columns_of_table
                this_can_delete_columns_list = columns_of_tables[delete_columns_of_table]
                # 遍历可以删除的列，先删除不是主外键的列
                for column in this_can_delete_columns_list:
                    if is_primary_or_foreign_key(table_info, delete_columns_of_table, column):
                        continue
                    else:
                        this_delete_column = column
                        delete_columns.append(column)
                        columns_of_tables[delete_columns_of_table].remove(column)
                        # 如果只剩下一个列，删除之后表就不存在了，也需要删除
                        if len(this_can_delete_columns_list) <= 1:
                            delete_tables.append(delete_columns_of_table)
                            can_delete_tables.remove(delete_columns_of_table)
                            del columns_of_tables[delete_columns_of_table]
                        break
                # 如果只剩主外键，则先删除整个表
                if this_delete_column == "":
                    delete_tables.append(delete_columns_of_table)
                    delete_columns.extend(this_can_delete_columns_list)
                    del columns_of_tables[delete_columns_of_table]
                    can_delete_tables.remove(delete_columns_of_table)
            else:
                delete_tables_index = random.sample(range(len(predict_tables)), 1)[0]
                delete_columns_of_table = predict_tables[delete_tables_index]
                deleting_table = delete_columns_of_table
                this_can_delete_columns_list = columns_of_tables[delete_columns_of_table]
                for column in this_can_delete_columns_list:
                    if is_primary_or_foreign_key(table_info, delete_columns_of_table, column):
                        continue
                    else:
                        delete_columns.append(column)
                        columns_of_tables[delete_columns_of_table].remove(column)
                        break
            schema_A = generate_deleted_schema_A(schema_info[db_name], skip_tables=delete_tables, skip_columns=delete_columns)
            input_str = PREDICT_SQL_PROMPT.format(table_info=schema_A, note=note, question=question,
                                                  used_tables_and_columns=get_used_columns_label(predict_tables,
                                                                                                 predict_columns,
                                                                                                 table_data[db_name]))
            all_tokens_len = count_tokens(input_str)
        predict_sql_dev_data["conversations"] = [
            {"from": "human",
             "value": input_str},
            {"from": "assistant",
             "value": sql}
        ]
        f.write(json.dumps(predict_sql_dev_data, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    get_predict_sql_full_of_dev_data_based_on_true_label()


