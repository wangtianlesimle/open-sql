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
Given the tables and columns: 
{used_tables_and_columns},
### Complete sqlite SQL query based on the given tables and columns
SELECT
"""



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

def generate_schema_A_based_on_used_tables_and_columns(schema, used_tables, used_columns):
    schema_A_str = ""
    foreign_keys = schema["foreign_keys"]
    for table_name, table_info in schema.items():
        if table_name not in used_tables or "foreign_keys" == table_name:
            continue
        schema_A_str += table_name + " (\n"
        for col, col_info in table_info.items():
            if col not in used_columns:
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
        if table_of_primary_key not in used_tables or table_of_foreign_key not in used_tables or \
            column_of_primary_key not in used_columns or column_of_foreign_key not in used_columns:
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

def get_predict_sql_of_train_data_based_on_true_label():
    schema_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/schema/schema_A.json"
    schema_info = json.load(open(schema_info_file, "r"))
    train_file = "/public14_data/wtl/text2sql/datasets/bird/train/train.json"
    train_data = json.load(open(train_file, "r"))
    table_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/schema/table_info.json"
    table_data = json.load(open(table_file, "r"))
    f = open("/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/sft/cot/train_cot_predict_sql_sp_pred_schema_A.json", "w")
    predict_sql_train_data = {}
    for idx, data in enumerate(train_data):
        db_name = data["db_id"]
        question = data["question"]
        note = data["evidence"]
        sql_tokens = data["SQL_toks"]
        sql = data["SQL"]
        predict_sql_train_data["id"] = idx
        predict_sql_train_data["db_id"] = db_name

        tables = table_data[db_name]["tables"]
        columns = get_all_columns(db_name, table_data)
        columns = list(set(columns))
        used_tables = separate_tables(sql_tokens, tables)
        used_columns = separate_columns(sql_tokens, columns)

        schema_A = generate_schema_A_based_on_used_tables_and_columns(schema_info[db_name], used_tables, used_columns)

        input_str = PREDICT_SQL_PROMPT.format(table_info=schema_A, note=note, question=question,
                                              used_tables_and_columns=get_used_columns_label(used_tables, used_columns, table_data[db_name]))
        predict_sql_train_data["conversations"] = [
            {"from": "human",
             "value": input_str},
            {"from": "assistant",
             "value": sql}
        ]
        f.write(json.dumps(predict_sql_train_data, ensure_ascii=False) + "\n")



if __name__ == "__main__":
    get_predict_sql_of_train_data_based_on_true_label()


