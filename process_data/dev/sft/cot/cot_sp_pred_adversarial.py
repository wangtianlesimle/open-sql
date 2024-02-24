"""
添加对抗样本
"""
import json
import random
import re

PREDICT_TABLES_PROMPT_SCHEMA_D = """### SQLite SQL tables are requested to be represented in the following format.
TABLE_NAME (
COLUMN_NAME: DESCRIPTION   
)
FOREIGN KEYS:
TABLE_NAME1.COLUMN_NAME1=TABLE_NAME2.COLUMN_NAME2
### Here are SQLite SQL tables , with their properties:
{table_info}
### Question: {question}
### Note that: {note}
Please generate the SQL script STEP BY STEP.
Find the required tables based on the QUESTION.
"""

PREDICT_COLUMNS_PROMPT_SCHEMA_A = """### SQLite SQL tables are requested to be represented in the following format.
TABLE_NAME (
COLUMN_NAME: TYPE, (DESCRIPTION), (VALUE1, VALUE2, ...) 
)
FOREIGN KEYS:
TABLE_NAME1.COLUMN_NAME1=TABLE_NAME2.COLUMN_NAME2
### Here are SQLite SQL tables that will be used, with their properties:
{table_info}
### Question: {question}
### Note that: {note}
Please generate the SQL script STEP BY STEP.
Given the tables: 
{used_tables},
From the given tables, find the required columns based on the QUESTION.
"""

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

a = 2
table_mu = 0.5
column_mu = 0.5

def get_all_columns(db_name, table_data):
    columns = []
    for table in table_data[db_name]:
        if table == "tables" :
            continue
        columns.extend(table_data[db_name][table])
    return columns

def separate_tables(sql_tokens, tables):
    used_tables = []
    for table in tables:
        table_without_space = table.replace(" ", "")
        table_without_space = table_without_space.lower()
        for new_tok in sql_tokens:
            new_tok = new_tok.lower()
            new_tok_without_space = new_tok.replace(" ", "")
            if table_without_space == new_tok_without_space:
                used_tables.append(table)
    return used_tables

def separate_columns(sql_tokens, columns):
    used_columns = []
    # 去除别名
    for idx, tok in enumerate(sql_tokens):
        if "." in tok:
            match = re.search(r'\.(.*)', tok)
            sql_tokens[idx] = match.group(1).strip()
    for column in columns:
        column_without_space = column.replace(" ", "")
        column_without_space = column_without_space.lower()
        for new_tok in sql_tokens:
            new_tok = new_tok.lower()
            new_tok_without_space = new_tok.replace(" ", "")
            if column_without_space == new_tok_without_space:
                used_columns.append(column)
    return list(set(used_columns))

def generate_schema_D(schema):
    schema_str = ""
    foreign_keys = schema["foreign_keys"]
    for table_name, table_info in schema.items():
        if "foreign_keys" == table_name:
            continue
        schema_str += table_name + " (\n"
        for col, col_info in table_info.items():
            schema_str += col + ": " + col_info + "\n"
        schema_str.rstrip("\n")
        schema_str += ")\n"
    foreign_keys_info = "FOREIGN KEYS:\n"
    for primary_and_foreign_key in foreign_keys:
        foreign_keys_info += primary_and_foreign_key + "\n"
    schema_info_str = schema_str + foreign_keys_info.rstrip("\n")
    return schema_info_str

def get_used_tables_label(used_tables):
    tables_label = ""
    for used_table in used_tables:
        tables_label += used_table + "\n"
    return tables_label.rstrip("\n")

def generate_schema_A_based_on_used_tables(schema, used_tables):
    schema_A_str = ""
    foreign_keys = schema["foreign_keys"]
    for table_name, table_info in schema.items():
        if table_name not in used_tables or "foreign_keys" == table_name:
            continue
        schema_A_str += table_name + " (\n"
        for col, col_info in table_info.items():
            schema_A_str += col + ": " + col_info + "\n"
        schema_A_str.rstrip("\n")
        schema_A_str += ")\n"
    foreign_keys_info = "FOREIGN KEYS:\n"
    for primary_and_foreign_key in foreign_keys:
        primary_key, foreign_key = primary_and_foreign_key.split("=")
        table_of_primary_key = primary_key.split(".")[0]
        table_of_foreign_key = foreign_key.split(".")[0]
        if table_of_primary_key not in used_tables or table_of_foreign_key not in used_tables:
            continue
        else:
            foreign_keys_info += primary_and_foreign_key + "\n"
    schema_info_str = schema_A_str + foreign_keys_info.rstrip("\n")
    return schema_info_str

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

def get_all_columns_of_specify_tables_dict(specify_tables, table_column):
    table_column_dict = {}
    for specify_table in specify_tables:
        table_column_dict[specify_table] = []
        table_column_dict[specify_table].extend(table_column[specify_table])
    return table_column_dict

def get_all_columns_of_specify_tables_list(specify_tables, table_column):
    table_column_list = []
    for specify_table in specify_tables:
        table_column_list.extend(table_column[specify_table])
    return table_column_list

def dev_sp_pred_adversarial():
    schema_D_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/schema_D.json"
    schema_D_info = json.load(open(schema_D_info_file, "r"))
    schema_A_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/schema_A.json"
    schema_A_info = json.load(open(schema_A_info_file, "r"))
    dev_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev.json"
    dev_data = json.load(open(dev_file, "r"))
    table_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/table_info.json"
    table_data = json.load(open(table_file, "r"))
    f = open( "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/sft/cot/cot_sp_pred_adversarial.json", "w")


    predict_tables_dev_data = {}
    predict_columns_dev_data = {}
    predict_sql_dev_data = {}
    for idx, data in enumerate(dev_data):
        db_name = data["db_id"]
        question = data["question"]
        note = data["evidence"]
        sql_tokens = data["SQL_toks"]
        sql = data["SQL"]

        predict_tables_dev_data["id"] = idx
        predict_tables_dev_data["db_id"] = db_name
        predict_columns_dev_data["id"] = idx
        predict_columns_dev_data["db_id"] = db_name
        predict_sql_dev_data["id"] = idx
        predict_sql_dev_data["db_id"] = db_name

        tables = table_data[db_name]["tables"]
        columns = get_all_columns(db_name, table_data)
        columns = list(set(columns))
        used_tables = separate_tables(sql_tokens, tables)
        used_columns = separate_columns(sql_tokens, columns)

        # 预测表
        schema_D_predict_tables = generate_schema_D(schema_D_info[db_name])
        predict_tables_input_str = PREDICT_TABLES_PROMPT_SCHEMA_D.format(table_info=schema_D_predict_tables, note=note, question=question)
        predict_tables_dev_data["conversations"] = [
            {"from": "human",
             "value": predict_tables_input_str},
            {"from": "assistant",
             "value": get_used_tables_label(used_tables)}
        ]
        f.write(json.dumps(predict_tables_dev_data, ensure_ascii=False) + "\n")


        # 预测列
        schema_A_predict_columns = generate_schema_A_based_on_used_tables(schema_A_info[db_name], used_tables)
        predict_columns_input_str = PREDICT_COLUMNS_PROMPT_SCHEMA_A.format(table_info=schema_A_predict_columns, note=note, question=question,
                                                           used_tables=get_used_tables_label(used_tables))
        predict_columns_dev_data["conversations"] = [
            {"from": "human",
             "value": predict_columns_input_str},
            {"from": "assistant",
             "value": get_used_columns_label(used_tables, used_columns, table_data[db_name])}
        ]
        f.write(json.dumps(predict_columns_dev_data, ensure_ascii=False) + "\n")

        # 给表添加对抗样本
        adversarial_tables_list = []
        for table_adv_id in range(a):
            # 没有使用到的表
            useless_tables = list(set(tables) - set(used_tables))
            adversarial_tables_num = round(len(used_tables) * table_mu)
            if adversarial_tables_num == 0:
                break
            if len(useless_tables) <= adversarial_tables_num:
                adversarial_tables = useless_tables
            else:
                adversarial_tables = random.sample(useless_tables, adversarial_tables_num)
            adversarial_tables_list.append(adversarial_tables)

            predict_columns_adv_input_str = PREDICT_COLUMNS_PROMPT_SCHEMA_A.format(table_info=schema_A_predict_columns, note=note, question=question,
                                                               used_tables=get_used_tables_label(used_tables + adversarial_tables))
            predict_columns_dev_data["conversations"] = [
                {"from": "human",
                 "value": predict_columns_adv_input_str},
                {"from": "assistant",
                 "value": get_used_columns_label(used_tables, used_columns, table_data[db_name])}
            ]
            f.write(json.dumps(predict_columns_dev_data, ensure_ascii=False) + "\n")

        # 预测sql
        schema_A_predict_sql = generate_schema_A_based_on_used_tables_and_columns(schema_A_info[db_name], used_tables, used_columns)

        predict_sql_input_str = PREDICT_SQL_PROMPT.format(table_info=schema_A_predict_sql, note=note, question=question,
                                              used_tables_and_columns=get_used_columns_label(used_tables, used_columns,
                                                                                             table_data[db_name]))
        predict_sql_dev_data["conversations"] = [
            {"from": "human",
             "value": predict_sql_input_str},
            {"from": "assistant",
             "value": sql}
        ]
        f.write(json.dumps(predict_sql_dev_data, ensure_ascii=False) + "\n")

        # 给列添加对抗样本
        for adversarial_tables in adversarial_tables_list:
            for i in range(a):
                adv_columns = []
                columns_of_adv_tables = get_all_columns_of_specify_tables_list(used_tables + adversarial_tables, table_data[db_name])
                adversarial_columns_num = round(len(used_columns) * column_mu)
                # 先在每个扰动表中挑选一个列，确保每个表都有对应的列
                adv_columns.extend([random.choice(table_data[db_name][adversarial_table]) for adversarial_table in adversarial_tables])
                # 在可选列表中去除选择出来的
                adversarial_columns_num -= len(adv_columns)
                if adversarial_columns_num > 0:
                    columns_of_adv_tables = list(set(columns_of_adv_tables) - set(adv_columns) - set(used_columns))
                    if len(columns_of_adv_tables) <=  adversarial_columns_num:
                        adv_columns.extend(columns_of_adv_tables)
                    else:
                        adv_columns.extend(random.sample(columns_of_adv_tables, adversarial_columns_num))
                predict_sql_input_str = PREDICT_SQL_PROMPT.format(table_info=schema_A_predict_sql, note=note,
                                                                  question=question,
                                                                  used_tables_and_columns=get_used_columns_label(
                                                                      used_tables + adversarial_tables, used_columns + adv_columns,
                                                                      table_data[db_name]))

                predict_sql_dev_data["conversations"] = [
                    {"from": "human",
                     "value": predict_sql_input_str},
                    {"from": "assistant",
                     "value": sql}
                ]
                f.write(json.dumps(predict_sql_dev_data, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    dev_sp_pred_adversarial()










