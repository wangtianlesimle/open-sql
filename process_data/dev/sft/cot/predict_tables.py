import json
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


def get_all_columns(db_name, table_data):
    columns = []
    for table in table_data[db_name]:
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

def get_predict_tables_of_dev_data():
    schema_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/schema_D.json"
    schema_info = json.load(open(schema_info_file, "r"))
    dev_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev.json"
    dev_data = json.load(open(dev_file, "r"))
    table_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/table_info.json"
    table_data = json.load(open(table_file, "r"))
    f = open(
        "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/sft/cot/dev_cot_predict_tables_schema_D.json",
        "w")
    predict_tables_dev_data = {}
    for idx, data in enumerate(dev_data):
        db_name = data["db_id"]
        question = data["question"]
        note = data["evidence"]
        sql_tokens = data["SQL_toks"]

        predict_tables_dev_data["id"] = idx
        predict_tables_dev_data["db_id"] = db_name

        tables = table_data[db_name]["tables"]
        columns = get_all_columns(db_name, table_data)
        columns = list(set(columns))
        used_tables = separate_tables(sql_tokens, tables)
        used_columns = separate_columns(sql_tokens, columns)
        # if get_used_tables_label(used_tables) == "":
        #     continue
        schema_D = generate_schema_D(schema_info[db_name])

        input_str = PREDICT_TABLES_PROMPT_SCHEMA_D.format(table_info=schema_D, note=note, question=question)
        predict_tables_dev_data["conversations"] = [
            {"from": "human",
             "value": input_str},
            {"from": "assistant",
             "value": get_used_tables_label(used_tables)}
        ]
        f.write(json.dumps(predict_tables_dev_data, ensure_ascii=False) + "\n")




if __name__ == "__main__":
    get_predict_tables_of_dev_data()








