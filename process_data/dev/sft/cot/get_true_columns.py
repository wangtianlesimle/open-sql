import json
import re

PREDICT_COLUMNS_PROMPT = """### Here are SQLite SQL tables that will be used, with their properties:
{table_info}
### Question: {question}
### Note that: {note}
Please generate the SQL script STEP BY STEP.
Given the tables: 
{used_tables},
From the given tables, find the required columns based on the QUESTION.
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




def get_true_columns_of_dev_data():
    dev_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev.json"
    dev_data = json.load(open(dev_file, "r"))
    table_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/table_info.json"
    table_data = json.load(open(table_file, "r"))
    f = open("/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/sft/cot/gold_columns.json", "w")
    true_columns_dev_data = {}
    for idx, data in enumerate(dev_data):
        db_name = data["db_id"]
        question = data["question"]
        note = data["evidence"]
        sql_tokens = data["SQL_toks"]

        true_columns_dev_data["id"] = idx
        true_columns_dev_data["db_id"] = db_name

        tables = table_data[db_name]["tables"]
        columns = get_all_columns(db_name, table_data)
        columns = list(set(columns))
        used_tables = separate_tables(sql_tokens, tables)
        used_columns = separate_columns(sql_tokens, columns)
        true_columns_dev_data["gold_columns"] = used_columns
        f.write(json.dumps(true_columns_dev_data, ensure_ascii=False) + "\n")




if __name__ == "__main__":
    get_true_columns_of_dev_data()


