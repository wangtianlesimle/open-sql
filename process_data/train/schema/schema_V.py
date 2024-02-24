import json
import os
import re
import sqlite3

import pandas as pd



"""
schema_V: 加上了列对应的枚举值
"""


def get_foreign_keys():
    train_tables_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/train_tables.json"
    train_tables_datas = json.load(open(train_tables_file, "r"))
    foreign_keys = {}
    for tables_info in train_tables_datas:
        db_name = tables_info["db_id"]
        tables = tables_info["table_names_original"]
        columns = tables_info["column_names_original"]
        foreign_keys_of_table = tables_info["foreign_keys"]
        foreign_keys_list = []
        for foreign_key in foreign_keys_of_table:
            column_of_primary_key = columns[foreign_key[1]][1]
            table_of_primary_key = tables[columns[foreign_key[1]][0]]
            column_of_foreign_key = columns[foreign_key[0]][1]
            table_of_foreign_key = tables[columns[foreign_key[0]][0]]
            if " " in column_of_primary_key:
                column_of_primary_key = "`" + column_of_primary_key + "`"
            if " " in column_of_foreign_key:
                column_of_foreign_key = "`" + column_of_foreign_key + "`"
            foreign_keys_list.append(table_of_primary_key + "." + column_of_primary_key + "=" + table_of_foreign_key + "." +
                                     column_of_foreign_key)
        foreign_keys[db_name] = {}
        foreign_keys[db_name]["foreign_keys"] = foreign_keys_list
    return foreign_keys


def generate_schema_V():
    databases_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/train_databases"
    database_names = [f for f in os.listdir(databases_file) if os.path.isdir(os.path.join(databases_file, f))]
    schema_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/schema/schema_V.json"
    columns_enumeration_values_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/schema/columns_enumeration_values.json"
    columns_enumeration_values_data = json.load(open(columns_enumeration_values_file, "r"))
    schema_data = {}
    primary_and_foreign_keys = get_foreign_keys()
    for idx, database_name in enumerate(database_names):
        # print(f"database_name:{idx}")
        schema_data[database_name] = {}
        # 获取数据库对应的文件路径
        database_file = databases_file + "/" + database_name + "/" + database_name + ".sqlite"
        # 连接数据库
        conn = sqlite3.connect(database_file)
        # 创建一个游标对象
        cursor = conn.cursor()
        # 查询 sqlite_master 表以获取数据库中所有表的定义语句
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table';")
        # 获取查询结果
        tables = cursor.fetchall()
        # 获取主外键信息
        foreign_infos = primary_and_foreign_keys[database_name]["foreign_keys"]
        # 遍历每个表，查询其列信息
        for t_idx, table in enumerate(tables):
            # print(f"\ttable:{t_idx}")
            table_name = table[0]
            if table_name == "sqlite_sequence":
                continue
            schema_data[database_name][table_name] = {}
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            # 获取查询结果
            columns = cursor.fetchall()
            for c_idx, column in enumerate(columns):
                column_name = column[1]

                columns_enumeration_values_info = columns_enumeration_values_data[database_name][table_name][column_name]

                schema_data[database_name][table_name][column_name] =  columns_enumeration_values_info
        schema_data[database_name]["foreign_keys"] = foreign_infos
        # 关闭数据库连接
        conn.close()
    with open(schema_file, 'w') as json_file:
        json.dump(schema_data, json_file, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    generate_schema_V()