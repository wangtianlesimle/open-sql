import random

import torch
import json
import sys
from transformers import LlamaForCausalLM, LlamaTokenizer, AutoTokenizer, AutoModelForCausalLM, AutoConfig, AutoModel
import argparse
from tqdm import tqdm
import json, os
from datasets import load_dataset
from torch.utils.data import DataLoader
import copy
import pdb
import logging
import re

from peft import PeftModel

IGNORE_INDEX = -100

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('--model_name_or_path', type=str, required=True)
parser.add_argument('--ckpt_path', type=str)
parser.add_argument('--log_file', type=str, required=True)
parser.add_argument('--result_file', type=str, required=True)
parser.add_argument('--use_lora', action="store_true")
parser.add_argument('--llama', action="store_true")
parser.add_argument('--dev_data_path', type=str, required=True)
args = parser.parse_args()

max_new_tokens = 2048
generation_config = dict(
    bos_token_id=1,
    eos_token_id=2,
    pad_token_id=0,
    temperature=0.001,
    top_k=30,
    top_p=0.85,
    do_sample=True,
    repetition_penalty=1.1,
    max_new_tokens=max_new_tokens
)


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

PREDICT_SQL_SKELETON_PROMPT_SCHEMA_A = """### SQLite SQL tables are requested to be represented in the following format.
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
Given the tables and columns: 
{used_tables_and_columns},
Based on the given the tables and columns, write the skeleton of the SQL query corresponding to the question.
"""

PREDICT_SQL_SK_PRED_PROMPT = """### SQLite SQL tables are requested to be represented in the following format.
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
Given the tables and columns: 
{used_tables_and_columns},
and sql skeleton: 
{sql_skeleton}, 
### Complete sqlite SQL query based on the given tables、 columns and sql_skeleton
SELECT
"""


def get_questions(val_data):
    questions = []
    answers = []
    db_ides = []
    for data in val_data:
        db_ides.append(data['db_id'])
        human = data['conversations'][0]
        assistant = data['conversations'][1]
        input = human["value"]
        sentence_ids = tokenizer.encode(input, add_special_tokens=False)
        questions.append(sentence_ids)
        output = assistant["value"]
        answers.append(output)

    return db_ides, questions, answers


def print_rank_0(msg, log_file, rank=0):
    if rank <= 0:
        with open(log_file, 'a') as f:
            print(msg)
            f.write(msg + '\n')
            f.close()


def write_cov(sql_dict, file):
    with open(file, encoding="utf-8", mode='a') as f:
        f.write(json.dumps(sql_dict, ensure_ascii=False) + "\n")
        f.close()

def codellama_generate(input):
    inputs = tokenizer.encode(input, add_special_tokens=False)
    inputs = torch.LongTensor(inputs).unsqueeze(0).to(device)
    generation_output = model.generate(input_ids=inputs, **generation_config)[0]
    input_len = inputs.size()[1]
    gen_len = len(generation_output) - input_len
    model_output = generation_output[-gen_len:]
    ans_text = tokenizer.decode(model_output, skip_special_tokens=True)
    return  ans_text

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

def get_used_tables_label(used_tables):
    tables_label = ""
    for used_table in used_tables:
        tables_label += used_table + "\n"
    return tables_label.rstrip("\n")

def generate_schema_A(schema, used_tables):
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

def get_used_columns_label(used_tables, used_columns, table_column):
    columns_label = ""
    for used_table in used_tables:
        if used_table not in table_column:
            continue
        columns_label += used_table + " (\n"
        for used_column in used_columns:
            if used_column in table_column[used_table]:
                columns_label += used_column + "\n"
        columns_label = columns_label.rstrip("\n")
        columns_label += "\n)\n"
    return columns_label.rstrip("\n")

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

def get_all_columns(db_name, table_data):
    columns = []
    for table in table_data[db_name]:
        if table == "tables":
            continue
        columns.extend(table_data[db_name][table])
    return columns

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

def get_sql_skeleton(sql_tokens, used_tables, used_columns):
    sql_keys = ["select","from","join","on","where","group","by","order","limit","desc","asc","having","union","except","intersect"]
    comparison_operators = ["=",">=",">","<","<=", "||", "!=", "!"]
    operator = ["+", "-", "*", "/"]
    sql_skeletons = ""
    is_add = True
    used_columns = [used_column.replace(" ", "").lower() for used_column in used_columns]
    used_tables = [used_table.replace(" ", "").lower() for used_table in used_tables]
    for id, tok in enumerate(sql_tokens):
        if len(tok) == 0:
            continue
        if tok in operator:
            continue
        if tok.lower() == "t":
            continue
        # if tok == "AND" or tok == "," or tok == "OR":
        #     continue
        if bool(re.match(r'^T\d\.', tok, re.IGNORECASE)):
            if is_add:
                sql_skeletons += "_ "
                is_add = False
            continue
        if bool(re.match(r'^[-+]?(\d+(\.\d*)?|\.\d+)$', tok)):
            if is_add:
                sql_skeletons += "_ "
                is_add = False
            continue
        if bool(re.match(r'^T\.', tok, re.IGNORECASE)):
            if is_add:
                sql_skeletons += "_ "
                is_add = False
            continue
        if tok == ",":
            continue
        if tok == "AS" and  bool(re.match(r'^T\d$', sql_tokens[id + 1],re.IGNORECASE)):
            if is_add:
                sql_skeletons += "_ "
                is_add = False
            continue
        if id > 0:
            if sql_tokens[id - 1] == "AS":
                if is_add:
                    sql_skeletons += "_ "
                    is_add = False
                continue
        tok_without_space = tok.replace(" ", "").lower().strip("`")
        if tok_without_space not in used_tables and \
                tok_without_space not in used_columns and \
                (tok[0] != "'" and tok[len(tok) - 1] != "'") and \
                tok not in comparison_operators and \
                not bool(re.match(r'^[0-9]+$', tok)) and \
                not bool(re.match(r'^T\d$', tok,re.IGNORECASE)):
            sql_skeletons += tok + " "
            is_add = True
        else:
            if is_add:
                sql_skeletons += "_ "
                is_add = False
    return sql_skeletons.strip(" ")

if __name__ == '__main__':
    load_type = torch.float16
    if torch.cuda.is_available():
        device = torch.device(0)
    else:
        device = torch.device('cpu')

    log_file = args.log_file
    result_file = args.result_file

    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info("Start inference , loading the model")

    tokenizer = LlamaTokenizer.from_pretrained(args.model_name_or_path)

    model_config = AutoConfig.from_pretrained(args.model_name_or_path, trust_remote_code=True)

    base_model = AutoModelForCausalLM.from_pretrained(args.model_name_or_path, torch_dtype=load_type,
                                                      trust_remote_code=True)
    model = PeftModel.from_pretrained(base_model, args.ckpt_path, torch_dtype=load_type)

    if device == torch.device('cpu'):
        model.float()

    model.to(device)
    model.eval()
    logger.info("Load model successfully")

    space_id = tokenizer.encode("\n", add_special_tokens=False)[0]
    space_tensor = torch.LongTensor([[space_id]]).to(device)
    schema_D_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/schema_D.json"
    schema_D_info = json.load(open(schema_D_info_file, "r"))
    schema_A_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/schema_A.json"
    schema_A_info = json.load(open(schema_A_info_file, "r"))
    table_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/table_info.json"
    table_data = json.load(open(table_file, "r"))
    dev_tables_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev_tables.json"
    dev_tables_info_data = json.load(open(dev_tables_info_file, "r"))
    dev_datas = json.load(open(args.dev_data_path, "r"))
    for i, dev_data in enumerate(dev_datas):
        question = dev_data["question"]
        note = dev_data["evidence"]
        db_name = dev_data["db_id"]
        sql = dev_data["SQL"]
        sql_tokens = dev_data["SQL_toks"]
        tables = table_data[db_name]["tables"]
        columns = get_all_columns(db_name, table_data)
        used_tables = separate_tables(sql_tokens, tables)
        used_columns = separate_columns(sql_tokens, columns)

        print_rank_0(
            "=============================== question: {}=====================================================================".format(
                i), log_file)
        schema_D = generate_schema_D(schema_D_info[db_name])
        predict_tables_input_str = PREDICT_TABLES_PROMPT_SCHEMA_D.format(table_info=schema_D, note=note, question=question)
        print_rank_0(predict_tables_input_str, log_file)


        print_rank_0(
            "===============================predict tables:=====================================================================",
            log_file)
        predict_tables_output = codellama_generate(predict_tables_input_str)
        predict_tables = [predict_table for predict_table in predict_tables_output.split("\n")]
        print_rank_0(predict_tables_output, log_file)
        print_rank_0(
            "===============================golden tables:=====================================================================",
            log_file)
        golden_tables = get_used_tables_label(used_tables)
        print_rank_0(golden_tables, log_file)

        print_rank_0(
            "===============================predict columns:=====================================================================",
            log_file)
        predict_columns_schema_A = generate_schema_A(schema_A_info[db_name], predict_tables)
        predict_columns_input_str = PREDICT_COLUMNS_PROMPT_SCHEMA_A.format(table_info=predict_columns_schema_A, note=note, question=question,
                                                           used_tables=get_used_tables_label(predict_tables))
        predict_columns_output = codellama_generate(predict_columns_input_str)
        print_rank_0(predict_columns_input_str + "\n", log_file)
        print_rank_0(predict_columns_output, log_file)
        print_rank_0(
            "===============================golden columns:=====================================================================",
            log_file)
        golden_columns = get_used_columns_label(used_tables, used_columns, table_data[db_name])
        print_rank_0(golden_columns, log_file)

        print_rank_0(
            "===============================predict sql skeleton:=====================================================================",
            log_file)
        predict_columns = get_predict_columns(predict_columns_output)
        predict_sql_skeleton_schema_A = generate_schema_A_based_on_used_tables_and_columns(schema_A_info[db_name], predict_tables, predict_columns)
        predict_sql_skeleton_input_str = PREDICT_SQL_SKELETON_PROMPT_SCHEMA_A.format(table_info=predict_sql_skeleton_schema_A, note=note, question=question,
                                                                used_tables_and_columns=get_used_columns_label(
                                                                    predict_tables, predict_columns,
                                                                    table_data[db_name]))
        predict_sql_skeleton_output = codellama_generate(predict_sql_skeleton_input_str)
        print_rank_0(predict_sql_skeleton_input_str + "\n", log_file)
        print_rank_0(predict_sql_skeleton_output, log_file)
        print_rank_0(
            "===============================golden sql skeleton:=====================================================================",
            log_file)
        sql_skeleton = get_sql_skeleton(sql_tokens, used_tables, used_columns)
        print_rank_0(sql_skeleton, log_file)

        print_rank_0(
            "===============================predict sql:=====================================================================",
            log_file)



        predict_sql_schema_A = generate_schema_A_based_on_used_tables_and_columns(schema_A_info[db_name], predict_tables, predict_columns)
        predict_sql_input_str = PREDICT_SQL_SK_PRED_PROMPT.format(table_info=predict_sql_schema_A, note=note, question=question,
                                                                  sql_skeleton = predict_sql_skeleton_output,
                                              used_tables_and_columns=get_used_columns_label(predict_tables, predict_columns, table_data[db_name]))


        predict_sql_output = codellama_generate(predict_sql_input_str)

        print_rank_0(predict_sql_input_str + "\n", log_file)
        infer_dict = {"cov_id": i, "db_id": db_name,
                      "predict_tables": predict_tables_output , "golden_tables" : golden_tables,
                      "predict_columns": predict_columns_output, "golden_columns": golden_columns,
                      "predict_sql_skeleton": predict_sql_skeleton_output, "golden_sql_skeleton": sql_skeleton,
                      "predict_sql": predict_sql_output, "golden_sql": sql}
        print_rank_0(predict_sql_output + "\n", log_file)
        print_rank_0(
            "=============================== GOLDEN SQL:=====================================================================",
            log_file)
        print_rank_0(sql + "\n", log_file)
        write_cov(infer_dict, result_file)

    logger.info("End inference")

