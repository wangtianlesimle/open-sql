import json
import re

import nltk

def merge_tokens(tokens :list):

    tokens = merge_tokens_base_backtick(tokens)
    tokens = merge_tokens_base_quotation_marks(tokens)
    tokens = merge_alais_name(tokens)
    return tokens

def merge_tokens_base_backtick(tokens :list):
    """
    根据反引号将列名被分为多个token的组合起来
    :param tokens:
    :return:
    """
    indexes = []
    for id, tok in enumerate(tokens):
        if "`" == tok:
            indexes.append(id)
    assert len(indexes) % 2 == 0
    new_sql_tokens = []
    if len(indexes) != 0:
        next_start = 0
        for start in range(0, len(indexes), 2):
            tok_str = join(tokens[indexes[start]:indexes[start + 1] + 1])
            new_sql_tokens.extend(tokens[next_start:indexes[start]])
            new_sql_tokens.append(tok_str)
            end_index = indexes[start + 2] if start + 2 < len(indexes) else len(tokens)
            if indexes[start + 1] + 1 > len(tokens):
                break
            new_sql_tokens.extend(tokens[indexes[start + 1] + 1: end_index])
            if end_index == len(tokens):
                break
            next_start = indexes[start + 2]
    else:
        new_sql_tokens = tokens
    return new_sql_tokens

def merge_tokens_base_quotation_marks(tokens :list):
    """
    根据引号将实体被分为多个token的组合起来
    :param tokens:
    :return:
    """
    indexes = []
    for id, tok in enumerate(tokens):
        if "'" == tok[0] and "'" == tok[len(tok) - 1] and len(tok) > 1:
            continue
        if "'" == tok[0] or "'" == tok[len(tok) - 1]:
            indexes.append(id)
    assert len(indexes) % 2 == 0
    new_sql_tokens = []
    if len(indexes) != 0:
        next_start = 0
        for start in range(0, len(indexes), 2):
            tok_str = join(tokens[indexes[start]:indexes[start + 1] + 1])
            new_sql_tokens.extend(tokens[next_start:indexes[start]])
            new_sql_tokens.append(tok_str)
            end_index = indexes[start + 2] if start + 2 < len(indexes) else len(tokens)
            if indexes[start + 1] + 1 > len(tokens):
                break
            new_sql_tokens.extend(tokens[indexes[start + 1] + 1: end_index])
            if end_index == len(tokens):
                break
            next_start = indexes[start + 2]
    else:
        new_sql_tokens = tokens
    return new_sql_tokens

def merge_alais_name(tokens :list):
    """
    将t1.columns被分为多个token的组合成一个token
    :param tokens:
    :return:
    """
    pattern = re.compile(r'T\d\.$')
    new_sql_tokens = []
    is_skip = False
    for idx, token in enumerate(tokens):
        if is_skip:
            is_skip = False
            continue
        if pattern.search(token):
            column_name_str = token + tokens[idx+1]
            new_sql_tokens.append(column_name_str)
            is_skip = True
        else:
            new_sql_tokens.append(token)
            is_skip = False
    return new_sql_tokens


def join(toks):
    tok_str = ""
    for tok in toks:
        if "`" == tok or ")" == tok or "(" == tok or "'" == tok:
            tok_str += tok
        else:
            tok_str += tok + " "
    tok_str = tok_str.replace(" '", "'")
    tok_str = tok_str.replace(" `", "`")
    return tok_str.strip()


# 将dev data中的sql语句进行分词
def generate_dev_sql_tokens():
    dev_data_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev.json"
    dev_datas = json.load(open(dev_data_file, "r"))
    for dev_data in dev_datas:
        # if dev_data["question_id"] == 2:
        #     print(1)
        sql = dev_data["SQL"]
        question = dev_data["question"]
        sql_tokens = nltk.word_tokenize(sql)
        sql_tokens =  merge_tokens(sql_tokens)
        question_tokens = nltk.word_tokenize(question)
        dev_data["SQL_toks"] = sql_tokens
        dev_data["question_toks"] = question_tokens
    json.dump(dev_datas, open(dev_data_file, "w"))

if __name__ == "__main__":
    generate_dev_sql_tokens()