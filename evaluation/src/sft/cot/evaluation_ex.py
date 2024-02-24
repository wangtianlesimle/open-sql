import sys
import json
import argparse
import sqlite3
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut
import multiprocessing



def execute_sql(example, db_path):
    predicted_sql = example["predict_sql"]
    ground_truth = example["golden_sql"]
    conn = sqlite3.connect(db_path)
    # Connect to the database
    cursor = conn.cursor()
    cursor.execute(predicted_sql)
    predicted_res = cursor.fetchall()
    cursor.execute(ground_truth)
    ground_truth_res = cursor.fetchall()
    res = 0
    example["result"] = "不正确"
    if set(predicted_res) == set(ground_truth_res):
        res = 1
        example["result"] = "正确"
    ground_truth_res.clear()
    predicted_res.clear()
    return res



def run_sqls(predict_data, db_places, num_cpus=1, meta_time_out=30.0):
    for i, example in enumerate(predict_data):
        # if i == 607:
        #     example["result"] = "不正确"
        #     continue
        # if i == 617:
        #     example["result"] = "不正确"
        #     continue
        if example["predict_sql"] == "":
            example["result"] = "不正确"
            continue
        db_place = db_places[i]
        try:
            res = func_timeout(meta_time_out, execute_sql,args=(example, db_place))
        except KeyboardInterrupt:
            sys.exit(0)
        except FunctionTimedOut:
            result = [(f'timeout',)]
            res = 0
            example["result"] = "不正确"
        except Exception as e:
            result = [(f'error',)]  # possibly len(query) > 512 or not executable
            res = 0
            example["result"] = "不正确"
            example["reason"] = e.__str__()
        print("idx:{}, res:{}".format(i,res))


def compute_acc(predict_data):
    num_queries = len(predict_data)
    results = [res['result'] == "正确" for res in predict_data]
    simple_results, moderate_results, challenging_results = [], [], []

    for i, content in enumerate(predict_data):
        if content['difficulty'] == 'simple':
            simple_results.append(predict_data[i])

        if content['difficulty'] == 'moderate':
            moderate_results.append(predict_data[i])

        if content['difficulty'] == 'challenging':
            challenging_results.append(predict_data[i])

    simple_acc = sum([res['result']=="正确" for res in simple_results]) / len(simple_results)
    moderate_acc = sum([res['result']=="正确" for res in moderate_results]) / len(moderate_results)
    challenging_acc = sum([res['result']=="正确"for res in challenging_results]) / len(challenging_results)
    all_acc = sum(results) / num_queries
    count_lists = [len(simple_results), len(moderate_results), len(challenging_results), num_queries]
    return simple_acc * 100, moderate_acc * 100, challenging_acc * 100, all_acc * 100, count_lists


def print_data(score_lists, count_lists):
    levels = ['simple', 'moderate', 'challenging', 'total']
    print("{:20} {:20} {:20} {:20} {:20}".format("", *levels))
    print("{:20} {:<20} {:<20} {:<20} {:<20}".format('count', *count_lists))

    print('======================================    ACCURACY    =====================================')
    print("{:20} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f}".format('accuracy', *score_lists))


if __name__ == '__main__':

    # 读取推测文件
    predict_data = json.load(open("/public14_data/wtl/work_point/open_sql_v1/evaluation/codellama/sft/cot/output/json/inference_cot_predict_sql_sk_pred_with_one_model_v3.json", "r"))
    # 读取dev文件
    dev_data = json.load(open("/public14_data/wtl/text2sql/datasets/bird/dev/dev.json", "r"))

    db_root_path = "/public14_data/wtl/text2sql/datasets/bird/dev/dev_databases/"

    db_paths = []

    for idx, example in enumerate(predict_data):
        # if idx > 500:
        #     break
        db_paths.append(db_root_path + example["db_id"] + '/' + example["db_id"] + '.sqlite')
        example["difficulty"] = dev_data[idx]["difficulty"]

    run_sqls(predict_data, db_places=db_paths, num_cpus=16, meta_time_out=60)

    print('start calculate')
    simple_acc, moderate_acc, challenging_acc, acc, count_lists = compute_acc(predict_data)
    score_lists = [simple_acc, moderate_acc, challenging_acc, acc]
    with open("/public14_data/wtl/work_point/open_sql_v1/evaluation/codellama/sft/cot/result/result_cot_predict_sql_sk_pred_with_one_model_v3.json", "w") as f:
        f.write(json.dumps(predict_data, ensure_ascii=False) + "\n")
    print_data(score_lists, count_lists)
    print('===========================================================================================')
    print("Finished evaluation")