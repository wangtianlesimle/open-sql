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

    val_data = load_dataset("json", data_files=args.dev_data_path, cache_dir='../')
    db_ids, questions, answers = get_questions(val_data['train'])
    test_dict = {}
    for i, inputs in enumerate(questions):
        print_rank_0(
            "=============================== question: {}=====================================================================".format(
                i), log_file)
        inputs = torch.LongTensor(inputs).unsqueeze(0).to(device)
        print_rank_0(
            "===============================model input:=====================================================================",
            log_file)
        print_rank_0(tokenizer.decode(inputs[0], skip_special_tokens=True), log_file)
        generation_output = model.generate(input_ids=inputs, **generation_config)[0]
        generate_text = tokenizer.decode(generation_output, skip_special_tokens=True)
        input_len = inputs.size()[1]
        gen_len = len(generation_output) - input_len
        model_ans = generation_output[-gen_len:]
        ans_text = tokenizer.decode(model_ans, skip_special_tokens=True)
        print_rank_0(
            "===============================predict columns:=====================================================================",
            log_file)
        print_rank_0(ans_text, log_file)
        infer_dict = {"cov_id": i, "db_id": db_ids[i], "predict_columns": ans_text, "golden_sql": answers[i]}
        test_dict[str(i)] = ans_text + ";\t----- bird -----\t" + db_ids[i]

        print_rank_0(
            "=============================== GOLDEN SQL:=====================================================================",
            log_file)
        print_rank_0(answers[i] + "\n", log_file)
        write_cov(infer_dict, result_file)

    logger.info("End inference")

