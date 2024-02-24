"""
schema_T
"""
import json

SCHEMA_VDP = """### Complete sqlite SQL query only and with no explanation
### SQLite SQL tables are requested to be represented in the following format.
TABLE1_NAME (
COLUMN_NAME: (COLUMN_DESCRIPTION), (ENUM_VALUE, ENUM_VALUE2, ...), PRIMARY_KEY
COLUMN_NAME: (COLUMN_DESCRIPTION), (ENUM_VALUE, ENUM_VALUE2, ...)
)
FOREIGN KEYS:
TABLE_NAME1.COLUMN_NAME1=TABLE_NAME2.COLUMN_NAME2
### Here are SQLite SQL tables, with their properties:
{table_info}
### Question: {question}
### Note that: {note}
SELECT
"""

def generate_schema_VDP(schema):
    schema_str = ""
    foreign_keys = schema["foreign_keys"]
    for table_name, table_info in schema.items():
        if "foreign_keys" == table_name:
            continue
        schema_str += table_name + " (\n"
        for col, col_info in table_info.items():
            schema_str += col + ": " + col_info +"\n"
        schema_str.rstrip("\n")
        schema_str += ")\n"
    foreign_keys_info = "FOREIGN KEYS:\n"
    for primary_and_foreign_key in foreign_keys:
        foreign_keys_info += primary_and_foreign_key + "\n"
    schema_info_str = schema_str + foreign_keys_info.rstrip("\n")
    return schema_info_str



def get_train_schema_VDP():
    schema_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/schema/schema_VDP.json"
    schema_info = json.load(open(schema_info_file, "r"))
    train_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/train.json"
    train_data = json.load(open(train_file, "r"))
    f = open("/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/sft/schema/train_schema_VDP.json", "w")
    train_schema_VDP = {}
    for id, data in enumerate(train_data):
        db_name = data["db_id"]
        train_schema_VDP["id"] = id
        train_schema_VDP["db_id"] = db_name
        question = data["question"]
        note = data["evidence"]
        sql = data["SQL"]
        schema = SCHEMA_VDP.format(table_info=generate_schema_VDP(schema_info[db_name]), question=question, note=note)
        train_schema_VDP["conversations"] = [{"from": "human", "value": schema},
                                                 {"from": "assistant", "value": sql}]
        f.write(json.dumps(train_schema_VDP, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    get_train_schema_VDP()