"""
schema_T
"""
import json

SCHEMA_VDT = """### Complete sqlite SQL query only and with no explanation
### SQLite SQL tables are requested to be represented in the following format.
TABLE1_NAME (
COLUMN_NAME: DATA_TYPE, (COLUMN_DESCRIPTION), (ENUM_VALUE, ENUM_VALUE2, ...)
)
FOREIGN KEYS:
TABLE.COLUMN_NAME=TABLE.COLUMN_NAME
### Here are SQLite SQL tables, with their properties:
{table_info}
### Question: {question}
### Note that: {note}
SELECT
"""

def generate_schema_VDT(schema):
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



def get_dev_schema_VDT():
    schema_info_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/schema/schema_VDT.json"
    schema_info = json.load(open(schema_info_file, "r"))
    dev_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/dev.json"
    dev_data = json.load(open(dev_file, "r"))
    f = open("/public14_data/wtl/work_point/open_sql_v1/datasets/bird/dev/sft/schema/dev_schema_VDT.json", "w")
    dev_schema_VD = {}
    for id, data in enumerate(dev_data):
        db_name = data["db_id"]
        dev_schema_VD["id"] = id
        dev_schema_VD["db_id"] = db_name
        question = data["question"]
        note = data["evidence"]
        sql = data["SQL"]
        schema = SCHEMA_VDT.format(table_info=generate_schema_VDT(schema_info[db_name]), question=question, note=note)
        dev_schema_VD["conversations"] = [{"from": "human", "value": schema},
                                                 {"from": "assistant", "value": sql}]
        f.write(json.dumps(dev_schema_VD, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    get_dev_schema_VDT()