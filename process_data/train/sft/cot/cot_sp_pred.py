predict_tables_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/sft/cot/train_cot_predict_tables_schema_D.json"
predict_columns_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/sft/cot/train_cot_predict_columns_pred_schema_A.json"
predict_sql_sp_pred_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/sft/cot/train_cot_predict_sql_sp_pred_schema_A.json"

cot_sp_pred_file = "/public14_data/wtl/work_point/open_sql_v1/datasets/bird/train/sft/cot/train_cot_sp_pred.json"

with open(predict_tables_file, 'r') as file1:
    predict_tables_data = file1.read()

with open(predict_columns_file, 'r') as file2:
    predict_columns_data = file2.read()

with open(predict_sql_sp_pred_file, 'r') as file3:
    predict_sql_sp_pred_data = file3.read()

sp_pred_data = predict_tables_data + predict_columns_data + predict_sql_sp_pred_data
with open(cot_sp_pred_file, 'w') as output_file:
    output_file.write(sp_pred_data)