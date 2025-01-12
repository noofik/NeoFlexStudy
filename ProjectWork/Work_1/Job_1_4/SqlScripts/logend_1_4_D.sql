INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Завершение загрузки из CSV', 
'{SHEMA=dm, TABLES=dm _f101_round_f_v2}', 
NOW())