INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Старт загрузки из CSV файла', 
'{SHEMA=dm, TABLES=dm _f101_round_f_v2}', 
NOW())