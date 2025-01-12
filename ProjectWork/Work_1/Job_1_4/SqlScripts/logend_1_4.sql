INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Завершение выгрузки в CSV файл', 
'{SHEMA=dm, TABLES=dm_f101_round_f}', 
NOW())