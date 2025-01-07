INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Завершение загрузки и миграции', 
'{SHEMA=preload, TABLES=ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s} | {SHEMA=dls, TABLES=ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s}', 
NOW())