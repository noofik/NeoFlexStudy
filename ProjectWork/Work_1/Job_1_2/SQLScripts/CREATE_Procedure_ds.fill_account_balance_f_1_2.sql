CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate date) 
LANGUAGE SQL AS $$

--Start log       
INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Старт процедуры', 
'{SHEMA=ds, PROCEDURE=ds.fill_account_balance_f, DATE = ' || i_OnDate || ' }', 
NOW());

--Clear table on date
DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate AND on_date >= '2018-01-01';

--INSERT Data on date
INSERT INTO dm.dm_account_balance_f(
	 on_date			
	,account_rk			
	,balance_out		
	,balance_out_rub 
    )
select 
	i_OnDate as on_date 
	,a.account_rk as account_rk
	,case
		when a.char_type = 'А' then COALESCE(b.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)	
		when a.char_type = 'П' then COALESCE(b.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
	 end as balance_out
	,case	
		when a.char_type = 'А' then COALESCE(b.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0) 
		when a.char_type = 'П' then COALESCE(b.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
	 end as balance_out_rub 
from ds.md_account_d as a
left join dm.dm_account_balance_f as b on
	b.account_rk = a.account_rk
	and on_date = i_OnDate - INTERVAL '1 Days'
left join dm.dm_account_turnover_f as t on
	t.account_rk = a.account_rk
	and t.on_date = i_OnDate
where 
	(i_OnDate between a.data_actual_date and a.data_actual_end_date); 

--End log
INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Завершение процедуры', 
'{SHEMA=ds, PROCEDURE=ds.fill_account_balance_f, DATE = ' || i_OnDate || ' }', 
NOW());

$$;
