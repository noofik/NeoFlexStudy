CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate date) 
LANGUAGE SQL AS $$

--Start log   
INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Старт процедуры', 
'{SHEMA=ds, PROCEDURE=ds.fill_account_turnover_f, DATE = ' || i_OnDate || ' }', 
NOW());

--UPDATE block
with
get_credit_data
	(c_account_rk 
	,sum_cred_ammount
	,sum_cred_ammount_r
	,op_date)
AS
(	select 
		 credit_account_rk 
		,sum(credit_amount)
		,sum(p.credit_amount*coalesce(e.reduced_cource, 1))
		,oper_date
	from ds.ft_posting_f as p
	left join ds.md_account_d as a on
		p.credit_account_rk = a.account_rk
	left join ds.md_exchange_rate_d as e on 
		(p.oper_date between e.data_actual_date and e.data_actual_end_date)
		and 
		a.currency_rk = e.currency_rk
	where oper_date = i_OnDate
	group by oper_date, credit_account_rk
),

get_debet_data
	(d_account_rk
	,sum_deb_ammount
	,sum_deb_ammount_r
	,op_date)
AS
(	select 
		 debet_account_rk
		,sum(debet_amount)
		,sum(p.debet_amount*coalesce(e.reduced_cource, 1))
		,oper_date
	from ds.ft_posting_f as p
	left join ds.md_account_d as a on
		p.debet_account_rk = a.account_rk
	left join ds.md_exchange_rate_d as e on 
		(p.oper_date between e.data_actual_date and e.data_actual_end_date)
		and 
		a.currency_rk = e.currency_rk
	where oper_date = i_OnDate
	group by oper_date, debet_account_rk
)

UPDATE dm.dm_account_turnover_f as dmatf 
	SET 
	 credit_amount = gdod.credit_amount
	,credit_amount_rub = gdod.credit_amount_rub
	,debet_amount = gdod.debet_amount
	,debet_amount_rub = gdod.debet_amount_rub
	FROM (	
		select 
			 COALESCE(gcd.op_date, gdd.op_date) as on_date
			,dsmad.account_rk as account_rk 
			,coalesce(gcd.sum_cred_ammount, 0) as credit_amount
			,coalesce(gcd.sum_cred_ammount_r, 0) as credit_amount_rub
			,coalesce(gdd.sum_deb_ammount, 0) as debet_amount
			,coalesce(gdd.sum_deb_ammount_r, 0) as debet_amount_rub
		from ds.md_account_d as dsmad
		left join get_credit_data as gcd on 
			gcd.c_account_rk = dsmad.account_rk
		left join get_debet_data as gdd on 
			gdd.d_account_rk = dsmad.account_rk
		where 
			(gcd.sum_cred_ammount is not null 
			or 
			gdd.sum_deb_ammount is not null)) as gdod
	WHERE 
	(gdod.on_date = dmatf.on_date and gdod.account_rk = dmatf.account_rk)
	AND
	(gdod.credit_amount::numeric(23,8) <> dmatf.credit_amount
		or gdod.credit_amount_rub::numeric(23,8) <> dmatf.credit_amount_rub
		or gdod.debet_amount::numeric(23,8) <> dmatf.debet_amount
		or gdod.debet_amount_rub::numeric(23,8) <> dmatf.debet_amount_rub);

--INSERT Block
with
get_credit_data
	(c_account_rk 
	,sum_cred_ammount
	,sum_cred_ammount_r
	,op_date)
AS
(	select 
		 credit_account_rk 
		,sum(credit_amount)
		,sum(p.credit_amount*coalesce(e.reduced_cource, 1))
		,oper_date
	from ds.ft_posting_f as p
	left join ds.md_account_d as a on
		p.credit_account_rk = a.account_rk
	left join ds.md_exchange_rate_d as e on 
		(p.oper_date between e.data_actual_date and e.data_actual_end_date)
		and 
		a.currency_rk = e.currency_rk
	where oper_date = i_OnDate
	group by oper_date, credit_account_rk
),

get_debet_data
	(d_account_rk
	,sum_deb_ammount
	,sum_deb_ammount_r
	,op_date)
AS
(	select 
		 debet_account_rk
		,sum(debet_amount)
		,sum(p.debet_amount*coalesce(e.reduced_cource, 1))
		,oper_date
	from ds.ft_posting_f as p
	left join ds.md_account_d as a on
		p.debet_account_rk = a.account_rk
	left join ds.md_exchange_rate_d as e on 
		(p.oper_date between e.data_actual_date and e.data_actual_end_date)
		and 
		a.currency_rk = e.currency_rk
	where oper_date = i_OnDate
	group by oper_date, debet_account_rk
),

get_data_on_date
	(on_date			
	,account_rk			
	,credit_amount		
	,credit_amount_rub	
	,debet_amount		
	,debet_amount_rub)
AS
(	select 
		COALESCE(gcd.op_date, gdd.op_date) as on_date
		,dsmad.account_rk as account_rk 
		,coalesce(gcd.sum_cred_ammount, 0) as credit_amount
		,coalesce(gcd.sum_cred_ammount_r, 0) as credit_amount_rub
		,coalesce(gdd.sum_deb_ammount, 0) as debet_amount
		,coalesce(gdd.sum_deb_ammount_r, 0) as debet_amount_rub
	from ds.md_account_d as dsmad
	left join get_credit_data as gcd on 
		gcd.c_account_rk = dsmad.account_rk
	left join get_debet_data as gdd on 
		gdd.d_account_rk = dsmad.account_rk
	where 
		(gcd.sum_cred_ammount is not null 
			or 
		gdd.sum_deb_ammount is not null)
)

INSERT INTO dm.dm_account_turnover_f(
	 on_date			
	,account_rk			
	,credit_amount		
	,credit_amount_rub	
	,debet_amount		
	,debet_amount_rub	
    )
SELECT 
	 i0.on_date			
	,i0.account_rk			
	,i0.credit_amount		
	,i0.credit_amount_rub	
	,i0.debet_amount		
	,i0.debet_amount_rub
	FROM get_data_on_date as i0
	LEFT JOIN dm.dm_account_turnover_f as dmatf ON 
		dmatf.on_date = i0.on_date 
		AND 
		dmatf.account_rk = i0.account_rk
	WHERE dmatf.account_rk IS NULL;

--End log
INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Завершение процедуры', 
'{SHEMA=ds, PROCEDURE=ds.fill_account_turnover_f, DATE = ' || i_OnDate || ' }', 
NOW()) 

$$;
