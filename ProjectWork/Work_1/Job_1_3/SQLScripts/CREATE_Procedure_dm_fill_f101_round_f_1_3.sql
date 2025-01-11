CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate date) 
LANGUAGE plpgsql 
AS $$

declare t_i_date date;

BEGIN

	select date_trunc('month', i_OnDate) INTO t_i_date;


--Start log       
INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Старт процедуры', 
'{SHEMA=dm, PROCEDURE=fill_f101_round_f, DATE = ' || t_i_date || ' }', 
NOW());

--Clear table on date
DELETE FROM dm.dm_f101_round_f WHERE FROM_DATE = t_i_date;

with get_all_rub (account_rk, turn_deb_rub, turn_cre_rub)
as (
	select 
		 a.account_rk
		,coalesce(sum(debet_amount_rub), 0)
		,coalesce(sum(credit_amount_rub), 0)
	from dm.dm_account_turnover_f as at
	join ds.md_account_d as a on
		a.account_rk = at.account_rk
	where 
		on_date between t_i_date - INTERVAL '1 Month' and t_i_date - INTERVAL '1 Days'
		and a.currency_code in ('810', '643')
	group by a.account_rk
),

get_all_val(account_rk, turn_deb_val, turn_cre_val)
as (
	select 
		 a.account_rk
		,coalesce(sum(debet_amount_rub), 0)
		,coalesce(sum(credit_amount_rub), 0)
	from dm.dm_account_turnover_f as at
	join ds.md_account_d as a on
		a.account_rk = at.account_rk
	where 
		on_date between t_i_date - INTERVAL '1 Month' and t_i_date - INTERVAL '1 Days'
		and a.currency_code not in ('810', '643')
	group by a.account_rk
),

get_fish 
as (
select 
	 t_i_date as FROM_DATE 
	,l.chapter as CHAPTER
	,left(a.account_number, 5) as LEDGER_ACCOUNT
	,a.char_type as CHARACTERISTIC
	,case 
		when a.currency_code not in ('810', '643') then 0
		else coalesce(ab.balance_out_rub, 0)
	 end as BALANCE_IN_RUB
	,case 
		when a.currency_code in ('810', '643') then 0
		else coalesce(ab.balance_out, 0) 
	 end as BALANCE_IN_VAL
	,ab.balance_out_rub as BALANCE_IN_TOTAL
	,coalesce(gr.turn_deb_rub, 0) as TURN_DEB_RUB
	,coalesce(gv.turn_deb_val, 0) as TURN_DEB_VAL
	,coalesce(gr.turn_deb_rub, 0) + coalesce(gv.turn_deb_val, 0) as TURN_DEB_TOTAL
	,coalesce(gr.turn_cre_rub, 0) as TURN_CRE_RUB
	,coalesce(gv.turn_cre_val, 0) as TURN_CRE_VAL
	,coalesce(gr.turn_cre_rub, 0) + coalesce(gv.turn_cre_val, 0) as TURN_CRE_TOTAL
	,case 
		when a.currency_code not in ('810', '643') then 0
		else coalesce(abe.balance_out, 0) 
		end as BALANCE_OUT_RUB
	,case 
		when a.currency_code in ('810', '643') then 0
		else coalesce(abe.balance_out, 0) 
	 end as BALANCE_OUT_VAL
	,coalesce(abe.balance_out_rub, 0) as BALANCE_OUT_TOTAL
from ds.md_ledger_account_s as l
join ds.md_account_d as a on
	left(a.account_number, 5) = l.ledger_account::text 
left join dm.dm_account_balance_f as ab on 
	ab.account_rk = a.account_rk and ab.on_date = t_i_date - INTERVAL '1 Month 1 Days'
left join get_all_rub as gr on gr.account_rk = a.account_rk 
left join get_all_val as gv on gv.account_rk = a.account_rk 
left join dm.dm_account_balance_f as abe on 
	abe.account_rk = a.account_rk and abe.on_date = t_i_date - INTERVAL '1 Days'
order by 3,4
)

INSERT INTO dm.dm_f101_round_f(
	 FROM_DATE
	,CHAPTER
	,LEDGER_ACCOUNT
	,CHARACTERISTIC
	,BALANCE_IN_RUB
	,BALANCE_IN_VAL
	,BALANCE_IN_TOTAL
	,TURN_DEB_RUB
	,TURN_DEB_VAL
	,TURN_DEB_TOTAL
	,TURN_CRE_RUB
	,TURN_CRE_VAL
	,TURN_CRE_TOTAL
	,BALANCE_OUT_RUB
	,BALANCE_OUT_VAL
	,BALANCE_OUT_TOTAL
    )
select 
	 MAX(FROM_DATE) as FROM_DATE
	,MAX(CHAPTER) as CHAPTER
	,LEDGER_ACCOUNT
	,MAX(CHARACTERISTIC) as CHARACTERISTIC
	,SUM(BALANCE_IN_RUB) as BALANCE_IN_RUB
	,SUM(BALANCE_IN_VAL) as BALANCE_IN_VAL
	,SUM(BALANCE_IN_TOTAL) as BALANCE_IN_TOTAL 
	,SUM(TURN_DEB_RUB) as TURN_DEB_RUB
	,SUM(TURN_DEB_VAL) as TURN_DEB_VAL
	,SUM(TURN_DEB_TOTAL) as TURN_DEB_TOTAL
	,SUM(TURN_CRE_RUB) as TURN_CRE_RUB
	,SUM(TURN_CRE_VAL) as TURN_CRE_VAL
	,SUM(TURN_CRE_TOTAL) as TURN_CRE_TOTAL
	,SUM(BALANCE_OUT_RUB) as BALANCE_OUT_RUB
	,SUM(BALANCE_OUT_VAL) as BALANCE_OUT_VAL
	,SUM(BALANCE_OUT_TOTAL) as BALANCE_OUT_TOTAL
from get_fish group by LEDGER_ACCOUNT;

--End log
INSERT INTO logs.load_log(actions, objects, action_date)
VALUES ('Завершение процедуры', 
'{SHEMA=dm, PROCEDURE=fill_f101_round_f, DATE = ' || t_i_date || ' }',
NOW());
END;
$$;
