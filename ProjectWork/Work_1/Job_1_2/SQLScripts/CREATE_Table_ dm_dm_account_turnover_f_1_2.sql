CREATE SCHEMA IF NOT EXISTS dm;

DROP TABLE IF EXISTS dm.dm_account_turnover_f;
CREATE TABLE dm.dm_account_turnover_f(
	 on_date			DATE
	,account_rk			NUMERIC
	,credit_amount		NUMERIC(23,8)
	,credit_amount_rub	NUMERIC(23,8)
	,debet_amount		NUMERIC(23,8)
	,debet_amount_rub	NUMERIC(23,8)
);


--select * from dm.dm_account_turnover_f;