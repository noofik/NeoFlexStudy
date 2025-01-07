--Производится обновление уже имеющихся в таблице ds.ft_balance_f, целевой схемы DS, данных.
UPDATE ds.ft_balance_f as dsfbf 
	SET 
		 currency_rk = fbf."CURRENCY_RK"
		,balance_out = fbf."BALANCE_OUT"
	FROM preload.ft_balance_f as fbf
	WHERE 
		(dsfbf.on_date = to_date(fbf."ON_DATE", 'dd.mm.YYYY') and fbf."ACCOUNT_RK" = dsfbf.account_rk)
		AND
		(dsfbf.currency_rk <> fbf."CURRENCY_RK"::INT8 
		OR dsfbf.balance_out <> fbf."BALANCE_OUT"::NUMERIC(19,2));

--Идёт пеернос новых данных из таблицы preload.ft_balance_f, схемы предзагрузки.
INSERT INTO ds.ft_balance_f(
      account_rk
    , currency_rk
    , balance_out
    , on_date
	)
--Из таблицы схемы предзагрузки, переносятся только уникальные записи, которых ещё нет в ds.ft_balance_f.
SELECT DISTINCT 
	fbf."ACCOUNT_RK" AS account_rk, 
	fbf."CURRENCY_RK",  
	fbf."BALANCE_OUT", 
	to_date(fbf."ON_DATE", 'dd.mm.YYYY') AS on_date
	FROM preload.ft_balance_f fbf
	LEFT JOIN ds.ft_balance_f dsfbf ON 
		to_date(fbf."ON_DATE", 'dd.mm.YYYY') = dsfbf.on_date 
		AND fbf."ACCOUNT_RK" = dsfbf.account_rk
	WHERE dsfbf.balance_id IS NULL; 

--Т.к. нет условия постоянного хранения исходных данных, таблица пердзагрузки очищается чтобы исключить накопление дублей и разрастания объёма при частом импорте данных из внешнего источника. 	
TRUNCATE TABLE preload.ft_balance_f;