--Производится обновление уже имеющихся в таблице ds.md_currency_d, целевой схемы DS, данных.
UPDATE ds.md_currency_d AS dsmcd 
	SET 
		 data_actual_end_date = to_date(mcd."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd')
		,currency_code = mcd."CURRENCY_CODE"
		,code_iso_char = mcd."CODE_ISO_CHAR"
	FROM preload.md_currency_d AS mcd
	WHERE 
		(to_date(mcd."DATA_ACTUAL_DATE", 'YYYY.mm.dd') = dsmcd.data_actual_date AND mcd."CURRENCY_RK" = dsmcd.currency_rk)
		AND
		(dsmcd.data_actual_end_date <> to_date(mcd."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd')
		OR dsmcd.currency_code <> mcd."CURRENCY_CODE"::VARCHAR(3)
		OR dsmcd.code_iso_char <> mcd."CODE_ISO_CHAR"::VARCHAR(3));
--Идёт пеернос новых данных из таблицы preload.ft_balance_f, схемы предзагрузки.
INSERT INTO ds.md_currency_d(
 	 currency_rk
	,data_actual_date
	,data_actual_end_date
	,currency_code
	,code_iso_char		
	)
--Из таблицы схемы предзагрузки, переносятся только уникальные записи, которых ещё нет в ds.md_currency_d.
SELECT DISTINCT
	 mcd."CURRENCY_RK"
	,to_date(mcd."DATA_ACTUAL_DATE", 'YYYY.mm.dd') AS data_actual_date
	,to_date(mcd."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd') AS data_actual_end_date
	,mcd."CURRENCY_CODE"
	,mcd."CODE_ISO_CHAR"
	FROM preload.md_currency_d mcd
	LEFT JOIN ds.md_currency_d dsmcd ON
		to_date(mcd."DATA_ACTUAL_DATE", 'YYYY.mm.dd') = dsmcd.data_actual_date 
		AND mcd."CURRENCY_RK" = dsmcd.currency_rk
	WHERE dsmcd.currency_id IS NULL;
--Т.к. нет условия постоянного хранения исходных данных, таблица пердзагрузки очищается чтобы исключить накопление дублей и разрастания объёма при частом импорте данных из внешнего источника. 
TRUNCATE TABLE preload.md_currency_d;