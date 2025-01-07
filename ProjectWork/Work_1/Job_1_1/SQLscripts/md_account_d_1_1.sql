--Производится обновление уже имеющихся в таблице ds.md_account_d, целевой схемы DS, данных.
UPDATE ds.md_account_d as dsmad 
	SET 
		 data_actual_end_date = to_date(mad."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd')
		,account_number = mad."ACCOUNT_NUMBER"
		,char_type = mad."CHAR_TYPE"
		,currency_rk = mad."CURRENCY_RK"
		,currency_code = mad."CURRENCY_CODE"
	FROM preload.md_account_d as mad
	WHERE 
		(dsmad.data_actual_date = to_date(mad."DATA_ACTUAL_DATE", 'YYYY.mm.dd') and mad."ACCOUNT_RK" = dsmad.account_rk)
		AND
		(dsmad.data_actual_end_date <> to_date(mad."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd')
		OR dsmad.account_number <> mad."ACCOUNT_NUMBER"::varchar(20)
		OR dsmad.char_type <> mad."CHAR_TYPE"::varchar(1)
		OR dsmad.currency_rk <> mad."CURRENCY_RK"::numeric
		OR dsmad.currency_code <> mad."CURRENCY_CODE"::varchar(3));
--Идёт пеернос новых данных из таблицы preload.md_account_d, схемы предзагрузки.
INSERT INTO ds.md_account_d(
	 data_actual_date		
	,data_actual_end_date	
	,account_rk 			
	,account_number 		
	,char_type 				
	,currency_rk 			
	,currency_code 			
	)
--Из таблицы схемы предзагрузки, переносятся только уникальные записи, которых ещё нет в ds.md_account_d.
SELECT DISTINCT
	 to_date(mad."DATA_ACTUAL_DATE", 'YYYY.mm.dd') AS data_actual_date
	,to_date(mad."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd') AS data_actual_end_date
	,mad."ACCOUNT_RK"
	,mad."ACCOUNT_NUMBER"
	,mad."CHAR_TYPE"
	,mad."CURRENCY_RK"
	,mad."CURRENCY_CODE"
	FROM preload.md_account_d mad
	LEFT JOIN ds.md_account_d dsmad ON 
		to_date(mad."DATA_ACTUAL_DATE", 'YYYY.mm.dd') = dsmad.data_actual_date
		AND mad."ACCOUNT_RK" = dsmad.account_rk
	WHERE dsmad.account_id IS NULL; 
--Т.к. нет условия постоянного хранения исходных данных, таблица пердзагрузки очищается чтобы исключить накопление дублей и разрастания объёма при частом импорте данных из внешнего источника. 	
TRUNCATE TABLE preload.md_account_d;