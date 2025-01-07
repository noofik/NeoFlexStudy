--Производится обновление уже имеющихся в таблице ds.md_exchange_rate_d, целевой схемы DS, данных.
UPDATE ds.md_exchange_rate_d as dsmerd 
	SET 
		 data_actual_end_date = to_date(merd."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd')
		,reduced_cource = merd."CURRENCY_RK"
		,code_iso_num = merd."CODE_ISO_NUM"
	FROM preload.md_exchange_rate_d as merd
	WHERE 
		(to_date(merd."DATA_ACTUAL_DATE", 'YYYY.mm.dd') = dsmerd.data_actual_date 
			AND merd."CURRENCY_RK" = dsmerd.currency_rk)
		AND
		(data_actual_end_date <> to_date(merd."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd')
			OR reduced_cource <> merd."CURRENCY_RK"::FLOAT8
			OR code_iso_num <> merd."CODE_ISO_NUM"::VARCHAR(3));
--Идёт пеернос новых данных из таблицы preload.md_exchange_rate_d, схемы предзагрузки.
INSERT INTO ds.md_exchange_rate_d(
 	 data_actual_date
	,data_actual_end_date
	,currency_rk
	,reduced_cource
	,code_iso_num	
)
--Из таблицы схемы предзагрузки, переносятся только уникальные записи, которых ещё нет в ds.md_exchange_rate_d.
SELECT DISTINCT
	 to_date(merd."DATA_ACTUAL_DATE", 'YYYY.mm.dd') AS data_actual_date
	,to_date(merd."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd') AS data_actual_end_date
	,merd."CURRENCY_RK"
	,merd."REDUCED_COURCE"
	,merd."CODE_ISO_NUM"
	FROM preload.md_exchange_rate_d merd
	LEFT JOIN ds.md_exchange_rate_d dsmerd ON to_date(merd."DATA_ACTUAL_DATE", 'YYYY.mm.dd') = dsmerd.data_actual_date AND merd."CURRENCY_RK" = dsmerd.currency_rk
	WHERE dsmerd.exchange_rate_id IS NULL;
--Т.к. нет условия постоянного хранения исходных данных, таблица пердзагрузки очищается чтобы исключить накопление дублей и разрастания объёма при частом импорте данных из внешнего источника. 
TRUNCATE TABLE preload.md_exchange_rate_d;