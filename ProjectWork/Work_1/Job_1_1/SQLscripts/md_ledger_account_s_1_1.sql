--Производится обновление уже имеющихся в таблице ds.md_ledger_account_s, целевой схемы DS, данных.
UPDATE ds.md_ledger_account_s as dsmlas 
	SET 
		 chapter = mlas."CHAPTER"
		,chapter_name = mlas."CHAPTER_NAME"
		,section_number = mlas."SECTION_NUMBER"
		,section_name = mlas."SECTION_NAME"
		,subsection_name = mlas."SUBSECTION_NAME"
		,ledger1_account = mlas."LEDGER1_ACCOUNT"
		,ledger1_account_name = mlas."LEDGER_ACCOUNT"
		,ledger_account_name = mlas."LEDGER_ACCOUNT_NAME"
		,characteristic = mlas."CHARACTERISTIC"
		,end_date = to_date(mlas."END_DATE", 'YYYY.mm.dd')
	FROM preload.md_ledger_account_s as mlas
	WHERE 
		(to_date(mlas."START_DATE", 'YYYY.mm.dd') = dsmlas.start_date 
			AND mlas."LEDGER_ACCOUNT" = dsmlas.ledger_account)
		AND
		(dsmlas.end_date <> to_date(mlas."END_DATE", 'YYYY.mm.dd')
		OR dsmlas.chapter <> mlas."CHAPTER"::BPCHAR(1)
		OR dsmlas.chapter_name <> mlas."CHAPTER_NAME"::VARCHAR(16)
		OR dsmlas.section_number <> mlas."SECTION_NUMBER"::INT4
		OR dsmlas.section_name <> mlas."SECTION_NAME"::VARCHAR(22)
		OR dsmlas.subsection_name <> mlas."SUBSECTION_NAME"::VARCHAR(21)
		OR dsmlas.ledger1_account <> mlas."LEDGER1_ACCOUNT"::INT4
		OR dsmlas.ledger1_account_name <> mlas."LEDGER1_ACCOUNT_NAME"::VARCHAR(47)
		OR dsmlas.ledger_account_name <> mlas."LEDGER_ACCOUNT_NAME"::VARCHAR(153)
		OR dsmlas.characteristic <> mlas."CHARACTERISTIC"::BPCHAR(1)
		);
--Идёт пеернос новых данных из таблицы preload.md_ledger_account_s, схемы предзагрузки.
INSERT INTO ds.md_ledger_account_s(
 	 chapter
	,chapter_name
	,section_number
	,section_name
	,subsection_name
	,ledger1_account
	,ledger1_account_name
	,ledger_account
	,ledger_account_name
	,characteristic
	,start_date
	,end_date
)
--Из таблицы схемы предзагрузки, переносятся только уникальные записи, которых ещё нет в ds.md_ledger_account_s.
SELECT DISTINCT 
	 mlas."CHAPTER"
	,mlas."CHAPTER_NAME"
	,mlas."SECTION_NUMBER"
	,mlas."SECTION_NAME"
	,mlas."SUBSECTION_NAME"
	,mlas."LEDGER1_ACCOUNT"
	,mlas."LEDGER1_ACCOUNT_NAME"
	,mlas."LEDGER_ACCOUNT"
	,mlas."LEDGER_ACCOUNT_NAME"
	,mlas."CHARACTERISTIC"
	,to_date(mlas."START_DATE", 'YYYY.mm.dd') AS start_date
	,to_date(mlas."END_DATE", 'YYYY.mm.dd') AS end_date
	FROM preload.md_ledger_account_s mlas
	LEFT JOIN ds.md_ledger_account_s dsmlas ON 
		to_date(mlas."START_DATE", 'YYYY.mm.dd') = dsmlas.start_date 
		AND mlas."LEDGER_ACCOUNT" = dsmlas.ledger_account
	WHERE dsmlas.ledger_account_id IS NULL;
--Т.к. нет условия постоянного хранения исходных данных, таблица пердзагрузки очищается чтобы исключить накопление дублей и разрастания объёма при частом импорте данных из внешнего источника. 		
TRUNCATE TABLE preload.md_ledger_account_s;