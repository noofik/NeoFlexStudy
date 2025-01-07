--Очистка таблицы целевой схемы DS.
--У данной таблицы нет первичного ключа. Можно считать, что мы всегда в нее будем загружать полный набор данных, поэтому перед каждой загрузкой ее необходимо очищать
truncate table ds.ft_posting_f; 

--Идёт пеернос новых всех данных из таблицы preload.ft_posting_f, схемы предзагрузки.
 INSERT INTO ds.ft_posting_f(
     credit_account_rk
    ,debet_account_rk
    ,credit_amount
    ,debet_amount
    ,oper_date
)
SELECT
	 fpf."CREDIT_ACCOUNT_RK"
    ,fpf."DEBET_ACCOUNT_RK" 
    ,fpf."CREDIT_AMOUNT" 
    ,fpf."DEBET_AMOUNT" 
    ,to_date(fpf."OPER_DATE", 'dd.mm.YYYY')
	FROM preload.ft_posting_f fpf;

--Т.к. нет условия постоянного хранения исходных данных, таблица пердзагрузки очищается чтобы исключить накопление дублей и разрастания объёма при частом импорте данных из внешнего источника. 	
truncate table preload.ft_posting_f;