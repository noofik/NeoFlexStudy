--TRUNCATE TABLE DM.DM_ACCOUNT_TURNOVER_F;

--CALL ds.fill_account_turnover_f('2018-01-09');

--SELECT * FROM dm.dm_account_turnover_f

--SELECT * FROM dm.dm_account_turnover_f WHERE on_date = '2018-01-09';


DO $$
declare day_var date;

begin
    FOR day_var IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day'::interval)
    LOOP
        CALL ds.fill_account_turnover_f(day_var); 
    END LOOP;
END; $$

--SELECT * FROM dm.dm_account_turnover_f WHERE on_date = '2018-01-09';

--SELECT * FROM dm.dm_account_turnover_f

--select * from logs.load_log ll 

