select * from logs.load_log ll order by id 

select * from dm.dm_account_balance_f dabf 

--call ds.fill_account_balance_f('2018-01-09');

select * from dm.dm_account_balance_f where on_date = '2018-01-09';

DO $$
declare day_var date;

begin
    FOR day_var IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day'::interval)
    LOOP
        call ds.fill_account_balance_f(day_var);
    END LOOP;
END; $$


select * from dm.dm_account_balance_f where on_date = '2018-01-09';

select * from dm.dm_account_balance_f order by 1;

select * from ds.md_account_d where account_rk  = 13630;

SELECT * FROM dm.dm_account_turnover_f where account_rk  = 13630;

select * from dm.dm_account_balance_f where account_rk  = 13630 order by 1;




