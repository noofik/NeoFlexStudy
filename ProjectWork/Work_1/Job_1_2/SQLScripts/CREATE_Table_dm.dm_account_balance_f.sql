create table dm.dm_account_balance_f
as
select 
	 dsfbf.on_date as on_date 
	,dsfbf.account_rk as account_rk
	,dsfbf.balance_out as balance_out
	,(dsfbf.balance_out*coalesce(e.reduced_cource, 1)) as balance_out_rub 
from ds.ft_balance_f as dsfbf
left join ds.md_account_d as dmad on 
	dmad.account_rk = dsfbf.account_rk 
left join ds.md_exchange_rate_d as e on 
	(dsfbf.on_date between e.data_actual_date and e.data_actual_end_date)
	and 
	dmad.currency_rk = e.currency_rk
where dsfbf.on_date = '2017-12-31';

--select * from dm.dm_account_balance_f
