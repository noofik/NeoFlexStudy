DROP TABLE IF EXISTS dm.dm_f101_round_f_v2;

CREATE TABLE dm.dm_f101_round_f_v2(
	 FROM_DATE				DATE
	,TO_DATE				DATE
	,CHAPTER				VARCHAR(1)
	,LEDGER_ACCOUNT			VARCHAR(5)
	,CHARACTERISTIC			VARCHAR(1)
	,BALANCE_IN_RUB			NUMERIC(23,8)
	,R_BALANCE_IN_RUB		NUMERIC(23,8)
	,BALANCE_IN_VAL			NUMERIC(23,8)
	,R_BALANCE_IN_VAL		NUMERIC(23,8)
	,BALANCE_IN_TOTAL		NUMERIC(23,8)
	,R_BALANCE_IN_TOTAL		NUMERIC(23,8)
	,TURN_DEB_RUB			NUMERIC(23,8)
	,R_TURN_DEB_RUB			NUMERIC(23,8)
	,TURN_DEB_VAL			NUMERIC(23,8)
	,R_TURN_DEB_VAL			NUMERIC(23,8)
	,TURN_DEB_TOTAL			NUMERIC(23,8)
	,R_TURN_DEB_TOTAL		NUMERIC(23,8)
	,TURN_CRE_RUB			NUMERIC(23,8)
	,R_TURN_CRE_RUB			NUMERIC(23,8)
	,TURN_CRE_VAL			NUMERIC(23,8)
	,R_TURN_CRE_VAL			NUMERIC(23,8)
	,TURN_CRE_TOTAL			NUMERIC(23,8)
	,R_TURN_CRE_TOTAL		NUMERIC(23,8)
	,BALANCE_OUT_RUB		NUMERIC(23,8)
	,R_BALANCE_OUT_RUB		NUMERIC(23,8)
	,BALANCE_OUT_VAL		NUMERIC(23,8)
	,R_BALANCE_OUT_VAL		NUMERIC(23,8)
	,BALANCE_OUT_TOTAL		NUMERIC(23,8)
	,R_BALANCE_OUT_TOTAL	NUMERIC(23,8)
);

INSERT INTO dm.dm_f101_round_f_v2(
 	 FROM_DATE
	,TO_DATE			
	,CHAPTER			
	,LEDGER_ACCOUNT		
	,CHARACTERISTIC		
	,BALANCE_IN_RUB		
	,R_BALANCE_IN_RUB	
	,BALANCE_IN_VAL		
	,R_BALANCE_IN_VAL	
	,BALANCE_IN_TOTAL	
	,R_BALANCE_IN_TOTAL	
	,TURN_DEB_RUB		
	,R_TURN_DEB_RUB		
	,TURN_DEB_VAL		
	,R_TURN_DEB_VAL		
	,TURN_DEB_TOTAL		
	,R_TURN_DEB_TOTAL	
	,TURN_CRE_RUB		
	,R_TURN_CRE_RUB		
	,TURN_CRE_VAL		
	,R_TURN_CRE_VAL		
	,TURN_CRE_TOTAL		
	,R_TURN_CRE_TOTAL	
	,BALANCE_OUT_RUB	
	,R_BALANCE_OUT_RUB	
	,BALANCE_OUT_VAL	
	,R_BALANCE_OUT_VAL	
	,BALANCE_OUT_TOTAL	
	,R_BALANCE_OUT_TOTAL
)
SELECT 
	 to_date(FROM_DATE, 'YYYY.mm.dd') AS FROM_DATE
	,to_date(TO_DATE::TEXT, 'YYYY.mm.dd') AS to_date			
	,CHAPTER			
	,LEDGER_ACCOUNT		
	,CHARACTERISTIC		
	,BALANCE_IN_RUB		
	,R_BALANCE_IN_RUB	
	,BALANCE_IN_VAL		
	,R_BALANCE_IN_VAL	
	,BALANCE_IN_TOTAL	
	,R_BALANCE_IN_TOTAL	
	,TURN_DEB_RUB		
	,R_TURN_DEB_RUB		
	,TURN_DEB_VAL		
	,R_TURN_DEB_VAL		
	,TURN_DEB_TOTAL		
	,R_TURN_DEB_TOTAL	
	,TURN_CRE_RUB		
	,R_TURN_CRE_RUB		
	,TURN_CRE_VAL		
	,R_TURN_CRE_VAL		
	,TURN_CRE_TOTAL		
	,R_TURN_CRE_TOTAL	
	,BALANCE_OUT_RUB	
	,R_BALANCE_OUT_RUB	
	,BALANCE_OUT_VAL	
	,R_BALANCE_OUT_VAL	
	,BALANCE_OUT_TOTAL	
	,R_BALANCE_OUT_TOTAL
FROM preload.dm_f101_round_f_v2;

TRUNCATE TABLE preload.dm_f101_round_f_v2;

