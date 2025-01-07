CREATE SCHEMA IF NOT EXISTS preload;
CREATE SCHEMA IF NOT EXISTS ds;

DROP TABLE IF EXISTS ds.ft_balance_f;
CREATE TABLE ds.ft_balance_f(
      balance_id    SERIAL8
    , account_rk    INT8
    , currency_rk   INT8
    , balance_out   NUMERIC(19,2)
    , on_date       DATE
);

DROP TABLE IF EXISTS ds.ft_posting_f;
CREATE TABLE ds.ft_posting_f(
      posting_id        SERIAL8
    , credit_account_rk INT8
    , debet_account_rk  INT8
    , credit_amount     NUMERIC(19,2)
    , debet_amount      NUMERIC(19,2)
    , oper_date         DATE
);

DROP TABLE IF EXISTS ds.md_account_d;
CREATE TABLE ds.md_account_d(
	 account_id        		SERIAL8
	,data_actual_date		DATE NOT NULL
	,data_actual_end_date	DATE NOT NULL
	,account_rk 			NUMERIC not null
	,account_number 		VARCHAR(20) not null
	,char_type 				VARCHAR(1) not null
	,currency_rk 			NUMERIC not null
	,currency_code 			VARCHAR(3) not null
);

DROP TABLE IF EXISTS ds.md_currency_d;
CREATE TABLE ds.md_currency_d(
	 currency_id			SERIAL8
	,currency_rk			NUMERIC not null
	,data_actual_date		DATE not null
	,data_actual_end_date	DATE
	,currency_code			VARCHAR(3)
	,code_iso_char			VARCHAR(3)
);

DROP TABLE IF EXISTS ds.md_exchange_rate_d;
CREATE TABLE ds.md_exchange_rate_d(
	 exchange_rate_id 		SERIAL8
	,data_actual_date		DATE not null
	,data_actual_end_date	DATE
	,currency_rk			NUMERIC not null
	,reduced_cource			FLOAT
	,code_iso_num			VARCHAR(3)
);

DROP TABLE IF EXISTS ds.md_ledger_account_s;
CREATE TABLE ds.md_ledger_account_s(
	ledger_account_id				SERIAL8
	,chapter						CHAR(1)
	,chapter_name					VARCHAR(16)
	,section_number					INTEGER
	,section_name					VARCHAR(22)
	,subsection_name				VARCHAR(21)
	,ledger1_account				INTEGER
	,ledger1_account_name			VARCHAR(47)
	,ledger_account					INTEGER not null
	,ledger_account_name			VARCHAR(153)
	,characteristic					CHAR(1)
	,is_resident					INTEGER
	,is_reserve						INTEGER
	,is_reserved					INTEGER
	,is_loan						INTEGER
	,is_reserved_assets				INTEGER
	,is_overdue						INTEGER
	,is_interest					INTEGER
	,pair_account					VARCHAR(5)
	,start_date						DATE not null
	,end_date						DATE
	,is_rub_only					INTEGER
	,min_term						VARCHAR(1)
	,min_term_measure				VARCHAR(1)
	,max_term						VARCHAR(1)
	,max_term_measure				VARCHAR(1)
	,ledger_acc_full_name_translit	VARCHAR(1)
	,is_revaluation					VARCHAR(1)
	,is_correct						VARCHAR(1)
);

CREATE SCHEMA IF NOT EXISTS logs;
DROP TABLE IF EXISTS logs.load_log;
CREATE TABLE logs.load_log(
	 id								SERIAL8
	,actions						CHAR(30)
	,objects						TEXT
	,action_date					TIMESTAMP
);

