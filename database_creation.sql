USE xlocal;

DROP TABLE IF EXISTS cryptocurrencies;

CREATE TABLE IF NOT EXISTS cryptocurrencies(
	currency_rank	INT(3)	NOT NULL,
	symbol	CHAR(5)	NOT NULL,
	name	VARCHAR(15) NOT NULL,
	market_value	VARCHAR(15) NOT NULL,
	prices 	INT(6) NOT NULL,
	date_of_ingest	DATETIME NOT NULL
)

