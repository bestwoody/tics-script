SELECT
	NATION,
	O_YEAR,
	SUM(AMOUNT) AS SUM_PROFIT
FROM
	(
	SELECT
		N_NAME AS NATION,
		YEAR(O_ORDERDATE) AS O_YEAR,
		L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
	FROM
		part,
		supplier,
		lineitem,
		partsupp,
		orders,
		nation
	WHERE
		S_SUPPKEY = L_SUPPKEY
		AND PS_SUPPKEY = L_SUPPKEY
		AND PS_PARTKEY = L_PARTKEY
		AND P_PARTKEY = L_PARTKEY
		AND O_ORDERKEY = L_ORDERKEY
		AND S_NATIONKEY = N_NATIONKEY
		AND P_NAME LIKE '%dim%'
	) AS PROFIT
GROUP BY
	NATION,
	O_YEAR
ORDER BY
	NATION,
	O_YEAR DESC
