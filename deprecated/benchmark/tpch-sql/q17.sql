SELECT
	SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM
	lineitem,
	part
WHERE
	P_PARTKEY = L_PARTKEY
	AND P_BRAND = 'Brand#23'
	AND P_CONTAINER = 'MED BAG'
	AND L_QUANTITY < (
		SELECT
			0.2 * AVG(L_QUANTITY)
		FROM
			lineitem
		WHERE
			L_PARTKEY = P_PARTKEY
	)