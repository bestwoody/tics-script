SELECT
	C_COUNT,
	COUNT(*) AS CUSTDIST
FROM
	(
	SELECT
		C_CUSTKEY,
		COUNT(O_ORDERKEY) AS C_COUNT
	FROM
		customer2 LEFT OUTER JOIN orders2 ON
		C_CUSTKEY = O_CUSTKEY
		AND O_COMMENT NOT LIKE '%PENDING%DEPOSITS%'
	GROUP BY
		C_CUSTKEY
	)	C_ORDERS
GROUP BY
	C_COUNT
ORDER BY
	CUSTDIST DESC,
	C_COUNT DESC
