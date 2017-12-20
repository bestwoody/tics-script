SELECT
	O_ORDERPRIORITY,
	COUNT(*) AS ORDER_COUNT
FROM
	orders2
WHERE
	O_ORDERDATE >= '1990-07-01'
	AND O_ORDERDATE < '1993-10-01'
	AND EXISTS (
		SELECT
			*
		FROM
			lineitem2
		WHERE
			L_ORDERKEY = O_ORDERKEY
			AND L_COMMITDATE < L_RECEIPTDATE
		)
GROUP BY
	O_ORDERPRIORITY
ORDER BY
	O_ORDERPRIORITY
