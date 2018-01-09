SELECT
	PS_PARTKEY,
	SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
FROM
	partsupp,
	supplier,
	nation
WHERE
	PS_SUPPKEY = S_SUPPKEY
	AND S_NATIONKEY = N_NATIONKEY
	AND N_NAME = 'GERMANY'
GROUP BY
	PS_PARTKEY HAVING
	SUM(PS_SUPPLYCOST * PS_AVAILQTY) >
	(
	SELECT
		SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000
	FROM
		partsupp,
		supplier,
		nation
	WHERE
		PS_SUPPKEY = S_SUPPKEY
		AND S_NATIONKEY = N_NATIONKEY
		AND N_NAME = 'GERMANY'
	)
ORDER BY
	VALUE DESC
