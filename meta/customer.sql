CREATE TABLE IF NOT EXISTS customer( 
	C_CUSTKEY     UInt8,
	C_NAME        String,
	C_ADDRESS     String,
	C_NATIONKEY   UInt8,
	C_PHONE       String,
	C_ACCTBAL     Float64,
	C_MKTSEGMENT  String,
	C_COMMENT     String
	)ENGINE = Log;

