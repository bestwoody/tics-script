CREATE TABLE IF NOT EXISTS nation (
	 N_NATIONKEY  UInt8,
         N_NAME       String,
         N_REGIONKEY  UInt8,
         N_COMMENT    String
)ENGINE = Log;
