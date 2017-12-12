CREATE TABLE IF NOT EXISTS part( 
	P_PARTKEY     UInt8,
        P_NAME        String,
        P_MFGR        String,
        P_BRAND       String,
        P_TYPE        String,
        P_SIZE        UInt8,
        P_CONTAINER   String,
        P_RETAILPRICE Float64,
        P_COMMENT     String
)ENGINE = Log;

