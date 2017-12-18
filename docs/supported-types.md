# Supported types

## NOTE: NOT cover all base types yet.

## Current Supported
* String
* DateTime
* Date
* Int8
* Int16
* Int32
* Int64
* UInt8
* UInt16
* UInt32
* UInt64
* Float32
* Float64

## TPCH Need
* Int(32)
* String
* Double
* Date

## Type difference from CH, Arrow and Spark
* All supported
    * String
    * Int8
    * Int16
    * Int32
    * Int64
    * Float32
    * Float64
* All supported but Spark
    * UInt8
    * UInt16
    * UInt32
    * UInt64
* Arrow specific
    * Bool: TODO
    * Binary: TODO
    * FixedSizeBinary: TODO
    * HalfFloat: store as UInt16
    * Time32: store as Int32
    * Time64: store as Int64
    * Date32: store as Int32
    * Date64: store as Int64
    * Timestamp: TODO
    * Interval: TODO
    * TODO: (Time32 vs Date32 vs Timestamp ?)
* CH specific
    * Bool: use UInt8 instead
    * FixedString
    * Date: store as UInt16, no timezone info
    * DateTime: store as UInt32, use system timezone
* Spark specific
    * Boolean
    * Binary
    * Decimal
    * BigDecimal
    * Date
    * Timestamp

## From CH types to Spark types, convertion:
* `CH => Spark`:
    * `String => StringType`
    * `FixedString(n) => StringType`
    * `DateTime => TimestampType`, promoted
    * `Date => TimestampType`, promoted
        * Since we convert both `DateTime` and `Date` to `TimstampType`, this will happend:
            * `CREATE TABLE T (D1 DateTime, D2 Date)`
            * `scala> spark.sql("DESC T")`: D1 Timestamp, D2 Timestamp
            * `scala> spark.sql("SELECT * FROM T WHERE D1 < '1980-01-01'")`: OK
            * `scala> spark.sql("SELECT * FROM T WHERE D1 < '1980-01-01 00:00:00'")`: OK
            * `scala> spark.sql("SELECT * FROM T WHERE D2 < '1980-01-01'")`: OK
            * `scala> spark.sql("SELECT * FROM T WHERE D2 < '1980-01-01 00:00:00'")`: ERROR
    * `Int8 => ByteType`
    * `Int16 => ShortType`
    * `Int32 => IntegerType`
    * `Int64 => LongType`
    * `UInt8 => IntegerType`, promoted
    * `UInt16 => IntegerType`, promoted
    * `UInt32 => LongType`, promoted
    * `UInt64 => LongType`, may overflow, unchecked
    * `Float32 => FloatType`
    * `Float64 => DoubleType`
