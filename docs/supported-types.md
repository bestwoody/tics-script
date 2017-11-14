# Supported types

## NOTE: NOT cover all base types yet.

## Current Supported
* String
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
* Int
* String
* Double
* Date

## Type deferrence from CH, Arrow and Spark
* All supported
    * String
    * Int8
    * Int16
    * Int32
    * Int64
* All supported but Spark
    * UInt8
    * UInt16
    * UInt32
    * UInt64
    * Float32
    * Float64
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
