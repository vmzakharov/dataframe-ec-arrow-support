# dataframe-ec-arrow-support

## Overview
Apache Arrow support - reading and writing [dataframe-ec](https://github.com/vmzakharov/dataframe-ec) data frames to and from Apache Arrow table format

**NOTE** This implementation is a proof-of-concept

## Supported Types 
The following data frame column types are supported for serializing data frames to/from Arrow:

| Data Frame Column Type | Java Type     | Arrow Type            | Arrow Vector    |
|------------------------|---------------|-----------------------|-----------------|
| STRING                 | String        | Utf8                  | VarCharVector   |
| INT                    | int           | Int(32)               | IntVector       |
| LONG                   | long          | Int(64)               | BigIntVector    |
| FLOAT                  | float         | FloatingPoint(SINGLE) | Float4Vector    |
| DOUBLE                 | double        | FloatingPoint(DOUBLE) | Float8Vector    |
| DECIMAL                | BigDecimal    | Decimal               | DecimalVector   |
| DATE                   | LocalDate     | Date(DAY)             | DateDayVector   |
| DATE_TIME              | LocalDateTime | Date(MILLISECOND)     | DateMilliVector |
| BOOLEAN                | boolean       | Types.MinorType.BIT   | BitVector       |

Reading and writing `null` values is supported for all the types listed here.
