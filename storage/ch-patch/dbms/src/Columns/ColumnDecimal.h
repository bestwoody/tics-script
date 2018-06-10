#pragma once

#include <Columns/ColumnVector.h>
#include <Common/Decimal.h>

namespace DB
{
    using ColumnDecimal = ColumnVector<DecimalValue>;
}
