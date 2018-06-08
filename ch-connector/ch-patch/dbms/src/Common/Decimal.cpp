#include "Common/Decimal.h"

namespace DB {

class DecimalMaxValue {
    int256_t number[decimal_max_prec+1];

public:
    DecimalMaxValue() {
        for (int i = 1; i <= decimal_max_prec; i++) {
            number[i] = number[i-1] * 10 + 9;
        }
    }

    int256_t operator [](uint8_t idx) const {
        return number[idx];
    }
} decimalMaxValues;

DecimalValue DecimalValue::operator + (const DecimalValue & v) const {
    uint8_t result_scale;
    uint8_t result_prec;
    PlusDecimalInferer::infer(precision, scale, v.precision, v.scale, result_prec, result_scale);
    int256_t value_a = value, value_b = v.value;
    for (uint8_t s = scale; s < result_scale; s++){
        value_a *= 10;
    }
    for (uint8_t s = v.scale; s < result_scale; s++){
        value_b *= 10;
    }
    return DecimalValue(value_a + value_b, result_prec, result_scale);
}

void DecimalValue::operator += (const DecimalValue & v) {
    if (precision == 0) {
        *this = v;
    } 
    else if (precision == v.precision && scale == v.scale)
    {
        value = value + v.value;
        if(abs(value) > decimalMaxValues[precision]) {
            throw Exception("overflow!");
        }
    } else {
        *this = *this + v;
    }
}

DecimalValue DecimalValue::operator - (const DecimalValue & v) const {
    DecimalValue tmp = v;
    tmp.value = -tmp.value;
    return (*this) + tmp;
}

DecimalValue DecimalValue::operator - () const {
    return DecimalValue(-value, precision, scale);
}

DecimalValue DecimalValue::operator ~ () const {
    return DecimalValue(~value, precision, scale);
}

DecimalValue DecimalValue::operator * (const DecimalValue & v) const {
    uint8_t result_scale;
    uint8_t result_prec;
    MulDecimalInferer::infer(precision, scale, v.precision, v.scale, result_prec, result_scale);
    int256_t result_value = value * v.value;
    return DecimalValue(result_value, result_prec, result_scale);
}

DecimalValue DecimalValue::operator / (const DecimalValue & v) const {
    uint8_t result_scale;
    uint8_t result_prec;
    DivDecimalInferer::infer(precision, scale, v.precision, v.scale, result_prec, result_scale);
    int256_t result_v = value;
    for (uint8_t i = 0; i < v.scale + DivDecimalInferer::div_precincrement; i++)
        result_v *= 10;
    result_v /= v.value;
    return DecimalValue(result_v, result_prec, result_scale);
}

std::string DecimalValue::toString() const 
{
    char str[decimal_max_prec + 5];
    size_t len = precision;
    if (value < 0) { // extra space for sign
        len ++;
    }
    if (scale > 0) { // for factional point
        len ++; 
    }
    if (scale == len) { // for leading zero
        len ++;
    }
    size_t end_point = len;
    int256_t cur_v = value;
    if (value < 0) {
        cur_v = -cur_v;
    }
    if (scale > 0) {
        for (size_t i = 0; i < scale; i++)
        {
            int d = static_cast<int>(cur_v % 10);
            cur_v = cur_v / 10;
            str[--len] = d + '0';
        }
        str[--len] = '.';
    }
    do {
        int d = static_cast<int>(cur_v % 10);
        cur_v = cur_v / 10;
        str[--len] = d + '0';
    } while(cur_v > 0);
    if (value < 0) {
        str[--len] = '-';
    }
    return std::string(str + len, end_point - len);
}

enum cmpResult {
    gt = 0,
    eq = 1,
    ls = 2,
};

inline cmpResult scaleAndCompare(const DecimalValue & v1, const DecimalValue & v2) {
    int256_t nv = v1.value;
    for (uint8_t i = v1.scale; i < v2.scale; i++) {
        nv = nv * 10;
        if (nv > v2.value) {
            return cmpResult::gt;
        }
    }
    return nv < v2.value ? cmpResult::ls : cmpResult::eq ;
}

bool DecimalValue::operator == (const DecimalValue & v) const {
    if (scale == v.scale) {
        return value == v.value;
    } else if (scale < v.scale) {
        cmpResult comp = scaleAndCompare(*this, v);
        return comp == cmpResult::eq;
    } else {
        cmpResult comp = scaleAndCompare(v, *this);
        return comp == cmpResult::eq;
    }
}

bool DecimalValue::operator < (const DecimalValue & v) const {
    if (scale == v.scale) {
        return value < v.value;
    } else if (scale < v.scale) {
        cmpResult comp = scaleAndCompare(*this, v);
        return comp == cmpResult::ls;
    } else {
        cmpResult comp = scaleAndCompare(v, *this);
        return comp == cmpResult::gt;
    }
}

bool DecimalValue::operator != (const DecimalValue & v) const {
    return !(*this == v);
}

bool DecimalValue::operator >= (const DecimalValue & v) const {
    return !(*this < v);
}

bool DecimalValue::operator <= (const DecimalValue & v) const {
    return !(*this > v);
}

bool DecimalValue::operator > (const DecimalValue & v) const {
    return v < *this;
}

// end namespace
}
