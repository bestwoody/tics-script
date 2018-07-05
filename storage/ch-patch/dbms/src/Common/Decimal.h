#pragma once
#include<boost/multiprecision/cpp_int.hpp>
#include <Common/Exception.h>

namespace DB {

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW_ERROR;
}

using int256_t = boost::multiprecision::int256_t;
using int512_t = boost::multiprecision::int512_t;

constexpr int decimal_max_prec = 65;
constexpr int decimal_max_scale = 30;

using PrecType = uint16_t;
using ScaleType = uint8_t;

template<typename T> struct IntPrec{};
template<> struct IntPrec<int8_t>{
    static const PrecType prec = 3;
};
template<> struct IntPrec<uint8_t>{
    static const PrecType prec = 3;
};
template<> struct IntPrec<int16_t>{
    static const PrecType prec = 5;
};
template<> struct IntPrec<uint16_t>{
    static const PrecType prec = 5;
};
template<> struct IntPrec<int32_t>{
    static const PrecType prec = 10;
};
template<> struct IntPrec<uint32_t>{
    static const PrecType prec = 10;
};
template<> struct IntPrec<int64_t>{
    static const PrecType prec = 20;
};
template<> struct IntPrec<uint64_t>{
    static const PrecType prec = 20;
};

//  1) If the declared type of both operands of a dyadic arithmetic operator is exact numeric, then the declared
//  type of the result is an implementation-defined exact numeric type, with precision and scale determined as
//  follows:
//    a) Let S1 and S2 be the scale of the first and second operands respectively.
//    b) The precision of the result of addition and subtraction is implementation-defined, and the scale is the
//       maximum of S1 and S2.
//    c) The precision of the result of multiplication is implementation-defined, and the scale is S1 + S2.
//    d) The precision and scale of the result of division are implementation-defined.

struct InfererHelper {
    template<typename T>
    static T min(T A, T B) {
        return A < B ? A: B;
    }

    template<typename T>
    static T max(T A, T B) {
        return A > B ? A: B;
    }
};

struct PlusDecimalInferer : public InfererHelper {
    static inline void infer(PrecType left_prec, ScaleType left_scale, PrecType right_prec, ScaleType right_scale, PrecType& result_prec, ScaleType& result_scale) {
        result_scale = max(left_scale, right_scale);
        PrecType result_int = max(left_prec - left_scale, right_prec - right_scale);
        result_prec = min(result_scale + result_int + 1, decimal_max_prec);
    }
};

struct MulDecimalInferer : public InfererHelper {
    static inline void infer(PrecType left_prec, ScaleType left_scale, PrecType right_prec, ScaleType right_scale, PrecType& result_prec, ScaleType& result_scale) {
        result_scale = min(left_scale + right_scale, decimal_max_scale);
        result_prec = min(left_prec + right_prec, decimal_max_prec);
    }
};

struct DivDecimalInferer : public InfererHelper {
    static const uint8_t div_precincrement = 4;
    static inline void infer(PrecType left_prec, ScaleType left_scale, PrecType /* right_prec is not used */ , ScaleType right_scale, PrecType& result_prec, ScaleType& result_scale) {
        result_prec = min(left_prec + right_scale + div_precincrement, decimal_max_prec);
        result_scale = min(left_scale + div_precincrement, decimal_max_scale);
    }
};

struct SumDecimalInferer : public InfererHelper {
    static constexpr uint8_t decimal_longlong_digits = 22;
    static inline void infer(PrecType prec, ScaleType scale, PrecType &result_prec, ScaleType &result_scale) {
        result_prec = min(prec + decimal_longlong_digits, decimal_max_prec);
        result_scale = scale;
    }
};

struct AvgDecimalInferer : public InfererHelper {
    static const uint8_t div_precincrement = 4;
    static inline void infer(PrecType left_prec, ScaleType left_scale, PrecType& result_prec, ScaleType& result_scale) {
        result_prec = min(left_prec + div_precincrement, decimal_max_prec);
        result_scale = min(left_scale + div_precincrement, decimal_max_scale);
    }
};

struct OtherInferer {
    static inline void infer(PrecType, ScaleType , PrecType , ScaleType, PrecType&, ScaleType&) {}
};

// TODO use template to make type of value arguable.
struct DecimalValue {
    int256_t value;
    PrecType precision;
    ScaleType scale;

    DecimalValue(const DecimalValue& d): value(d.value), precision(d.precision), scale(d.scale) {}
    DecimalValue():value(0), precision(0), scale(0) {}
    DecimalValue(int256_t v_, PrecType prec_, ScaleType scale_): value(v_), precision(prec_), scale(scale_){}

    template<typename T, std::enable_if_t<std::is_integral<T>{}>* = nullptr >
    DecimalValue(T v): value(v), precision(IntPrec<T>::prec), scale(0) {}

    // check if DecimalValue is inited without any change.
    bool isZero() const {
        return precision == 0 && scale == 0;
    }

    DecimalValue operator + (const DecimalValue& v) const ;

    void operator += (const DecimalValue& v) ;

    void operator = (const DecimalValue& v) {
        value = v.value;
        precision = v.precision;
        scale = v.scale;
    }

    DecimalValue operator - (const DecimalValue& v) const ;

    DecimalValue operator - () const ;

    DecimalValue operator ~ () const ;

    DecimalValue operator * (const DecimalValue& v) const ;

    DecimalValue operator / (const DecimalValue& v) const ;

    template <typename T, std::enable_if_t<std::is_floating_point<T>{}>* = nullptr>
    operator T () const {
        T result = static_cast<T> (value);
        for (ScaleType i = 0; i < scale; i++) {
            result /= 10;
        }
        return result;
    }

    template <typename T, std::enable_if_t<std::is_integral<T>{}>* = nullptr>
    operator T () const {
        int256_t v = value;
        for (ScaleType i = 0; i < scale; i++) {
            v = v / 10 + (i + 1 == scale && v % 10 >= 5);
        }
        if (value > std::numeric_limits<T>::max() || value < std::numeric_limits<T>::min()) {
            throw Exception("Decimal value overflow", ErrorCodes::DECIMAL_OVERFLOW_ERROR);
        }
        T result = static_cast<T>(v);
        return result;
    }

    bool operator < (const DecimalValue& v) const;

    bool operator <= (const DecimalValue& v) const;

    bool operator == (const DecimalValue& v) const;

    bool operator >= (const DecimalValue& v) const;

    bool operator > (const DecimalValue& v) const;

    bool operator != (const DecimalValue& v) const;

    void checkOverflow() const;

    std::string toString() const;

    DecimalValue getAvg(uint64_t cnt, PrecType result_prec, ScaleType result_scale) const {
        auto tmpValue = value;
        if (result_scale > scale) {
            for (ScaleType i = 0; i < result_scale - scale; i++) {
                tmpValue *= 10;
            }
        }
        tmpValue /= cnt;
        DecimalValue dec(tmpValue, result_prec, result_scale);
        dec.checkOverflow();
        return dec;
    }
};

template <typename DataType> constexpr bool IsDecimalValue = false;
template <> constexpr bool IsDecimalValue<DecimalValue> = true;

}
