#pragma once
#include<boost/multiprecision/cpp_int.hpp>
#include <Common/Exception.h>

namespace DB {

using int256_t = boost::multiprecision::int256_t;
using int512_t = boost::multiprecision::int512_t;

constexpr int decimal_max_prec = 65;

template<typename T>
T getScaleMultiplier(uint8_t precision) {
    T mx(1);
    for(auto i = 0; i < precision; i++) {
        mx *= 10;
    }
    return mx;
}

template<typename T>
T max(T A, T B) {
    return A > B ? A: B;
}

template<typename T>
T min(T A, T B) {
    return A < B ? A: B;
}

template<typename T> struct IntPrec{};
template<> struct IntPrec<int8_t>{
    static const uint8_t prec = 3;
};
template<> struct IntPrec<uint8_t>{
    static const uint8_t prec = 3;
};
template<> struct IntPrec<int16_t>{
    static const uint8_t prec = 5;
};
template<> struct IntPrec<uint16_t>{
    static const uint8_t prec = 5;
};
template<> struct IntPrec<int32_t>{
    static const uint8_t prec = 10;
};
template<> struct IntPrec<uint32_t>{
    static const uint8_t prec = 10;
};
template<> struct IntPrec<int64_t>{
    static const uint8_t prec = 20;
};
template<> struct IntPrec<uint64_t>{
    static const uint8_t prec = 20;
};

struct PlusDecimalInferer {
    static inline void infer(uint8_t left_prec, uint8_t left_scale, uint8_t right_prec, uint8_t right_scale, uint8_t& result_prec, uint8_t& result_scale) {
        result_scale = max(left_scale, right_scale);
        uint8_t result_int = max(left_prec - left_scale, right_prec - right_scale);
        result_prec = result_scale + result_int + 1;
    }
};

struct MulDecimalInferer {
    static inline void infer(uint8_t left_prec, uint8_t left_scale, uint8_t right_prec, uint8_t right_scale, uint8_t& result_prec, uint8_t& result_scale) {
         result_scale = left_scale + right_scale;
        if (result_scale > decimal_max_prec) {
            throw Exception("overflow!");
        }
        result_prec = min(left_prec + right_prec, decimal_max_prec);
    }
};

struct DivDecimalInferer {
    static const uint8_t div_precincrement = 4;
    static inline void infer(uint8_t left_prec, uint8_t left_scale, uint8_t , uint8_t right_scale, uint8_t& result_prec, uint8_t& result_scale) {
        result_prec = left_prec + right_scale + div_precincrement;
        result_scale = left_scale + div_precincrement;
    }
};

struct OtherInferer {
    static inline void infer(uint8_t, uint8_t , uint8_t , uint8_t, uint8_t&, uint8_t&) {}
};

// TODO use template to make type of value arguable.
struct DecimalValue {
    int256_t value;
    uint8_t precision;
    uint8_t scale;

    DecimalValue(const DecimalValue& d): value(d.value), precision(d.precision), scale(d.scale) {}
    DecimalValue():value(0), precision(0), scale(0) {}
    DecimalValue(int256_t v_, uint8_t prec_, uint8_t scale_): value(v_), precision(prec_), scale(scale_){}
    DecimalValue(double v, uint8_t scale_) {
        scale = scale_;
        v *= getScaleMultiplier<double>(scale);
        value = int256_t(v);
    }

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
    operator T () const ;

    template <typename T, std::enable_if_t<std::is_integral<T>{}>* = nullptr>
    operator T () const ;

    bool operator < (const DecimalValue& v) const;

    bool operator <= (const DecimalValue& v) const;

    bool operator == (const DecimalValue& v) const;

    bool operator >= (const DecimalValue& v) const;

    bool operator > (const DecimalValue& v) const;

    bool operator != (const DecimalValue& v) const;

    std::string toString() const;
};

template <typename T, std::enable_if_t<std::is_floating_point<T>{}>* = nullptr>
DecimalValue::operator T() const {
    T result = static_cast<T> (value);
    for (uint8_t i = 0; i < scale; i++) {
        result /= 10;
    }
    return result;
}

template <typename T, std::enable_if_t<std::is_integral<T>{}>* = nullptr>
DecimalValue::operator T() const {
    int256_t v = value;
    for (uint8_t i = 0; i < scale; i++) {
        v = v / 10 + (i + 1 == scale && v % 10 >= 5);
    }
    if (value > std::numeric_limits<T>::max() || value < std::numeric_limits<T>::min()) {
        throw Exception("overflow!");
    }
    T result = static_cast<T>(v);
    return result;
}

template <typename DataType> constexpr bool IsDecimalValue = false;
template <> constexpr bool IsDecimalValue<DecimalValue> = true;

}
