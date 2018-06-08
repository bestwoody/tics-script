# The polymorphic timing difference between rowwise and columnwise

## Prefix meaning
* T: definition of template interface
* I: difinition of vtable interface
* P: pointer
* No prefix: no polymorphism, simple struct or class

## Base structs
```
Schema
    DataType * N
    Name * N

IBlock
    Schema

RowwiseBlock:IBlock
    Row * N
        IValue

ColumnwiseBlock:IBlock
    IColumn * N
        Value
```

## Key operations path
* Op
    * Serialize
    * Deserialize
    * Compare
    * Cast
```
RowwiseBlock.Op()
    Row.Op()
        IValue.Op()

ColumnwiseBlock.Op()
    IColumn.Op()
        Value.Op()
```

## Conclusion
* Since schema is indefinite/various
    * Rowwise struct need polymorphism in any operation on any value
    * Columnwise struct need polymorphism in any operation on any column, but not on any value
    * Columnwise is faster. (ref: benchmark below)
        * A lot less vptr cost
        * More changes for compiler optimization
* Columnwise struct should be used in memory
* Since serialize/deserialize is operation too, coloumnwise struct should be used in disk

## CH implement
* Structs
```
Block
    IColumn * N
        Value

ColumnImplements:IColumn

Field
    DataType
    Bytes
```
* Value access
    * `IColumn.Op()`: vtable interface
    * `(typeid_cast<ColumnImplements*>(IColumn)).Op()`:
        * Normal class interface
        * Most usage method
    * `Field value = IColumn[i]; value.Op<Type>()`:
        * Normal class interface + template class interface
        * Is used in rare cases

## Kudu implement
```
ColumnwiseRowSet
    CFileSet
        ColumnBlock * N
            Cell * N

RowwiseRowSet
    RowBlock * N
        RowBlockRow * N
            Cell * N
```
* Value access
    * `byte *value = Block.Cell(i).Ptr()`: normal class interface
    * Basically all operation are base on bytes, except input/output (AKA: insert/scan)
    * Kudu client value access in Impala/Spark:
        * `result.get<Type>()`
        * Rowwise, need to be branched by `case schema.Type`, it's slow


## Simple VTable benchmark result
|    OS |  Compiler | Optimization |     Class |    VTable |
| ----- | --------- | -----------: | --------: | --------: |
| Mac   | clang-902 |          -O3 |  0m0.432s |  0m1.672s |
| Mac   | clang-902 |          -O2 |  0m0.433s |  0m1.680s |
| Mac   | clang-902 |          -O1 |  0m2.203s |  0m1.684s |
| Mac   | clang-902 |          -O0 |  0m3.660s |  0m4.256s |
| Linux | g++ 7.2.0 |          -O3 |  0m1.196s |  0m7.708s |
| Linux | g++ 7.2.0 |          -O2 |  0m1.270s |  0m8.243s |
| Linux | g++ 7.2.0 |          -O1 |  0m1.904s |  0m8.517s |
| Linux | g++ 7.2.0 |          -O0 |  0m5.047s | 0m14.348s |

* It's a very simple, not strict benchmark.
* TODO: detail analysis
* Compiled code of `Class` version, auto SIMD:
```
Main loop:
100000e89:    0f 1f 80 00 00 00 00     nopl   0x0(%rax)
100000e90:    f2 0f 58 ca              addsd  %xmm2,%xmm1
100000e94:    f2 0f 58 c3              addsd  %xmm3,%xmm0
100000e98:    48 83 c0 fe              add    $0xfffffffffffffffe,%rax
100000e9c:    75 f2                    jne    100000e90 <__Z11test_normalv+0x30>
100000e9e:    48 8d 3d ee 00 00 00     lea    0xee(%rip),%rdi        # 100000f93 <__ZTS2T2+0x4>
100000ea5:    b0 02                    mov    $0x2,%al
100000ea7:    5d                       pop    %rbp
100000ea8:    e9 97 00 00 00           jmpq   100000f44 <_main+0x54>
```

## Benchmark code
```
#include <iostream>

struct T {
    T(double v_) : v(v_)
    {}
    virtual void increase() = 0;
    double v;
};

struct T1 : public T {
    T1(double v) : T(v) {}
    virtual void increase() {
        v += 0.1;
    }
};

struct T2 : public T {
    T2(double v) : T(v) {}
    virtual void increase() {
        v += 0.2;
    }
};

void test_vtable() {
    T1 t1(0.5);
    T2 t2(1);
    T *r = &t1;
    for (size_t i = 0; i < 1e9; ++i)
    {
        r = (r == (T*)&t1) ? (T*)&t2 : (T*)&t1;
        r->increase();
    }
    std::cout << t1.v << ", " << t2.v << std::endl;
}

struct N {
    N(double v_) : v(v_)
    {}
    double v;
};

struct N1 : public N {
    N1(double v) : N(v) {}
    virtual void increase() {
        v += 0.1;
    }
};

struct N2 : public N {
    N2(double v) : N(v) {}
    virtual void increase() {
        v += 0.2;
    }
};

void test_normal() {
    T1 t1(0.5);
    T2 t2(1);
    bool use_t1 = true;
    for (size_t i = 0; i < 1e9; ++i)
    {
        use_t1 = (!use_t1);
        if (use_t1)
            t1.increase();
        else
            t2.increase();
    }
    std::cout << t1.v << ", " << t2.v << std::endl;
}

int main() {
    // test_vtable();
    test_normal();
    return 0;
}
```
