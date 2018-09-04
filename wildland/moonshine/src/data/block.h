#pragma once

#include <vector>
#include <ostream>
#include <functional>

#include "data/sorting.h"

namespace moonshine {

using std::vector;
using std::ostream;
using std::endl;
using std::function;

struct ColumnWithTypeAndName {
    ColumnPtr column;
    TypeAndName info;

    ColumnWithTypeAndName() {}
    ColumnWithTypeAndName(const ColumnPtr &column_, const TypeAndName &info_) : column(column_), info(info_) {}

    operator bool() const {
        return bool(column);
    }
};

class Block : public vector<ColumnPtr> {
public:
    void CreateColumnsBySchema(const Schema &schema) {
        resize(schema.size());
        for (size_t i = 0; i < size(); ++i) {
            ColumnPtr &column = (*this)[i];
            column = schema[i].type->CreateColumn();
        }
    }

    void FillRand(const Schema &schema, size_t rows) {
        CreateColumnsBySchema(schema);
        for (ColumnPtr &column: *this) {
            column->FillRand(rows);
        }
        this->rows = rows;
    }

    size_t Rows() const {
        return rows;
    }

    // TODO: enlarge + shrink
    void Resize(size_t new_size) {
        // TODO: check all columns' length
        //if (new_size > rows)
        //    throw ErrWrongUsage("can't enlarge a block");
        rows = new_size;
    }

    void SortRows(const SortDesc &sorting) {
        throw ErrNoImpl("Block.SortRows");
    }

    void SortRowsByPK() {
        if (!encoded_pk)
            return;
        Permutation perm;
        encoded_pk.column->GetPermutation(perm, rows);
        for (ColumnPtr &column: *this)
            column->ApplyPermutation(perm);
    }

    void GenEncodedPKColumn(const SortedSchema &schema) {
        if (encoded_pk)
            return;
        if (schema.pk_column_count == 0)
            return;
        if (schema.pk_column_count == 1 && schema[0].type->is_numberic) {
            encoded_pk = ColumnWithTypeAndName((*this)[0], schema[0]);
            return;
        }

        // TODO: If all pk are numberic and total size <= 64, use simple shifting as hash
        // TODO: use the algorithm that kudu used to Calculate pk hash and keep pk order
        throw ErrNoImpl("Block.GenEncodedPKColumn");
    }

    ColumnWithTypeAndName GetEncodedPKColumn() const {
        return encoded_pk;
    }

    void DebugPrintByColumn(ostream &w, const Schema &schema) const {
        w << "[block rows] " << Rows() << endl;
        for (size_t i = 0; i < size(); ++i) {
            auto &column = (*this)[i];
            auto &type_and_name = schema[i];
            w << "[column] " << type_and_name.name << "(" << type_and_name.type->name << ") #" << i << endl;
            for (size_t j = 0; j < Rows(); ++j) {
                column->DebugPrint(w, j);
                if (j != Rows() - 1)
                    w << ", ";
            }
            w << endl;
        }
    }

    void DebugPrintByRow(ostream &w) const {
        w << "[block rows] " << Rows() << endl;
        for (size_t i = 0; i < Rows(); ++i) {
            for (size_t j = 0; j < size(); ++j) {
                auto &column = (*this)[j];
                column->DebugPrint(w, i);
                if (j != size() - 1)
                    w << ", ";
            }
            w << endl;
        }
    }

    friend ostream & operator << (ostream &w, const Block &block) {
        block.DebugPrintByRow(w);
        return w;
    }

private:
    size_t rows = 0;
    ColumnWithTypeAndName encoded_pk;
};

}
