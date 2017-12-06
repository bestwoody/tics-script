/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.ch.mock;

import static java.util.Arrays.asList;

import org.apache.arrow.vector.types.FloatingPointPrecision;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;


public class BuildSchema {
    private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
        return new Field(name, new FieldType(nullable, type, null, null), asList(children));
    }

    private static Field field(String name, ArrowType type, Field... children) {
        return field(name, true, type, children);
    }
    public static Schema typesTestSchema() {
        Schema schema = new Schema(asList(
            field("string", new Utf8()),
            field("int8", new Int(8,true)),
            field("int6", new Int(16,true)),
            field("int32", new Int(32,true)),
            field("int64", new Int(64,true)),
            field("float32", new FloatingPoint(FloatingPointPrecision.SINGLE)),
            field("float64", new FloatingPoint(FloatingPointPrecision.DOUBLE))
        ));
        return schema;
    }
}
