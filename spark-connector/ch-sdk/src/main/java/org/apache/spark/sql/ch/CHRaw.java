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

package org.apache.spark.sql.ch;

import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class CHRaw {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    private static void dump(Field field, int order) {
        System.out.println("    #" + order + " name:\"" + field.getName() + "\" type:" +
                field.getType().getTypeID() + " nullable:" + field.isNullable());
    }

    private static void dump(CHResponse result, boolean decode) throws Exception {
        Schema schema = result.getSchema();
        List<Field> fields = schema.getFields();
        int i = 0;
        if (decode) {
            System.out.println("[schema]");
            for (Field field: fields) {
                dump(field, i);
                i += 1;
            }
        }

        while (result.hasNext()) {
            VectorSchemaRoot block = result.next();
            if (block == null) {
                break;
            }
            if (!decode) {
                continue;
            }
            System.out.println("[result]");

            List<FieldVector> columns = block.getFieldVectors();
            int j = 0;
            for (FieldVector column: columns) {
                Field field = column.getField();
                ArrowType.ArrowTypeID type = field.getType().getTypeID();
                dump(field, j);
                ValueVector.Accessor acc = column.getAccessor();
                for (int k = 0; k < acc.getValueCount(); ++k) {
                    Object v = acc.getObject(k);
                    if (v instanceof Character) {
                        System.out.println("    " + (int)(Character)v);
                    } else if (type == ArrowType.ArrowTypeID.Time) {
                        long ts = (Integer)v;
                        //System.out.println("    " + sdf.format(new Date(ts * 1000)));
                        System.out.println("    " + ts);
                    } else {
                        System.out.println("    " + acc.getObject(k).toString());
                    }
                }
                j += 1;
            }

            System.out.println("    ---");
        }

        System.out.println("[query done]");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: <bin> query 'decode' ch-host [port]");
            System.exit(-1);
        }

        String query = args[0];
        boolean decode = Boolean.parseBoolean(args[1]);
        String host = args[2];
        int port = 9001;
        if (args.length > 3) {
            port = Integer.parseInt(args[3]);
        }

        CHResponse result = new CHResponse(query, host, port, null);
        dump(result, decode);
        result.close();
    }
}