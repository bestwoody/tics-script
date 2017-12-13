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
import java.util.Scanner;
import java.sql.Timestamp;
import java.io.File;
import java.io.PrintStream;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class CHRaw {
    private static void dump(Field field, int order) {
        System.out.println("    #" + order + " name:\"" + field.getName() + "\" type:" +
                field.getType().getTypeID() + " nullable:" + field.isNullable());
    }

    private static void dump(CHExecutor executor, boolean decode) throws Exception {
        Schema schema = executor.getSchema();
        List<Field> fields = schema.getFields();
        int i = 0;
        if (decode) {
            System.out.println("[schema]");
            for (Field field: fields) {
                dump(field, i);
                i += 1;
            }
        }

        while (executor.hasNext()) {
            CHExecutor.Result block = executor.decode(executor.next());
            if (block.isEmpty()) {
                break;
            }
            if (block.error != null) {
                throw new Exception(block.error);
            }
            if (!decode) {
                System.out.println("[fetched block]");
                continue;
            }
            System.out.println("[result]");

            List<FieldVector> columns = block.block.getFieldVectors();
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
                        System.out.println("    " + new Timestamp((Long)v * 1000));
                    } else {
                        System.out.println("    " + v.toString());
                    }
                }
                j += 1;
            }

            System.out.println("    ---");
        }

        System.out.println("[query done]");
    }

    private void exec(String query) throws Exception {
        CHExecutor result = new CHExecutor(query, host, port);
        dump(result, decode);
        result.close();
    }

    public int loop(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: <bin> ch-host cli|query|querys");
            return -1;
        }

        String host = args[0];
        int port = 9006;

        String cmd = args[1];

        if (cmd.equals("cli")) {
            System.out.println("[intereact mode, type 'quit' to exit]");
            while (true) {
                Scanner reader = new Scanner(System.in);
                String line = reader.nextLine();
                if (line.equals("help")) {
                    System.out.println("[usage: help|quit|to-log|no-decode|bench]");
                    continue;
                } else if (line.equals("quit")) {
                    return 0;
                } else if (line.equals("to-log")) {
                    System.out.println("[redirecting output to chraw-java.log]");
                    System.setOut(new PrintStream(new File("chraw-java.log")));
                    continue;
                } else if (line.equals("no-decode")) {
                    System.out.println("[throughput bench mode, disable data decoding and printing]");
                    decode = false;
                    continue;
                } else if (line.equals("")) {
                    continue;
                }
                System.out.println();
                exec(line);
            }
        }

        if (cmd.equals("query")) {
            if (args.length < 3) {
                System.out.println("usage: <bin> ch-host query <query-string>");
                return -1;
            }
            exec(args[2]);
        }

        if (cmd.equals("querys")) {
            if (args.length < 4) {
                System.out.println("usage: <bin> ch-host querys <query-string> <times>");
                return -1;
            }
            String query = args[2];
            int times = Integer.parseInt(args[3]);
            for (int i = 0; i < times; ++i) {
                exec(query);
            }
        }

        return 0;
    }

    public CHRaw() {
    }

    private String host = "127.0.0.1";
    private int port = 9006;
    private boolean decode = true;

    public static void main(String[] args) throws Exception {
        CHRaw ch = new CHRaw();
        int code = ch.loop(args);
        if (code != 0) {
            System.exit(code);
        }
    }
}
