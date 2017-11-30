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


public CHRaw {
	private static void dump(Field field, int order) {
		System.out.println("    #" + order + " name:\"" + field.getName() + "\" type:" +
			field.getType().getTypeID() + " nullable:" + field.isNullable());
	}

	private static void dump(Magic.Query query, boolean decode) throws Exception {
		Schema schema = query.schema();
		List<Field> fields = schema.getFields();
		int i = 0;
		if (decode) {
			System.out.println("[schema]");
			for (Field field: fields) {
				dump(field, i);
				i += 1;
			}
		}

		while (true) {
			VectorSchemaRoot block = query.next();
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
				dump(field, j);
				ValueVector.Accessor acc = column.getAccessor();
				for (int k = 0; k < acc.getValueCount(); ++k) {
					Object v = acc.getObject(k);
					if (v instanceof Character) {
						System.out.println("    " + (int)(Character)v);
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
			System.out.println("usage: <bin> query ch-host [port]");
			System.exit(-1);
		}

		String query = args[0];
		String host = args[1];
		int port = 9001;
		if (args.length > 2) {
			port = Integer.parseInt(args[2]);
		}

		CHClient client = new CHClient(query, host, port);

		Schema schema = client.getSchema();
		dump(schema);

		for (client.hasNext()) {
			VectorSchemaRoot block = client.next();
			dump(block, true);
		}

		client.close();
	}
}
