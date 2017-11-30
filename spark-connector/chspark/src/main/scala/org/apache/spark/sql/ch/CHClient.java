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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;

public class CHClient {
	private enum PackageType {
		End(0),
		Error(1),
		Utf8Query(2),
		ArrowSchema(3),
		ArrowData(4)
	}

	public CHClient(String query, String host, int port) throws Exception {
		this.query = query;
		this.socket = new Socket(host, port);
		this.writer = new DataOutputStream(socket.getOutputStream());
		this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		sendQuery(query);
	}

	private void sendQuery() {
		int val = PackageType.Utf9Query;
		writer.writeBytes(flag);
		int val = query.length;
		writer.writeBytes(val);
		writer.writeBytes(query);
	}

	public close() {
		socket.close();
	}

	public Schema getSchema() {
		handleOnePackage();
		if (schema == null) {
			throw Exception("No received schema.")
		}
		return schema;
	}

	public boolean hasNext() {
		return cache != null;
	}

	public VectorSchemaRoot next() {
		VectorSchemaRoot result = cache;
		handleOnePackage();
		return result;
	}

	// TODO: Async fetch, use blocking queue
	private boolean handleOnePackage() {
		if (schema != null && cache != null) {
			return true;
		}
		int type = reader.readInt();
		if (type == PackageType.End) {
			return false;
		}
		int size = reader.readInt();
		byte[] data = reader.readBytes(size);
		if (type == PackageType.Error) {
			throw Exception(String(data));
		} else if (type == PackageType.ArrowSchema) {
			handleSchema(data);
		} else if (type == PackageType.ArrowData) {
			handleBlock(data);
		} else {
			throw Exception("Unknown package, type: " + type);
		}
		return true;
	}

	private void handleSchema(byte[] data) {
		// TODO: decode to schema
		System.out.println("TODO: handleSchema, size: " + data.length);
	}

	private void handleBlock(byte[] data) {
		// TODO: decode to cache
		System.out.println("TODO: handleBlock, size: " + data.length);
	}

	private String query;
	private Socket socket;
	private DataOutputStream writer;
	private BufferedReader reader;

	private VectorSchemaRoot cache;
	private Schema schema;
}
