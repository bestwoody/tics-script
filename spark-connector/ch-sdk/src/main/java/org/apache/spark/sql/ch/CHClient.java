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

import java.net.Socket;
import java.io.DataOutputStream;
import java.io.BufferedReader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;


public class CHClient {
/*
	private enum PackageType {
		End(0),
		Utf8Error(1),
		Utf8Query(2),
		ArrowSchema(3),
		ArrowData(4)
	}

	public CHClient(String query, String host, int port) throws Exception {
		this.query = query;
		this.socket = new Socket(host, port);
		this.writer = new DataOutputStream(socket.getOutputStream());
		this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		this.finished = false;

		sendQuery(query);
		fetchSchema();
		startFetchPackages();
		startDecodePackages();
	}

	public void close() {
		socket.close();
	}

	public Schema getSchema() {
		return schema;
	}

	public boolean hasNext() {
		return !finished;
	}

	public VectorSchemaRoot next() throws Exception {
		Decoded decoded = decodeds.take();
		if (decoded.isEmpty()) {
			finished = true;
		}
		if (decoded.error != null) {
			finished = true;
			throw Exception(decoded.error);
		}
		return decoded.block;
	}

	private void sendQuery() {
		int val = PackageType.Utf8Query;
		writer.writeBytes(flag);
		int val = query.length;
		writer.writeBytes(val);
		writer.writeBytes(query);
	}

	private void startFetchPackages() {
		Thread worker = new Thread(new Runnable() {
			public void run() throws Exception {
				while (fetchPackage());
			}});
		worker.start();
	}

	private void startDecodePackage() {
		// TODO: multi decode workers, or just one is fine, need to run a benchmark
		// NOTE: if we use multi workers, reordering is needed
		Thread worker = new Thread(new Runnable() {
			public void run() throws Exception {
				while (decodePackage());
			}});
		worker.start();
	}

	private boolean fetchPackage() throws Exception {
		int type = reader.readInt();
		if (type == PackageType.End) {
			return false;
		}
		byte[] data = null;
		if (type != PackageType.End) {
			int size = reader.readInt();
			data = reader.readBytes(size);
		}
		decodings.put(new Decoding(type, data));
		return type != PackageType.Utf8Error;
	}

	private boolean decodePackage() throws Exception {
		Decoding decoding = decodings.take();
		if (decoding.type == PackageType.Utf8Error) {
			decodeds.put(new Decoded(String(decoding.data)));
		} else if (type == PackageType.ArrowData) {
			decodeds.put(new Decoded(decodeBlock(decoding.data)));
		} else if (type == PackageType.End) {
			decodeds.put(new Decoded());
			return false;
		} else {
			throw Exception("Unknown package, type: " + type);
		}
		return true;
	}

	private void fetchSchema() {
		int type = reader.readInt();
		if (type != PackageType.Schema) {
			throw Exception("No received schema.");
		}
		int size = reader.readInt();
		byte[] data = reader.readBytes(size);
		// TODO: decode to schema
		System.out.println("TODO: handleSchema, size: " + data.length);
	}

	private VectorSchemaRoot decodeBlock(byte[] data) {
		// TODO: decode to cache
		System.out.println("TODO: handleBlock, size: " + data.length);
		return null;
	}

	private static class Decoding {
		Decoding(int type, byte[] data) {
			this.type = type;
			this.data = data;
		}

		private int type;
		private byte[] data;
	}

	private static class Decoded {
		Decoded(VectorSchemaRoot block) {
			this.block = block;
		}
		Decoded(String error) {
			this.error = error;
		}
		Decoded() {
		}
		boolean isEmpty() {
			return error == null && block == null;
		}

		private String error;
		private VectorSchemaRoot block;
	}

	private String query;
	private Socket socket;
	private DataOutputStream writer;
	private BufferedReader reader;
	private Schema schema;
	private boolean finished;

	private BlockingQueue<Decoding> decodings;
	private BlockingQueue<Decoded> decodeds;
*/
}
