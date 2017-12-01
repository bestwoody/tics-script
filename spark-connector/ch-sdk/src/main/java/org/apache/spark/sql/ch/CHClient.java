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
import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;


public class CHClient {
	private final int PackageTypeEnd = 0;
	private final int PackageTypeUtf8Error = 1;
	private final int PackageTypeUtf8Query = 2;
	private final int PackageTypeArrowSchema = 3;
	private final int PackageTypeArrowData = 4;

	public static class CHClientException extends Exception {
		public CHClientException(String msg) {
			super(msg);
		}
	}

	public CHClient(String query, String host, int port, ArrowDecoder arrowDecoder) throws Exception {
		this.query = query;
		this.socket = new Socket(host, port);
		this.writer = new DataOutputStream(socket.getOutputStream());
		this.reader = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
		this.finished = false;

		if (arrowDecoder == null) {
			arrowDecoder = new ArrowDecoder();
		}
		this.arrowDecoder = arrowDecoder;

		sendQuery(query);
		fetchSchema();
		startFetchPackages();
		startDecodePackages();
	}

	public void close() throws IOException {
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
			throw new CHClientException(decoded.error);
		}
		return decoded.block;
	}

	private void sendQuery(String query) throws IOException {
		int val = PackageTypeUtf8Query;
		writer.writeInt(val);
		val = query.length();
		writer.writeInt(val);
		writer.writeBytes(query);
	}

	private void startFetchPackages() {
		Thread worker = new Thread(new Runnable() {
			public void run() {
				try {
					while (fetchPackage());
				}
				catch (Exception e) {
					try {
						decodings.put(new Decoding(PackageTypeUtf8Error, e.toString().getBytes()));
					}
					catch (Exception _) {
					}
				}
			}});
		worker.start();
	}

	private void startDecodePackages() {
		// TODO: multi decode workers, or just one is fine, need to run a benchmark
		// NOTE: if we use multi workers, reordering is needed
		Thread worker = new Thread(new Runnable() {
			public void run() {
				try {
					while (decodePackage());
				}
				catch (Exception e) {
					try {
						decodings.put(new Decoding(PackageTypeUtf8Error, e.toString().getBytes()));
					}
					catch (Exception _) {
					}
				}
			}});
		worker.start();
	}

	private boolean fetchPackage() throws InterruptedException, IOException {
		int type = reader.readInt();
		if (type == PackageTypeEnd) {
			decodings.put(new Decoding(type, null));
			return false;
		}
		byte[] data = null;
		if (type != PackageTypeEnd) {
			int size = reader.readInt();
			data = new byte[size];
			reader.readFully(data);
		}
		decodings.put(new Decoding(type, data));
		return type != PackageTypeUtf8Error;
	}

	private boolean decodePackage() throws Exception {
		Decoding decoding = decodings.take();
		if (decoding.type == PackageTypeUtf8Error) {
			decodeds.put(new Decoded(new String(decoding.data)));
		} else if (decoding.type == PackageTypeArrowData) {
			decodeds.put(new Decoded(arrowDecoder.decodeBlock(schema, decoding.data)));
		} else if (decoding.type == PackageTypeEnd) {
			decodeds.put(new Decoded());
			return false;
		} else {
			throw new CHClientException("Unknown package, type: " + decoding.type);
		}
		return true;
	}

	private void fetchSchema() throws Exception {
		int type = reader.readInt();
		if (type != PackageTypeArrowSchema) {
			throw new CHClientException("No received schema.");
		}
		int size = reader.readInt();
		byte[] data = new byte[size];
		reader.readFully(data);
		schema = arrowDecoder.decodeSchema(data);
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
	private DataInputStream reader;
	private Schema schema;
	private boolean finished;

	private ArrowDecoder arrowDecoder;

	private BlockingQueue<Decoding> decodings;
	private BlockingQueue<Decoded> decodeds;
}
