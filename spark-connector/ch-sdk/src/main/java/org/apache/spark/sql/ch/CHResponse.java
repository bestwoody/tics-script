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


public class CHResponse {
	private final long PackageTypeEnd = 0;
	private final long PackageTypeUtf8Error = 1;
	private final long PackageTypeUtf8Query = 2;
	private final long PackageTypeArrowSchema = 3;
	private final long PackageTypeArrowData = 4;

	public static class CHResponseException extends Exception {
		public CHResponseException(String msg) {
			super(msg);
		}
	}

	public CHResponse(String query, String host, int port, ArrowDecoder arrowDecoder) throws Exception {
		this.query = query;
		this.socket = new Socket(host, port);
		this.writer = new DataOutputStream(socket.getOutputStream());
		this.reader = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
		this.finished = false;

		this.decodeds = new LinkedBlockingQueue();
		this.decodings = new LinkedBlockingQueue();

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
			throw new CHResponseException(decoded.error);
		}
		return decoded.block;
	}

	private void sendQuery(String query) throws IOException {
		long val = PackageTypeUtf8Query;
		writer.writeLong(val);
		val = query.length();
		writer.writeLong(val);
		writer.writeBytes(query);
		writer.flush();
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

	private boolean fetchPackage() throws Exception {
		long type = reader.readLong();
		if (type == PackageTypeEnd) {
			decodings.put(new Decoding(type, null));
			return false;
		}
		byte[] data = null;
		if (type != PackageTypeEnd) {
			long size = reader.readLong();
			if (size >= Integer.MAX_VALUE) {
				throw new CHResponseException("Package too big, size: " + size);
			}
			data = new byte[(int)size];
			reader.readFully(data);
		}
		decodings.put(new Decoding(type, data));
		return type != PackageTypeUtf8Error;
	}

	private boolean decodePackage() throws Exception {
		Decoding decoding = decodings.take();
		if (decoding.type == PackageTypeUtf8Error) {
			decodeds.put(new Decoded(new String(decoding.data)));
			return false;
		} else if (decoding.type == PackageTypeArrowData) {
			decodeds.put(new Decoded(arrowDecoder.decodeBlock(schema, decoding.data)));
		} else if (decoding.type == PackageTypeEnd) {
			decodeds.put(new Decoded());
			return false;
		} else {
			throw new CHResponseException("Unknown package, type: " + decoding.type);
		}
		return true;
	}

	private void fetchSchema() throws Exception {
		long type = reader.readLong();
		long size = reader.readLong();
		if (size >= Integer.MAX_VALUE) {
			throw new CHResponseException("Package too big, size: " + size);
		}
		byte[] data = new byte[(int)size];
		reader.readFully(data);
		if (type == PackageTypeUtf8Error) {
			throw new CHResponseException("Error from storage: " + new String(data));
		}
		if (type != PackageTypeArrowSchema) {
			throw new CHResponseException("Received package, but not schema, type: " + type);
		}
		
		schema = arrowDecoder.decodeSchema(data);
	}

	private static class Decoding {
		Decoding(long type, byte[] data) {
			this.type = type;
			this.data = data;
		}

		private long type;
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
