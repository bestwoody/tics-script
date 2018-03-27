package pingcap.com;

import java.lang.String;
import java.util.List;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorLoader;

public class TheFlash {
	public static class TheFlashException extends Exception {
		public TheFlashException(String msg) {
			super(msg);
		}
	}

	public class Query {
		public Query(TheFlash lib, long token) {
			this.lib = lib;
			this.token = token;
		}

		public Schema schema() throws IOException, TheFlashException {
			if (schema == null) {
				schema = lib.schema(token);
			}
			return schema;
		}

		public VectorSchemaRoot next() throws IOException, TheFlashException {
			return lib.next(schema(), token);
		}

		public void close() throws TheFlashException {
			lib.close(token);
		}

		public final TheFlash lib;
		private final long token;
		private Schema schema;
	}

	public TheFlash() {
		System.loadLibrary("ch");
		this.lib = new TheFlashProto();
		this.alloc = new RootAllocator(Long.MAX_VALUE);
	}

	public String version() {
		return lib.version();
	}

	public void init(String path) {
		this.path = path;
	}

	public void _init() throws TheFlashException {
		if (inited) {
			return;
		}
		if (path == null) {
			throw new TheFlashException("init failed: path not set");
		}
		TheFlashProto.InitResult result = lib.init(path);
		if (result.error != null) {
			throw new TheFlashException("init failed: " + result.error);
		}
		inited = true;
	}

	public Query query(String query) throws TheFlashException {
		_init();
		TheFlashProto.QueryResult result = lib.query(query);
		if (result.error != null) {
			throw new TheFlashException("query failed: " + result.error);
		}
		return new Query(this, result.token);
	}

	public Schema schema(long token) throws IOException, TheFlashException {
		if (!inited) {
			throw new TheFlashException("get schema failed: uninited");
		}
		// TODO: maybe better: Schema::deserialize(ByteBuffer buffer)
		byte[] result = lib.schema(token);
		if (result == null) {
			throw new TheFlashException("get schema failed: " + lib.error(token));
		}
		ByteArrayInputStream in = new ByteArrayInputStream(result);
		ReadChannel channel = new ReadChannel(Channels.newChannel(in));
		return MessageSerializer.deserializeSchema(channel);
	}

	public VectorSchemaRoot next(Schema schema, long token) throws IOException, TheFlashException {
		if (!inited) {
			throw new TheFlashException("get next block failed: uninited");
		}
		byte[] result = lib.next(token);
		if (result == null) {
			String error = lib.error(token);
			if (error != null) {
				throw new TheFlashException("get next block failed: " + error);
			}
			return null;
		}

		ByteArrayInputStream in = new ByteArrayInputStream(result);
		ReadChannel channel = new ReadChannel(Channels.newChannel(in));
		ArrowRecordBatch batch = (ArrowRecordBatch)MessageSerializer.deserializeMessageBatch(channel, alloc);

		VectorSchemaRoot block = VectorSchemaRoot.create(schema, alloc);
		VectorLoader loader = new VectorLoader(block);
		loader.load(batch);
		return block;
	}

	public void close(long token) throws TheFlashException {
		if (!inited) {
			return;
		}
		lib.close(token);
		String error = lib.error(token);
		if (error != null) {
			throw new TheFlashException("close failed: " + error);
		}
	}

	public void close() throws TheFlashException {
		TheFlashProto.FinishResult result = lib.finish();
		if (result.error != null) {
			throw new TheFlashException("finish failed: " + result.error);
		}
	}

	public final TheFlashProto lib;
	private String path;
	private boolean inited;
	private final BufferAllocator alloc;
}
