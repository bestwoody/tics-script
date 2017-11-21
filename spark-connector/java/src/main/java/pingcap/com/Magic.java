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

public class Magic {
	public static class MagicException extends Exception {
		public MagicException(String msg) {
			super(msg);
		}
	}

	public class Query {
		public Query(Magic magic, long token) {
			this.magic = magic;
			this.token = token;
		}

		public Schema schema() throws IOException, MagicException {
			if (schema == null) {
				schema = magic.schema(token);
			}
			return schema;
		}

		public VectorSchemaRoot next() throws IOException, MagicException {
			return magic.next(schema(), token);
		}

		public void close() throws MagicException {
			magic.close(token);
		}

		public final Magic magic;
		private final long token;
		private Schema schema;
	}

	public Magic() {
		System.loadLibrary("ch");
		this.lib = new MagicProto();
		this.alloc = new RootAllocator(Long.MAX_VALUE);
	}

	public String version() {
		return lib.version();
	}

	public void init(String path) {
		this.path = path;
	}

	public void _init() throws MagicException {
		if (inited) {
			return;
		}
		if (path == null) {
			throw new MagicException("init failed: path not set");
		}
		MagicProto.InitResult result = lib.init(path);
		if (result.error != null) {
			throw new MagicException("init failed: " + result.error);
		}
		inited = true;
	}

	public Query query(String query) throws MagicException {
		_init();
		MagicProto.QueryResult result = lib.query(query);
		if (result.error != null) {
			throw new MagicException("query failed: " + result.error);
		}
		return new Query(this, result.token);
	}

	public Schema schema(long token) throws IOException, MagicException {
		if (!inited) {
			throw new MagicException("get schema failed: uninited");
		}
		// TODO: maybe better: Schema::deserialize(ByteBuffer buffer)
		byte[] result = lib.schema(token);
		if (result == null) {
			throw new MagicException("get schema failed: " + lib.error(token));
		}
		ByteArrayInputStream in = new ByteArrayInputStream(result);
		ReadChannel channel = new ReadChannel(Channels.newChannel(in));
		return MessageSerializer.deserializeSchema(channel);
	}

	public VectorSchemaRoot next(Schema schema, long token) throws IOException, MagicException {
		if (!inited) {
			throw new MagicException("get next block failed: uninited");
		}
		byte[] result = lib.next(token);
		if (result == null) {
			String error = lib.error(token);
			if (error != null) {
				throw new MagicException("get next block failed: " + error);
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

	public void close(long token) throws MagicException {
		if (!inited) {
			return;
		}
		lib.close(token);
		String error = lib.error(token);
		if (error != null) {
			throw new MagicException("close failed: " + error);
		}
	}

	public void close() throws MagicException {
		MagicProto.FinishResult result = lib.finish();
		if (result.error != null) {
			throw new MagicException("finish failed: " + result.error);
		}
	}

	public final MagicProto lib;
	private String path;
	private boolean inited;
	private final BufferAllocator alloc;
}
