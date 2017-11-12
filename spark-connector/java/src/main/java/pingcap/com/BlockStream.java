package pingcap.com;

import static java.util.Arrays.asList;

import java.util.List;
import java.util.ArrayList;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.FieldVector;


public class BlockStream {
	private MagicProto magic;
	private long token;
	private BufferAllocator alloc;
	private Schema schema;

	public BlockStream(MagicProto magic, long token) {
		this.magic = magic;
		this.token = token;
		this.alloc = new RootAllocator(Long.MAX_VALUE);
	}

	private Schema schema() throws Exception {
		if (schema == null) {
			// TODO: maybe better: Schema::deserialize(ByteBuffer buffer)
			byte[] result = magic.schema(token);
			ByteArrayInputStream in = new ByteArrayInputStream(result);
			ReadChannel channel = new ReadChannel(Channels.newChannel(in));
			schema = MessageSerializer.deserializeSchema(channel);
		}
		return schema;
	}

	public VectorSchemaRoot next() throws Exception {
		byte[] result = magic.next(token);
		ByteArrayInputStream in = new ByteArrayInputStream(result);
		ReadChannel channel = new ReadChannel(Channels.newChannel(in));
		ArrowRecordBatch batch = (ArrowRecordBatch)MessageSerializer.deserializeMessageBatch(channel, alloc);

		VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
		VectorLoader loader = new VectorLoader(root);
		loader.load(batch);
		return root;
	}

	// Just for test
	// TODO: handle more types
	public void dump() throws Exception {
		System.out.println("Schema:");
		Schema schema = schema();
		if (schema == null) {
			System.out.println("get schema failed:");
			System.out.println(magic.error(token));
			return;
		}
		List<Field> fields = schema.getFields();
		int i = 0;
		for (Field field: fields) {
			System.out.println("#" + i + " " + field.getName() + " nullable:" + field.isNullable());
			i += 1;
		}
		System.out.println("");

		while (true) {
			VectorSchemaRoot block = next();
			if (block == null) {
				String error = magic.error(token);
				if (error != null) {
					System.out.println("get block failed: " + error);
				}
				break;
			}

			List<FieldVector> columns = block.getFieldVectors();
			int j = 0;
			for (FieldVector column: columns) {
				Field field = fields.get(j);
				if (field.getName() == "BigInt") {
					if (field.isNullable()) {
						NullableBigIntVector.Accessor acc = ((NullableBigIntVector)column).getAccessor();
						for (int k = 0; k < acc.getValueCount(); ++k) {
							System.out.println(acc.get(k));
						}
					} else {
						// TODO
					}
				}
				j += 1;
				System.out.println("");
			}
		}
	}
}
