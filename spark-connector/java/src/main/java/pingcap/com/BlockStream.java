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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;

import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableVarCharVector;

import org.apache.arrow.vector.types.pojo.ArrowType;

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
		if (result == null) {
			return null;
		}

		ByteArrayInputStream in = new ByteArrayInputStream(result);
		ReadChannel channel = new ReadChannel(Channels.newChannel(in));
		ArrowRecordBatch batch = (ArrowRecordBatch)MessageSerializer.deserializeMessageBatch(channel, alloc);

		VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
		VectorLoader loader = new VectorLoader(root);
		loader.load(batch);
		return root;
	}

	// Just for test
	public void dump() throws Exception {
		Schema schema = schema();
		if (schema == null) {
			System.out.println("get schema failed: " + magic.error(token));
			return;
		}
		List<Field> fields = schema.getFields();
		int i = 0;
		for (Field field: fields) {
			System.out.println("#" + i + " name=\"" + field.getName() + "\" type=" +
				field.getType().getTypeID() + " nullable=" + field.isNullable());
			i += 1;
		}

		while (true) {
			VectorSchemaRoot block = next();
			if (block == null) {
				String error = magic.error(token);
				if (error != null) {
					System.out.println("get block failed: " + error);
				}
				break;
			}
			System.out.println("======");

			List<FieldVector> columns = block.getFieldVectors();
			int j = 0;
			for (FieldVector column: columns) {
				Field field = column.getField();
				String name = field.getName();
				String type = field.getType().getTypeID().toString();
				System.out.println("#" + j + " \"" + name + "\"");
				System.out.println("------");
				ValueVector.Accessor acc = column.getAccessor();
				for (int k = 0; k < acc.getValueCount(); ++k) {
					System.out.println(acc.getObject(k));
				}
				System.out.println("------");
				j += 1;
			}

			System.out.println("======");
		}
	}
}
