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


public class BlockStream {
	private MagicProto magic;
	private long token;
	private BufferAllocator alloc;

	public BlockStream(MagicProto magic, long token) {
		this.magic = magic;
		this.token = token;
		this.alloc = new RootAllocator(Long.MAX_VALUE);
	}

	public void schema() {
		// TODO
	}

	public void next() throws Exception {
		byte[] result = magic.next(token);
		ByteArrayInputStream in = new ByteArrayInputStream(result);
		ReadChannel channel = new ReadChannel(Channels.newChannel(in));
		ArrowRecordBatch batch = (ArrowRecordBatch)MessageSerializer.deserializeMessageBatch(channel, alloc);
		ArrowType art = new Int(64, true);
		FieldType type = new FieldType(true, art, null, null);
		Schema schema = new Schema(asList(new Field("f0", type, new ArrayList<Field>())));
		VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
		VectorLoader loader = new VectorLoader(root);
		loader.load(batch);
		NullableBigIntVector.Accessor acc = ((NullableBigIntVector)root.getVector("f0")).getAccessor();
		for (int j = 0; j < acc.getValueCount(); j++) {
			assert acc.get(j) == j;
		}
	}
}
