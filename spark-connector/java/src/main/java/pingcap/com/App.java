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

public class App {
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("usage: <bin> version|scanAll|scan");
			System.exit(-1);
		}

		System.loadLibrary("ch");

		MagicProto magic= new MagicProto();

		String cmd = args[0];

		if (cmd.equals("version")) {
			System.out.print("libch version: " + magic.version());
		}

		if (cmd.equals("query")) {
			System.out.print("query: TODO");
		}
	}
}