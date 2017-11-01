package pingcap.com;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;

import io.netty.buffer.ArrowBuf;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowMessage.ArrowMessageVisitor;

public class App {
	static {
		System.loadLibrary("bench");
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("usage: <bin> sum-int|sum-double|bytes|arrow-array times");
			System.exit(-1);
		}

		String cmd = args[0];
		int times = Integer.parseInt(args[1]);
		MagicProtoBench bench = new MagicProtoBench();

		if (cmd.equals("sum-int")) {
			int result = 0;
			for (int i = 0; i < times; i++) {
				result = bench.benchSumInt(3, 2);
			}
		}

		if (cmd.equals("sum-double")) {
			double result = 0;
			for (int i = 0; i < times; i++) {
				result = bench.benchSumDouble(2.3, 3.2);
			}
		}

		if (cmd.equals("bytes")) {
			if (args.length < 3) {
				System.out.println("usage: <bin> times size");
				System.exit(-1);
			}
			int size = Integer.parseInt(args[2]);
			byte[] result;
			for (int i = 0; i < times; i++) {
				result = bench.benchAlloc(size);
			}
		}

		if (cmd.equals("arrow-array")) {
			if (args.length < 3) {
				System.out.println("usage: <bin> times rows [decode]");
				System.exit(-1);
			}
			int size = Integer.parseInt(args[2]);
			boolean decode = false;
			if (args.length > 3 && args[3].equals("decode")) {
				decode = true;
			}

			byte[] result;
			long cb = 0;
			BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
			ArrowRecordBatch batch;
			for (int i = 0; i < times; i++) {
				result = bench.benchArrowArray(size);
				if (result == null || result.length == 0) {
					System.out.println("fetch arrow array failed");
					System.exit(-1);
				}
				cb += result.length;
				if (decode) {
					ByteArrayInputStream in = new ByteArrayInputStream(result);
					ReadChannel channel = new ReadChannel(Channels.newChannel(in));
					batch = (ArrowRecordBatch)MessageSerializer.deserializeMessageBatch(channel, alloc);
					List<ArrowBuf> bufs = batch.getBuffers();
					ArrowBuf values = bufs.get(1);
					// TODO: verify rows
				}
			}
			System.out.println("total size: " + cb);
		}

	}
}
