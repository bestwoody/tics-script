package pingcap.com;

public class App {
	static {
		System.loadLibrary("bench");
	}

	public static void main(String[] args) {
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
				System.out.println("usage: <bin> bytes times size");
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
				System.out.println("usage: <bin> bytes times size");
				System.exit(-1);
			}
			int size = Integer.parseInt(args[2]);
			byte[] result;
			long cb = 0;
			for (int i = 0; i < times; i++) {
				result = bench.benchArrowArray(size);
				if (result == null || result.length == 0) {
					System.out.println("Fetch arrow array failed");
					System.exit(-1);
				}
				cb += result.length;
			}
			System.out.println("Total size: " + cb);
		}
	
	}
}
