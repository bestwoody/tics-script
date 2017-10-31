package pingcap.com;

public class App {
	static {
		System.loadLibrary("bench");
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("usage: <bin> sum-int|sum-double times");
			System.exit(-1);
		}

		String cmd = args[0];
		int times = Integer.parseInt(args[1]);
		MagicProtoBench bench = new MagicProtoBench();

		if (cmd.equals("sum-int")) {
			int result = 0;
			for (int i = 0; i < times; i++) {
				result = bench.sumInt(3, 2);
			}
		}

		if (cmd.equals("sum-double")) {
			double result = 0;
			for (int i = 0; i < times; i++) {
				result = bench.sumDouble(2.3, 3.2);
			}
		}
	}
}
