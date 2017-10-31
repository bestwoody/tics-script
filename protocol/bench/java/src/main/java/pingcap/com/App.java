package pingcap.com;

public class App {
	static {
		System.loadLibrary("bench");
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("usage: <bin> times");
			System.exit(-1);
		}

		int times = Integer.parseInt(args[0]);
		MagicProtoBench bench = new MagicProtoBench();
		double result = 0;
		for (int i = 0; i < times; i++) {
			result = bench.sum(2.0, 3.0);
		}
	}
}
