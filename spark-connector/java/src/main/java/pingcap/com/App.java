package pingcap.com;

public class App {
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("usage: <bin> version|query");
			System.exit(-1);
		}

		Magic magic = new Magic();
		String cmd = args[0];

		if (cmd.equals("version")) {
			System.out.print("libch version: " + magic.version());
			return;
		}

		if (cmd.equals("query")) {
			if (args.length < 3) {
				System.out.println("usage: <bin> query <db-path> <query-string>");
				System.exit(-1);
			}
			String path = args[1];
			String query = args[2];

			System.out.println("[" + query + "]");
			magic.init(path);

			Magic.Query result = magic.query(query);
			BlockStream stream = new BlockStream(result);
			stream.dump();
			result.close();
		}

		magic.close();
	}
}
