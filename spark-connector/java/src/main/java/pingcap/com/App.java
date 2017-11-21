package pingcap.com;

import java.util.List;
import java.util.Scanner;
import java.lang.Character;
import java.io.File;
import java.io.PrintStream;
import java.io.FileOutputStream;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

public class App {
	private void dump(Field field, int order) {
		System.out.println("    #" + order + " name:\"" + field.getName() + "\" type:" +
			field.getType().getTypeID() + " nullable:" + field.isNullable());
	}

	private void dump(Magic.Query query) throws Exception {
		Schema schema = query.schema();
		List<Field> fields = schema.getFields();
		int i = 0;
		if (decode) {
			System.out.println("[schema]");
			for (Field field: fields) {
				dump(field, i);
				i += 1;
			}
		}

		while (true) {
			VectorSchemaRoot block = query.next();
			if (block == null) {
				break;
			}
			if (!decode) {
				continue;
			}
			System.out.println("[result]");

			List<FieldVector> columns = block.getFieldVectors();
			int j = 0;
			for (FieldVector column: columns) {
				Field field = column.getField();
				dump(field, j);
				ValueVector.Accessor acc = column.getAccessor();
				for (int k = 0; k < acc.getValueCount(); ++k) {
					Object v = acc.getObject(k);
					if (v instanceof Character) {
						System.out.println("    " + (int)(Character)v);
					} else {
						System.out.println("    " + acc.getObject(k).toString());
					}
				}
				j += 1;
				if (j != columns.size()) {
					System.out.println("    ------");
				}
			}

			System.out.println();
		}

		System.out.println("[query done]");
	}

	public void exec(Magic magic, String query) throws Exception {
		System.out.println("[query]");
		System.out.println("    " + query);
		Magic.Query result = magic.query(query);
		dump(result);
		result.close();
	}

	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("usage: <bin> version|cli|query|querys");
			return -1;
		}

		Magic magic = new Magic();
		String cmd = args[0];

		if (cmd.equals("version")) {
			System.out.print("libch version: " + magic.version());
			return 0;
		}

		if (args.length < 2) {
			System.out.println("usage: <bin> cmd <db-path> ...");
			return -1;
		}

		String path = args[1];
		magic.init(path);

		if (cmd.equals("cli")) {
			System.out.println("[intereact mode, type 'quit' to exit]");
			while (true) {
				Scanner reader = new Scanner(System.in);
				String line = reader.nextLine();
				if (line.equals("quit")) {
					return 0;
				} else if (line.equals("to-log")) {
					System.out.println("[redirecting output to magic-java.log]");
					System.setOut(new PrintStream(new File("magic-java.log")));
					continue;
				} else if (line.equals("no-decode")) {
					System.out.println("[throughput bench mode, disable data decoding and printing]");
					decode = false;
					continue;
				} else if (line.equals("gc")) {
					System.out.println("[gc start]");
					System.gc();
					System.out.println("[gc done]");
					continue;
				} else if (line.equals("version")) {
					System.out.println("[libch version]");
					System.out.println("    " + magic.version());
					continue;
				} else if (line.equals("bench")) {
					System.out.println("[enter simple data bench mode, type 'exit' to exit]");
					while (true) {
						line = reader.nextLine();
						if (line.equals("exit")) {
							break;
						}
						int code = Bench.run(line.split(" "));
						System.out.println("[bench done: " + code + "]");
					}
					continue;
				}
				System.out.println();
				try {
					exec(magic, line);
				} catch (Exception e) {
					System.err.println("[exception]");
					System.err.println("    " + e);
					System.err.println();
					return -1;
				}
			}
		}

		if (cmd.equals("query")) {
			if (args.length < 3) {
				System.out.println("usage: <bin> query <db-path> <query-string>");
				return -1;
			}
			exec(magic, args[2]);
		}

		if (cmd.equals("querys")) {
			if (args.length < 4) {
				System.out.println("usage: <bin> query <db-path> <query-string> <times>");
				return -1;
			}
			String query = args[2];
			int times = Integer.parseInt(args[3]);
			for (int i = 0; i < times; ++i) {
				exec(magic, query);
			}
		}

		magic.close();
		return 0;
	}

	public App() {
	}

	private boolean decode = true;

	public static void main(String[] args) throws Exception {
		App app = new App();
		int code = app.run(args);
		if (code != 0) {
			System.exit(code);
		}
	}
}
