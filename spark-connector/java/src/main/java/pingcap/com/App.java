package pingcap.com;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

public class App {
	private static void dump(Magic.Query query) throws Exception {
		Schema schema = query.schema();
		List<Field> fields = schema.getFields();
		int i = 0;
		for (Field field: fields) {
			System.out.println("#" + i + " name=\"" + field.getName() + "\" type=" +
				field.getType().getTypeID() + " nullable=" + field.isNullable());
			i += 1;
		}

		while (true) {
			VectorSchemaRoot block = query.next();
			if (block == null) {
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

	public static void exec(Magic magic, String query) throws Exception {
		System.out.println("[" + query + "]");
		Magic.Query result = magic.query(query);
		dump(result);
		result.close();
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("usage: <bin> version|cli|query|querys");
			System.exit(-1);
		}

		Magic magic = new Magic();
		String cmd = args[0];

		if (cmd.equals("version")) {
			System.out.print("libch version: " + magic.version());
			return;
		}

		if (args.length < 2) {
			System.out.println("usage: <bin> cmd <db-path> ...");
			System.exit(-1);
		}

		String path = args[1];
		magic.init(path);

		if (cmd.equals("cli")) {
			// TODO
			return;
		}

		if (cmd.equals("query")) {
			if (args.length < 3) {
				System.out.println("usage: <bin> query <db-path> <query-string>");
				System.exit(-1);
			}
			exec(magic, args[2]);
		}

		if (cmd.equals("querys")) {
			if (args.length < 4) {
				System.out.println("usage: <bin> query <db-path> <query-string> <times>");
				System.exit(-1);
			}
			String query = args[2];
			int times = Integer.parseInt(args[3]);
			for (int i = 0; i < times; ++i) {
				exec(magic, query);
			}
		}

		magic.close();
	}
}
