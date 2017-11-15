package pingcap.com;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

public class BlockStream {
	private Magic.Query query;

	public BlockStream(Magic.Query query) {
		this.query = query;
	}

	// Just for test
	public void dump() throws Exception {
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
}
