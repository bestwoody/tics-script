package org.apache.spark.sql.ch;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;
import java.util.stream.Collectors;

public class CHArrowResult {
  private final CHExecutor.Result result;
  private final VectorSchemaRoot schemaRoot;
  private final List<FieldVector> columns;
  private final List<ArrowType> fieldTypes;

  public CHArrowResult(CHExecutor.Result result) {
    this.result = result;
    this.schemaRoot = result.block;
    this.columns = result.block.getFieldVectors();
    this.fieldTypes = columns.stream().map(valueVectors -> valueVectors.getField().getType()).collect(Collectors.toList());
  }

  public CHExecutor.Result getResult() {
    return result;
  }


}
