package com.pingcap.ch.datatypes;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

public class CHTypeFactory {

  public static CHType parseType(String typeName) throws IOException {
    if (typeName == null || typeName.isEmpty()) {
      throw new IOException("Empty CH type!");
    }
    typeName = typeName.trim();
    switch (typeName) {
      case "UInt8":
        return CHTypeNumber.CHTypeUInt8.instance;
      case "UInt16":
        return CHTypeNumber.CHTypeUInt16.instance;
      case "UInt32":
        return CHTypeNumber.CHTypeUInt32.instance;
      case "UInt64":
        return CHTypeNumber.CHTypeUInt64.instance;
      case "Int8":
        return CHTypeNumber.CHTypeInt8.instance;
      case "Int16":
        return CHTypeNumber.CHTypeInt16.instance;
      case "Int32":
        return CHTypeNumber.CHTypeInt32.instance;
      case "Int64":
        return CHTypeNumber.CHTypeInt64.instance;
      case "Float32":
        return CHTypeNumber.CHTypeFloat32.instance;
      case "Float64":
        return CHTypeNumber.CHTypeFloat64.instance;
      case "Date":
        return CHTypeDate.instance;
      case "DateTime":
        return CHTypeDateTime.instance;
      case "String":
        return CHTypeString.instance;
    }
    if (typeName.startsWith("FixedString")) {
      String remain = StringUtils.removeStart(typeName, "FixedString");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      try {
        int length = Integer.parseInt(remain);
        return new CHTypeFixedString(length);
      } catch (NumberFormatException e) {
        throw new IOException("Illegal CH type: " + typeName);
      }
    }
    if (typeName.startsWith("Decimal")) {
      String remain = StringUtils.removeStart(typeName, "Decimal");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      try {
        String[] args = remain.split(",");
        int precision = Integer.parseInt(args[0]);
        int scale = Integer.parseInt(args[1]);
        return new CHTypeDecimal(precision, scale);
      } catch (Exception e) {
        throw new IOException("Illegal CH type: " + typeName);
      }
    }
    if (typeName.startsWith("Nullable")) {
      String remain = StringUtils.removeStart(typeName, "Nullable");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      return new CHTypeNullable(parseType(remain));
    }
    throw new IOException("Unsupported CH type: " + typeName);
  }
}
