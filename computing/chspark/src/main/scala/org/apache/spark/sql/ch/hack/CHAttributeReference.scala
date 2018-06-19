package org.apache.spark.sql.ch.hack

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.types.{DataType, Metadata}

class CHAttributeReference(name: String,
    dataType: DataType,
    nullable: Boolean,
    metadata: Metadata,
    exprId: ExprId = NamedExpression.newExprId,
    qualifier: Option[String] = None,
    isGenerated: java.lang.Boolean = false)
  extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier, isGenerated) {

  override def newInstance(): AttributeReference =
    new CHAttributeReference(name, dataType, nullable, metadata, qualifier = qualifier, isGenerated = isGenerated)

  override def withNullability(newNullability: Boolean): AttributeReference = {
    if (nullable == newNullability) {
      this
    } else {
      new CHAttributeReference(name, dataType, newNullability, metadata, exprId, qualifier, isGenerated)
    }
  }

  override def withName(newName: String): AttributeReference = {
    if (name == newName) {
      this
    } else {
      new CHAttributeReference(newName, dataType, nullable, metadata, exprId, qualifier, isGenerated)
    }
  }

  override def withQualifier(newQualifier: Option[String]): AttributeReference = {
    if (newQualifier == qualifier) {
      this
    } else {
      new CHAttributeReference(name, dataType, nullable, metadata, exprId, newQualifier, isGenerated)
    }
  }

  override def withExprId(newExprId: ExprId): AttributeReference = {
    if (exprId == newExprId) {
      this
    } else {
      new CHAttributeReference(name, dataType, nullable, metadata, newExprId, qualifier, isGenerated)
    }
  }

  override def withMetadata(newMetadata: Metadata): Attribute = {
    new CHAttributeReference(name, dataType, nullable, newMetadata, exprId, qualifier, isGenerated)
  }
}

object CHAttributeReference {
  def unapply(chAttr: CHAttributeReference): Option[(String, DataType, Boolean, Metadata, ExprId, Option[String], java.lang.Boolean)] =
    Option(chAttr.name, chAttr.dataType, chAttr.nullable, chAttr.metadata, chAttr.exprId, chAttr.qualifier, chAttr.isGenerated)
}
