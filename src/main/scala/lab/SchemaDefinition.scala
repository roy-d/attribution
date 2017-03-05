package lab

import org.apache.spark.sql.types.{DataType, StructField}

import scala.collection.mutable.ListBuffer

trait SchemaDefinition extends Serializable {
  private val builder = ListBuffer.empty[StructField]

  protected def structField(name: String, dataType: DataType): StructField = {
    val field = StructField(name, dataType)
    builder += field
    field
  }

  def fields: Seq[StructField] = builder.toSeq
}
