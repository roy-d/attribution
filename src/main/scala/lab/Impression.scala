package lab

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType, _}

case class Impression(timestamp: Int, advertiserId: Int, creativeId: Int, userId: String)

object Impression {
  self =>
  val timestamp = "timestamp"
  val advertiser_id = "advertiser_id"
  val creative_id = "creative_id"
  val user_id = "user_id"

  object Schema extends SchemaDefinition {
    val timestamp = structField(self.timestamp, IntegerType)
    val advertiser_id = structField(self.advertiser_id, IntegerType)
    val creative_id = structField(self.creative_id, IntegerType)
    val user_id = structField(self.user_id, StringType)
  }

  val schema: StructType = StructType(Schema.fields)

  import RowOps._

  def parse(row: Row): Impression = Impression(
    row.read[IntColumn](0),
    row.read[IntColumn](1),
    row.read[IntColumn](2),
    row.read[StringColumn](3)
  )
}
