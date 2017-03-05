package lab

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType, _}

case class Event(timestamp: Int, eventId: String, advertiserId: Int, userId: String, eventType: String)

object Event {
  self =>
  val timestamp = "timestamp"
  val event_id = "event_id"
  val advertiser_id = "advertiser_id"
  val user_id = "user_id"
  val event_type = "event_type"

  object Schema extends SchemaDefinition {
    val timestamp = structField(self.timestamp, IntegerType)
    val event_id = structField(self.event_id, StringType)
    val advertiser_id = structField(self.advertiser_id, IntegerType)
    val user_id = structField(self.user_id, StringType)
    val event_type = structField(self.event_type, StringType)
  }

  val schema: StructType = StructType(Schema.fields)

  import RowOps._

  def parse(row: Row): Event = Event(
    row.read[IntColumn](0),
    row.read[StringColumn](1),
    row.read[IntColumn](2),
    row.read[StringColumn](3),
    row.read[StringColumn](4)
  )
}
