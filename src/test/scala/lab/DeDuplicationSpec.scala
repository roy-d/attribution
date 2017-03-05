package lab

import lab.Transformers._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, MustMatchers}

class DeDuplicationSpec extends FlatSpec with MustMatchers with SparkSupport {

  //| timestamp  | event_id  | advertiser_id  | user_id | event_type |
  private val eventRows = Seq(
    Row(1450631450, "5bb2b119-226d-4bdf-95ad-a1cdf9659789", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click"),
    Row(1450631452, "23aa6216-3997-4255-9e10-7e37a1f07060", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click"),
    Row(1450631464, "61c3ed32-01f9-43c4-8f54-eee3857104cc", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "purchase"),
    Row(1450631466, "20702cb7-60ca-413a-8244-d22353e2be49", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click"),
    Row(1450691451, "20702cb7-60ca-413a-8244-d22353e2be49", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "click")
  )

  private val eventsDF: DataFrame = sqlContext.createDataFrame(sc.parallelize(eventRows), Event.schema)

  "Events DataFrame" should "allow parsing" in {
    val events = eventsDF.map(Event.parse)
    events.collect.length must ===(5)

    val clicks = events.filter(_.eventType == "click")
    clicks.collect.length must ===(4)
  }

  it should "allow de-duplication" in {
    val events = eventsDF.map(Event.parse)
    val deDuped = getDeDupedEvents(events)

    deDuped.collect.length must ===(3)

    val deDupedClicks = deDuped.filter(_.eventType == "click")
    deDupedClicks.collect.length must ===(2)
  }

}
