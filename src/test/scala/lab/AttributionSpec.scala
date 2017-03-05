package lab

import lab.Transformers._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, MustMatchers}

class AttributionSpec extends FlatSpec with MustMatchers with SparkSupport {
  //| timestamp  | event_id  | advertiser_id  | user_id | event_type |
  private val eventRows = Seq(
    Row(1450631448, "event1", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "event"),
    Row(1450631452, "event2", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "event"),
    Row(1450631453, "event3", 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "event"),
    Row(1450631464, "event4", 1, "16340204-80e3-411f-82a1-e154c0845cae", "event"),
    Row(1450631466, "event5", 2, "60b74052-fd7e-48e4-aa61-3c14c9c714d5", "event")
  )

  //| timestamp  | advertiser_id  | creative_id  | user_id |
  private val impressionRows = Seq(
    Row(1450631450, 1, 1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5")
  )

  private val eventsDF: DataFrame = sqlContext.createDataFrame(sc.parallelize(eventRows), Event.schema)
  private val impressionsDF: DataFrame = sqlContext.createDataFrame(sc.parallelize(impressionRows), Impression.schema)

  "Events DataFrame" should "allow parsing" in {
    val events = eventsDF.map(Event.parse)
    events.collect.length must ===(5)
  }

  "Impressions DataFrame" should "allow parsing" in {
    val impressions = impressionsDF.map(Impression.parse)
    impressions.collect.length must ===(1)
  }

  "Both DataFrames" should "allow full outer join by advertiser and user" in {
    val events = eventsDF.map(Event.parse).map(event => ((event.advertiserId, event.userId), event))
    val impressions = impressionsDF.map(Impression.parse).map(impression => ((impression.advertiserId, impression.userId), impression))
    val eventImpressions = impressions.fullOuterJoin(events).groupByKey()

    eventImpressions.foreach(println)
    eventImpressions.collect.length must ===(3)
  }

  it should "allow attribution" in {
    val events = eventsDF.map(Event.parse).map(event => ((event.advertiserId, event.userId), event))
    val impressions = impressionsDF.map(Impression.parse).map(impression => ((impression.advertiserId, impression.userId), impression))
    val eventImpressionJoin = impressions.fullOuterJoin(events)
    val advUserEvents = eventImpressionJoin.groupByKey().mapValues(attributedEvents)

    val countOfEvents = getCountOfEvents(advUserEvents)
    countOfEvents.foreach(println)
    countOfEvents.collect.length must ===(1)

    val countOfUsers = getCountOfUsers(advUserEvents)
    countOfUsers.foreach(println)
    countOfUsers.collect.length must ===(1)
  }

}