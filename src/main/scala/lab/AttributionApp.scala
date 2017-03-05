package lab

import java.io.File

import lab.Transformers._
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AttributionApp extends App {
  // FILES and DIRS SETUP
  val outputNames = List("count_of_events", "count_of_users", "de_duped_events")
  outputNames.foreach { name =>
    FileUtils.deleteQuietly(new File(s"data/output/$name.csv"))
  }
  val runId = System.currentTimeMillis()
  def outPath(name: String) = s"data/runs/$runId/$name"

  // SPARK SETUP
  val conf = new SparkConf().setMaster("local[4]").set("spark.local.ip", "127.0.0.1").set("spark.driver.host", "127.0.0.1").setAppName("AttributionApp")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  // EVENTS
  val events: RDD[Event] = sqlContext.read.format("com.databricks.spark.csv").schema(Event.schema)
    .load("data/input/events.csv").map(Event.parse)
  val deDupedEvents = getDeDupedEvents(events) // OPTIMIZATION based on Working Assumption #1
  deDupedEvents.coalesce(1, shuffle = true).toDF.write.format("com.databricks.spark.csv")
    .save(outPath("de_duped_events"))
  val eventsPair = deDupedEvents.map(event => ((event.advertiserId, event.userId), event))

  // IMPRESSIONS
  val impressions = sqlContext.read.format("com.databricks.spark.csv").schema(Impression.schema)
    .load("data/input/impressions.csv").map(Impression.parse)
  val impressionsPair = impressions.map(impression => ((impression.advertiserId, impression.userId), impression))
  val firstImpressions = impressionsPair
    .groupByKey.mapValues(firstImpression) // OPTIMIZATION based on Working Assumption #2 

  // ATTRIBUTION
  val eventImpressionJoin = firstImpressions.fullOuterJoin(eventsPair)
  val advUserEvents: RDD[((Int, String), Seq[Event])] = eventImpressionJoin.groupByKey().mapValues(attributedEvents)
  advUserEvents.cache()

  // STATISTICS
  val countOfEvents: RDD[(Int, String, Int)] = getCountOfEvents(advUserEvents)
  countOfEvents.coalesce(1, shuffle = true).toDF.write.format("com.databricks.spark.csv")
    .save(outPath("count_of_events"))

  val countOfUsers: RDD[(Int, String, Int)] = getCountOfUsers(advUserEvents)
  countOfUsers.coalesce(1, shuffle = true).toDF.write.format("com.databricks.spark.csv")
    .save(outPath("count_of_users"))

  // SPARK STOP
  sc.stop()

  // RESULTS
  outputNames.foreach { name =>
    FileUtils.copyFile(new File(s"${outPath(name)}/part-00000"), new File(s"data/output/$name.csv"))
  }

}
