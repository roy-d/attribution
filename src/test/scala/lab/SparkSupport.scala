package lab

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object SparkSupport {
  private val conf =
    new SparkConf()
      .setMaster("local[4]")
      .set("spark.local.ip", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")
      .setAppName("AttributionTestApp")
  lazy val sc: SparkContext = new SparkContext(conf)
  lazy val sqlContext: SQLContext = new SQLContext(sc)
}

trait SparkSupport {
  def sc: SparkContext = SparkSupport.sc

  implicit def sqlContext: SQLContext = SparkSupport.sqlContext

  def waitFor[T](future: Future[T], duration: Duration = 5.second): T =
    Await.result(future, duration)
}
