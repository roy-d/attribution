package lab

import org.apache.spark.rdd.RDD

object Transformers {

  def deDupeEventsByTime(es: Iterable[Event], deDuplicationTimeThreshold: Int = 60000): Seq[Event] = es
    .toSeq.sortWith(_.timestamp < _.timestamp)
    .foldLeft(List[Event]()) { case (accum: List[Event], event: Event) =>
      accum match {
        case Nil => List(event)
        case e :: Nil =>
          if ((event.timestamp - e.timestamp) < deDuplicationTimeThreshold) accum else accum :+ event
        case _ =>
          val front = accum.dropRight(1)
          val e = accum.last
          if ((event.timestamp - e.timestamp) < deDuplicationTimeThreshold) accum else front :+ event
      }
    }

  def getDeDupedEvents(events: RDD[Event]): RDD[Event] = events
    .map(event => ((event.userId, event.advertiserId, event.eventType), event))
    .groupByKey()
    .flatMap { case (_, vs) => deDupeEventsByTime(vs) }

  def attributedEvents(ps: Iterable[(Option[Impression], Option[Event])]): Seq[Event] = ps
    .filter { case (io, eo) =>
      val isAttributed = for {i <- io; e <- eo} yield i.timestamp < e.timestamp
      isAttributed.getOrElse(false)
    }
    .map { case (_, e) => e.get }
    .toSeq

  def getCountOfEvents(advUserEvents: RDD[((Int, String), Seq[Event])]): RDD[(Int, String, Int)] = advUserEvents
    .flatMap { case ((a, _), es) => es.map(e => ((a, e.eventType), e)) }
    .groupByKey
    .mapValues(_.map(_.eventId).toSet.size)
    .map { case ((a, et), c) => (a, et, c) }

  def getCountOfUsers(advUserEvents: RDD[((Int, String), Seq[Event])]): RDD[(Int, String, Int)] = advUserEvents
    .flatMap { case ((a, u), es) => es.map(e => ((a, e.eventType), u)) }
    .groupByKey
    .mapValues(_.toSet.size)
    .map { case ((a, et), c) => (a, et, c) }
}
