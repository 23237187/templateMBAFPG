package zte.MBA.fpg

import io.prediction.controller.PPreparator
import io.prediction.data.storage.Event

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def sampleWindowData[T](iter: Iterator[ActionDataTuple]) : Iterator[List[String]] = {
    var actionWindowsList = List[List[String]]()
    var actionList = List[String]()

    var windowStart = iter.next.time_stamp
    var windowEnd = windowStart + 30 * 60 * 1000
    while (iter.hasNext) {
      val currentActionTuple = iter.next
      val currentAppId = currentActionTuple.app_id
      val time_stamp = currentActionTuple.time_stamp

      if (time_stamp <= windowEnd) {
        actionList.::(currentAppId)
      } else if ((time_stamp - windowEnd) <= 3 * 60 * 1000) {
        actionList.::(currentAppId)
        windowEnd = time_stamp
      } else {
        windowStart = time_stamp
        windowEnd = windowStart + 30 * 60 * 1000
        actionWindowsList.::(actionList)
        actionList = List[String]()
      }
    }

    actionWindowsList.iterator
  }



  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    val windowedRDD = trainingData.actions.mapPartitions(sampleWindowData)
    .map { actionWindow =>
      actionWindow.distinct
    }.map { distictedActionWindow =>
      distictedActionWindow.toArray
    }

    PreparedData(windowedRDD)
  }
}

case class PreparedData(
  val actionWindows: RDD[Array[String]]
) extends Serializable