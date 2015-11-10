package zte.MBA.fpg

import io.prediction.controller.PPreparator
import io.prediction.data.storage.Event

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def sampleWindowData(iter: Iterator[List[ActionDataTuple]]) : Iterator[List[String]] = {
    var actionWindowsList = List[List[String]]()
    var actionList = List[String]()

    while(iter.hasNext) {

      var usr_list = iter.next
      var windowStart = usr_list(0).time_stamp
      var windowEnd = windowStart + 30 * 60 * 1000
      usr_list.tail.map { currentActionTuple =>
        val currentAppId = currentActionTuple.app_id
        val time_stamp = currentActionTuple.time_stamp
        if (time_stamp <= windowEnd) {
          actionList = currentAppId :: actionList
        } else if ((time_stamp - windowEnd) <= 3 * 60 * 1000) {
          actionList.::(currentAppId)
          windowEnd = time_stamp
        } else {
          windowStart = time_stamp
          windowEnd = windowStart + 30 * 60 * 1000
          actionWindowsList = actionList :: actionWindowsList
          actionList = List[String]()
        }
      }


    }
    actionWindowsList.iterator
  }



  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
//    println("runing PPor")


    val windowedRDD = trainingData.actions.mapPartitions(sampleWindowData)
    .map { actionWindow =>
      actionWindow.distinct
    }.map { distictedActionWindow =>
      distictedActionWindow.toArray
    }

//    println(windowedRDD.count())

//    windowedRDD.coalesce(1).saveAsTextFile("/ZTE_DEMO/WOW2")


    PreparedData(windowedRDD)
  }
}

case class PreparedData(
  val actionWindows: RDD[Array[String]]
) extends Serializable