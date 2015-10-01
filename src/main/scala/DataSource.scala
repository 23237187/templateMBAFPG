package zte.MBA.fpg

import grizzled.slf4j.Logger
import io.prediction.controller.{EmptyActualResult, EmptyEvaluationInfo, PDataSource, Params}
import io.prediction.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // read all events of EVENT involving ENTITY_TYPE and TARGET_ENTITY_TYPE
    val actionsRDD: RDD[List[ActionDataTuple]] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("3")))(sc)
    .map{ event =>
      ActionDataTuple(
        usr_id = event.entityId.toInt,
        app_id = event.targetEntityId.get,
        time_stamp = event.eventTime.getMillis
      )
    }
    .map { actionData =>
      (actionData.usr_id, actionData)
    }
    .groupByKey()
    .mapValues { userActionsTuples =>
      userActionsTuples.toList.sortBy { action =>
        action.time_stamp
      }
    }
    .values


//    actionsRDD.coalesce(1).saveAsTextFile("/ZTE_DEMO/WOW")


    new TrainingData(actionsRDD)
  }
}

case class ActionDataTuple(
  usr_id: Int,
  app_id: String,
  time_stamp: Long
  )

class TrainingData(
  val actions: RDD[List[ActionDataTuple]]
) extends Serializable {
}