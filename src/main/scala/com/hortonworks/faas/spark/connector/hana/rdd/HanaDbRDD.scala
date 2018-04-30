package com.hortonworks.faas.spark.connector.hana.rdd

import java.sql.ResultSet

import com.hortonworks.faas.spark.connector.hana.config.{HanaDbCluster, HanaDbConnectionPool}
import com.hortonworks.faas.spark.connector.hana.util.HanaDbConnectionInfo
import com.hortonworks.faas.spark.connector.util.JDBCImplicits._
import com.hortonworks.faas.spark.connector.util.NextIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag



class HanaDbRDDPartition(override val index: Int,
                          val connectionInfo: HanaDbConnectionInfo,
                          val query: Option[String]=None
                        ) extends Partition

case class ExplainRow(selectType: String, extra: String, query: String)

/**
  * An [[org.apache.spark.rdd.RDD]] that can read data from a HanaDb database based on a SQL query.
  *
  * If the given query supports it, this RDD will read data directly from the
  * HanaDb cluster's leaf nodes rather than from the master aggregator, which
  * typically results in much faster reads.  However, if the given query does
  * not support this (e.g. queries involving joins or GROUP BY operations), the
  * results will be returned in a single partition.
  *
  * @param cluster A connected HanaDbCluster instance.
  * @param sql The text of the query. Can be a prepared statement template,
  *    in which case parameters from sqlParams are substituted.
  * @param sqlParams The parameters of the query if sql is a template.
  * @param namespaceName Optionally provide a database name for this RDD.
  *                     This is required for Partition Pushdown
  * @param mapRow A function from a ResultSet to a single row of the desired
  *   result type(s).  This should only call getInt, getString, etc; the RDD
  *   takes care of calling next.  The default maps a ResultSet to an array of
  *   Any.
  */
case class HanaDbRDD[T: ClassTag](@transient sc: SparkContext,
                                   cluster: HanaDbCluster,
                                   sql: String,
                                   sqlParams: Seq[Any] = Nil,
                                   namespaceName: Option[String] = None,
                                   mapRow: (ResultSet) => T = HanaDbRDD.resultSetToArray _,
                                   disablePartitionPushdown: Boolean = false,
                                   enableStreaming: Boolean = false
                                 ) extends RDD[T](sc, Nil){

  override def getPartitions: Array[Partition] = {
    if(disablePartitionPushdown) {
      getSinglePartition
    } else {
      getSinglePartition
    }
  }

  private def getSinglePartition: Array[Partition] = {
    Array[Partition](new HanaDbRDDPartition(0, cluster.getHanaInfo))
  }


  override def compute(sparkPartition: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener(context => closeIfNeeded())

    val partition: HanaDbRDDPartition = sparkPartition.asInstanceOf[HanaDbRDDPartition]

    val (query, queryParams) = partition.query match {
      case Some(partitionQuery) => (partitionQuery, Nil)
      case None => (sql, sqlParams)
    }

    val conn = HanaDbConnectionPool.connect(partition.connectionInfo)
    val stmt = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    if (enableStreaming) {
      // setFetchSize(Integer.MIN_VALUE) enables row-by-row streaming for the MySQL JDBC connector
      // https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
      // Currently, it does not allow custom fetch size
      stmt.setFetchSize(Integer.MIN_VALUE)
    }
    stmt.fillParams(queryParams)

    val rs = stmt.executeQuery

    override def getNext: T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (stmt != null && !stmt.isClosed) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (conn != null && !conn.isClosed) {
          conn.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }

  override def getPreferredLocations(sparkPartition: Partition): Seq[String] = {
    val partition = sparkPartition.asInstanceOf[HanaDbRDDPartition]
    Seq(partition.connectionInfo.dbHost)
  }

}

object HanaDbRDD {
  def resultSetToArray(rs: ResultSet): Array[Any] = rs.toArray

}
