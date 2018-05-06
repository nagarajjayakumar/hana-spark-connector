package com.hortonworks.faas.spark.connector.hana

import java.sql.{Connection, PreparedStatement, Statement}

import com.hortonworks.faas.spark.connector.hana.config.HanaDbConnectionPool
import com.hortonworks.faas.spark.connector.hana.util.HanaDbConnectionInfo
import com.hortonworks.faas.spark.connector.util.JDBCImplicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait TestBase {
  val dbName: String = {
    // Generate a unique database name based on this machine
    // This is so that multiple people can run tests against
    // the same HanaDb cluster.
//    val hostMD5 = MessageDigest.getInstance("md5").digest(
//      InetAddress.getLocalHost.getAddress)
//    "connector_tests_" + hostMD5.slice(0, 2).map("%02x".format(_)).mkString
    "SLTECC"
  }

  val masterHost = sys.env.get("HANADB_HOST_TEST").getOrElse("127.0.0.1")
  val masterConnectionInfo: HanaDbConnectionInfo =
    HanaDbConnectionInfo(masterHost, 30015, "SYS_VDM", "Cnct2VDM4", dbName) // scalastyle:ignore
  val leafConnectionInfo: HanaDbConnectionInfo =
    HanaDbConnectionInfo(masterHost, 30015, "SYS_VDM", "Cnct2VDM4", dbName) // scalastyle:ignore

  var ss: SparkSession = null
  var sc: SparkContext = null

  def sparkUp(local: Boolean=false): Unit = {
    //recreateDatabase

    var conf = new SparkConf()
      .setAppName("HanaDb Connector Test")
      .set("spark.hanadb.host", masterConnectionInfo.dbHost)
      .set("spark.hanadb.port", masterConnectionInfo.dbPort.toString)
      .set("spark.hanadb.user", masterConnectionInfo.user)
      .set("spark.hanadb.password", masterConnectionInfo.password)
      .set("spark.hanadb.defaultDatabase", masterConnectionInfo.dbName)

    if (local) {
      conf = conf.setMaster("local")
    }

    ss = SparkSession.builder().config(conf).getOrCreate()
    sc = ss.sparkContext
  }

  def sparkDown: Unit = {
    ss.stop()
    sc.stop()
    ss = null
    sc = null
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  def withStatement[T](handle: Statement => T): T =
    withConnection(conn => conn.withStatement(handle))

  def withStatement[T](info: HanaDbConnectionInfo)(handle: Statement => T): T =
    withConnection(info)(conn => conn.withStatement(handle))

  def withPreparedStatement[T](query: String, handle: PreparedStatement => T): T =
    withConnection(conn => conn.withPreparedStatement(query, handle))

  def withConnection[T](handle: Connection => T): T =
    withConnection[T](masterConnectionInfo)(handle)

  def withConnection[T](info: HanaDbConnectionInfo)(handle: Connection => T): T = {
    HanaDbConnectionPool.withConnection(info)(handle)
  }

  def recreateDatabase: Unit = {
    withConnection(masterConnectionInfo.copy(dbName=""))(conn => {
      conn.withStatement(stmt => {
        stmt.execute("DROP DATABASE IF EXISTS " + dbName)
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName)
      })
    })
  }

  def sqlExec(query: String, params: Any*): Unit = {
    withPreparedStatement(query, stmt => {
      stmt.fillParams(params)
      stmt.execute()
    })
  }
}
