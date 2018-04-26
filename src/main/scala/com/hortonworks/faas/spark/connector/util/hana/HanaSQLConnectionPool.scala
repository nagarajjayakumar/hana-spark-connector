package com.hortonworks.faas.spark.connector.util.hana

import java.util.concurrent.ConcurrentHashMap
import java.sql.{Connection, DriverManager}

import com.hortonworks.faas.spark.connector.util.Loan
import org.apache.commons.dbcp2.BasicDataSource

object MemSQLConnectionPool {
  val DEFAULT_JDBC_LOGIN_TIMEOUT = 10 //seconds
  val pools: ConcurrentHashMap[HanaSQLConnectionInfo, BasicDataSource] = new ConcurrentHashMap

  def createPool(info: HanaSQLConnectionInfo): BasicDataSource = {
    DriverManager.setLoginTimeout(DEFAULT_JDBC_LOGIN_TIMEOUT)

    val newPool = new BasicDataSource
    newPool.setDriverClassName("com.mysql.jdbc.Driver")
    newPool.setUrl(info.toJDBCAddress)
    newPool.setUsername(info.user)
    newPool.setPassword(info.password)
    newPool.addConnectionProperty("zeroDateTimeBehavior", "convertToNull")
    newPool.setMaxTotal(-1)
    newPool.setMaxConnLifetimeMillis(1000 * 60 * 60)

    newPool
  }

  def connect(info: HanaSQLConnectionInfo): Connection = {
    if (!pools.containsKey(info)) {
      val newPool = createPool(info)
      pools.putIfAbsent(info, newPool)
    }
    pools.get(info).getConnection
  }

  def withConnection[T](info: HanaSQLConnectionInfo)(handle: Connection => T): T =
    Loan[Connection](connect(info)).to(handle)
}
