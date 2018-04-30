package com.hortonworks.faas.spark.connector.hana

import java.util.TimeZone

import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `HanaDbContext` between all tests in a suite and closes it at the end */
trait SharedHanaDbContext extends TestBase with BeforeAndAfterAll {self: Suite =>
  override def beforeAll() {
    sparkUp(local = true)

    val timeZone = TimeZone.getTimeZone("GMT")
    TimeZone.setDefault(timeZone)

    super.beforeAll()
  }

  override def afterAll() {
    sparkDown
    super.afterAll()
  }

}
