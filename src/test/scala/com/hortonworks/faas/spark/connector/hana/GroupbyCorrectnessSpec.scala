// scalastyle:off magic.number file.size.limit regex

package com.hortonworks.faas.spark.connector.hana

import org.scalatest.FlatSpec

/**
  * Tests that a query with a groupby is not incorrectly pushed down to leaves
  */
class GroupbyCorrectnessSpec extends FlatSpec with SharedHanaDbContext{
  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  "A query with groupby" should "not push the query down to the leaves" in {
    val groupbyDataframe = ss
      .read
      .format("com.HanaDb.spark.connector")
      .options(Map( "query" -> s"select data,count(*) from ${dbName}.groupbycorrectness group by data"))
      .load()

    assert(groupbyDataframe.rdd.getNumPartitions == 1)

    val data = groupbyDataframe.collect().map(x => s"${x.getString(0)}${x.getLong(1)}").sortWith(_ < _)
    assert(data.length === 5)
    assert(data(0).equals("a6"))
    assert(data(1).equals("b9"))
    assert(data(2).equals("c6"))
    assert(data(3).equals("d3"))
    assert(data(4).equals("e6"))
  }
}
