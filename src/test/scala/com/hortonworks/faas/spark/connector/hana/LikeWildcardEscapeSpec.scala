// scalastyle:off magic.number file.size.limit regex

package com.hortonworks.faas.spark.connector.hana

import org.scalatest.{FlatSpec, Matchers}

/**
  * Test that using a String-contains filter returns the correct result when the
  * search string includes "%" and "'", which are wildcards for HanaDb's LIKE operator
  */
class LikeWildcardEscapeSpec extends FlatSpec with SharedHanaDbContext with Matchers {
  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  "StringContains filter" should "correctly escape '%' in SQL pushdown" in {
    val spark = ss
    import spark.implicits._

    val escapePercentResult = ss
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map( "path" -> (dbName + ".wildcardEscape")))
      .load().filter($"data".contains("10%"))
      .collect()

    assert(escapePercentResult.length === 1)
    assert(escapePercentResult(0)(1) === "10% discount")
  }

  it should "correctly escape single quote in SQL pushdown" in {
    val spark = ss
    import spark.implicits._

    val escapeQuoteResult = ss
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map( "path" -> (dbName + ".wildcardEscape")))
      .load().filter($"data".contains("25's"))
      .collect()

    assert(escapeQuoteResult.length === 1)
    assert(escapeQuoteResult(0)(1) === "user 25's data")
  }

  it should "correctly escape '_' in SQL pushdown" in {
    val spark = ss
    import spark.implicits._

    val escapeUnderscoreResult = ss
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map( "path" -> (dbName + ".wildcardEscape")))
      .load().filter($"data".contains("_31"))
      .collect()

    assert(escapeUnderscoreResult.length === 1)
    assert(escapeUnderscoreResult(0)(1) === "test_data_31")
  }
}
