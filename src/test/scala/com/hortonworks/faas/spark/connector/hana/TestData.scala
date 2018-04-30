// scalastyle:off magic.number file.size.limit regex

package com.hortonworks.faas.spark.connector.hana

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import com.hortonworks.faas.spark.connector.hana.sql.ColumnDefinition

object TestData {
  case class HanaDbType(sqlType: String, sampleValues: Seq[Any]) {
    val name = "val_" + sqlType.replace("(", "_").replace(")", "").replace(",", "_")
    val columnDefn = ColumnDefinition(name, sqlType)
  }

  val utcOffset = -1 * TimeZone.getDefault.getRawOffset
  val HanaDbTypes: Seq[HanaDbType] = Seq(
    HanaDbType("int", Seq(0, 2, 1041241)),
    HanaDbType("bigint", Seq(0L, 5L, 123123123L)),
    HanaDbType("tinyint", Seq(0.toShort, 7.toShort, 40.toShort)),
    HanaDbType("smallint", Seq(0.toShort, 64.toShort, 100.toShort)),
    HanaDbType("text", Seq("aasdfasfasfasdfasdfa", "", "ʕ ᓀ ᴥ ᓂ ʔ")),
    HanaDbType("blob", Seq("e", "f", "g")),
    HanaDbType("bool", Seq(0.toShort, 1.toShort, 1.toShort)),
    HanaDbType("char(1)", Seq("a", "b", "c")),
    HanaDbType("varchar(100)", Seq("do", "rae", "me")),
    HanaDbType("varbinary(100)", Seq("one", "two", "three")),
    HanaDbType("decimal(20,10)", Seq(BigDecimal(3.00033358), BigDecimal(3.442), BigDecimal(121231.12323))),
    HanaDbType("real", Seq(0.5, 2.3, 123.13451)),
    HanaDbType("double", Seq(0.3, 2.7, 234324.2342)),
    HanaDbType("float", Seq(0.5f, 3.4f, 123.1234f)),
    HanaDbType("datetime", Seq(new Timestamp(utcOffset), new Timestamp(1449615940000L), new Timestamp(1049615940000L))),
    HanaDbType("timestamp", Seq(new Timestamp(utcOffset), new Timestamp(1449615940000L), new Timestamp(1049615940000L))),
    HanaDbType("date", Seq(new Date(90, 8, 23), new Date(100, 3, 5), new Date(utcOffset)))
  )

}
