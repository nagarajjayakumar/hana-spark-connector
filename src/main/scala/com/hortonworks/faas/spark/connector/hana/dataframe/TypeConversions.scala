package com.hortonworks.faas.spark.connector.hana.dataframe

import java.sql.{ResultSet, ResultSetMetaData, Types => JDBCTypes}

import org.apache.spark.sql.types._

object TypeConversions {

  val HANA_SQL_DECIMAL_MAX_PRECISION = 65
  val HANA_SQL_DECIMAL_MAX_SCALE = 30

  def decimalTypeToHanaDbType(decimal: DecimalType): String = {
    val precision = Math.min(HANA_SQL_DECIMAL_MAX_PRECISION, decimal.precision)
    val scale = Math.min(HANA_SQL_DECIMAL_MAX_SCALE, decimal.scale)
    s"DECIMAL($precision, $scale)"
  }

  /**
   * Find the appropriate HanaDb type from a SparkSQL type.
   *
   * Most types share the same name but there are a few special cases because
   * the types don't align perfectly.
   *
   * @param dataType A SparkSQL Type
   * @return Corresponding HanaDb type
   */
  def DataFrameTypeToHanaDbTypeString(dataType: DataType): String = {
    dataType match {
      case ShortType => "SMALLINT"
      case LongType => "BIGINT"
      case BooleanType => "BOOLEAN"
      case StringType => "TEXT"
      case BinaryType => "BLOB"
      case dt: DecimalType => decimalTypeToHanaDbType(dt)
      case _ => dataType.typeName
    }
  }

  def JDBCTypeToDataFrameType(rsmd: ResultSetMetaData, ix: Int): DataType = {
    rsmd.getColumnType(ix) match {
      // scalastyle:off

      case JDBCTypes.TINYINT => IntegerType
      case JDBCTypes.SMALLINT => IntegerType
      case JDBCTypes.INTEGER => if (rsmd.isSigned(ix)) { IntegerType } else { LongType }
      case JDBCTypes.BIGINT => if (rsmd.isSigned(ix)) { LongType } else { DecimalType(20,0) }

      case JDBCTypes.DOUBLE => DoubleType
      case JDBCTypes.NUMERIC => DoubleType
      case JDBCTypes.REAL => FloatType
      case JDBCTypes.DECIMAL => DecimalType(
        math.min(DecimalType.MAX_PRECISION, rsmd.getPrecision(ix)),
        math.min(DecimalType.MAX_SCALE, rsmd.getScale(ix))
      )

      case JDBCTypes.TIMESTAMP => TimestampType
      case JDBCTypes.DATE => DateType
      // MySQL TIME type is represented as a string
      case JDBCTypes.TIME => StringType

      case JDBCTypes.CHAR => StringType
      case JDBCTypes.VARCHAR => StringType
      case JDBCTypes.NVARCHAR => StringType
      case JDBCTypes.LONGVARCHAR => StringType
      case JDBCTypes.LONGNVARCHAR => StringType
      case JDBCTypes.NCLOB => StringType
      case JDBCTypes.CLOB => StringType


      case JDBCTypes.BIT => BinaryType
      case JDBCTypes.BINARY => BinaryType
      case JDBCTypes.VARBINARY => BinaryType
      case JDBCTypes.LONGVARBINARY => BinaryType
      case JDBCTypes.BLOB => BinaryType

      case _ => throw new IllegalArgumentException("Can't translate type " + rsmd.getColumnTypeName(ix))
    }
  }

  def GetJDBCValue(columnType: Int, resultSet: ResultSet, index: Int) = {
    val value = columnType match {
      case java.sql.Types.VARCHAR | java.sql.Types.NVARCHAR |
           java.sql.Types.NCHAR | java.sql.Types.CHAR
           | java.sql.Types.LONGNVARCHAR => resultSet.getString(index)

      case java.sql.Types.BOOLEAN => resultSet.getBoolean(index)

      case java.sql.Types.BINARY | java.sql.Types.VARBINARY |
           java.sql.Types.LONGVARBINARY | java.sql.Types.BLOB => resultSet.getBytes(index)

      case java.sql.Types.TINYINT | java.sql.Types.INTEGER |
           java.sql.Types.SMALLINT => resultSet.getInt(index)

      case java.sql.Types.BIGINT => resultSet.getLong(index)

      case java.sql.Types.FLOAT | java.sql.Types.REAL => resultSet.getFloat(index)

      case java.sql.Types.DECIMAL => resultSet.getString(index) match {
        case null => null
        case value => new java.math.BigDecimal(value)
      }

      case java.sql.Types.DOUBLE => resultSet.getDouble(index)

      case java.sql.Types.DATE => resultSet.getDate(index)
      case java.sql.Types.TIME => resultSet.getTime(index)
      case java.sql.Types.TIMESTAMP => resultSet.getTimestamp(index)

      case java.sql.Types.NCLOB | java.sql.Types.CLOB => resultSet.getString(index)

      case _ => throw new UnsupportedOperationException(s"HANA Jdbc Client doesn't " +
        s"support column types with typecode '$columnType'")
    }

    if (resultSet.wasNull()) {
      null
    } else {
      value
    }
  }
}
