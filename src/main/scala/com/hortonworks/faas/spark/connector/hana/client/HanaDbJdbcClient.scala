package com.hortonworks.faas.spark.connector.hana.client

import java.sql.{Connection, ResultSet}

import com.hortonworks.faas.spark.connector.hana.client.hana.metaAttr
import com.hortonworks.faas.spark.connector.hana.config.{HanaDbConf, HanaDbConnectionPool}
import com.hortonworks.faas.spark.connector.hana.util.{HANAJdbcConnectionException, HANAJdbcException, HanaDbConnectionInfo}
import com.hortonworks.faas.spark.connector.util.{ExecuteWithExceptions, WithCloseables}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

case class HanaDbJdbcClient(hanaConfiguration: HanaDbConf)  {
  private val log: Logger = LoggerFactory.getLogger(classOf[HanaDbJdbcClient])

  protected val driver: String = "com.sap.db.jdbc.Driver"

  private val CONNECTION_FAIL_R = ".*Failed to open connection.*".r

  def getHanaInfo: HanaDbConnectionInfo = hanaConfiguration.hanaConnectionInfo


  def withHanaConn[T]: ((Connection) => T) => T =
    HanaDbConnectionPool.withConnection(getHanaInfo)

  /**
   * Checks whether the provided exception is a connection opening failure one.
   * This method is marked as protected in order to be overrideable in the tests.
   *
   * @param ex The exception to check
   * @return `true` if the exception is a connection failure one, `false` otherwise
   */
  protected def isFailedToOpenConnectionException(ex: Throwable): Boolean = ex match {
    case e if e.getMessage == null => false
    case _: RuntimeException => CONNECTION_FAIL_R.pattern.matcher(ex.getMessage).matches()
    case _ => false
  }

  /**
   * Returns a fully qualified table name.
   *
   * @param namespace The table namespace
   * @param tableName The table name
   * @return the fully qualified table name
   */
  def tableWithNamespace(namespace: Option[String], tableName: String) =
    namespace match {
      case Some(value) => s""""$value"."$tableName""""
      case None => tableName
    }

  /**
   * Returns the connection URL. This method is marked as protected in order to be
   * overrideable in the tests.
   *
   * @return The HANA JDBC connection URL
   */
  protected def jdbcUrl: String = {
    getHanaInfo.toJDBCAddress
  }

  /**
   * Returns a JDBC connection object. This method is marked as protected in order to be
   * overrideable in the tests.
   *
   * @return The created JDBC [[Connection]] object
   */
   def getConnection: Connection = {
     HanaDbConnectionPool.connect(getHanaInfo)
  }

  /**
   * Retrieves table metadata.
   *
   * @param tableName The table name
   * @param namespace The table namespace
   * @return A sequence of [[metaAttr]] objects (JDBC representation of the schema)
   */
   def getMetaData(tableName: String, namespace: Option[String]): Seq[metaAttr] = {
     ExecuteWithExceptions[Seq[metaAttr], Exception, HANAJdbcException](
      new HANAJdbcException(s"Fetching of metadata for $tableName failed")) { () =>
       val fullTableName = tableWithNamespace(namespace, tableName)
       WithCloseables(getConnection) { conn =>
         WithCloseables(conn.createStatement()) { stmt =>
           WithCloseables(stmt.executeQuery(s"SELECT * FROM $fullTableName LIMIT 0")) { rs =>
             val metadata = rs.getMetaData
             val columnCount = metadata.getColumnCount
             (1 to columnCount).map(col => metaAttr(
               metadata.getColumnName(col), metadata.getColumnType(col),
               metadata.isNullable(col), metadata.getPrecision(col),
               metadata.getScale(col), metadata.isSigned(col)))
           }
         }
       }
     }
  }

  /**
    * Retrieves query metadata.
    *
    * @param query The SQL Query string
    * @return A sequence of [[metaAttr]] objects (JDBC representation of the schema)
    */
  def getMetadata(query: String): Seq[metaAttr] = {
    ExecuteWithExceptions[Seq[metaAttr], Exception, HANAJdbcException](
      new HANAJdbcException(s"Fetching of metadata for $query failed")) { () =>
        val fullQueryForMetadata = query + " LIMIT 0"
        WithCloseables(getConnection) { conn =>
          WithCloseables(conn.createStatement()) { stmt =>
            WithCloseables(stmt.executeQuery(fullQueryForMetadata)) { rs =>
              val metadata = rs.getMetaData
              val columnCount = metadata.getColumnCount
              (1 to columnCount).map(col => metaAttr(
                metadata.getColumnName(col), metadata.getColumnType(col),
                metadata.isNullable(col), metadata.getPrecision(col),
                metadata.getScale(col), metadata.isSigned(col)))
            }
          }
        }
      }
  }


  /**
   * Checks if the given table name corresponds to an existent
   * table in the HANA backend within the provided namespace.
   *
   * @param namespace (optional) The table namespace
   * @param tableName The name of the table
   * @return `true`, if the table exists; `false`, otherwise
   */
  def tableExists(namespace: Option[String], tableName: String): Boolean =
    ExecuteWithExceptions[Boolean, Exception, HANAJdbcException] (
      new HANAJdbcException(s"Checking if table $tableName exists in vora backend failed")) { () =>
      WithCloseables(getConnection) { conn =>
        val dbMetaData = conn.getMetaData
        val tables = dbMetaData.getTables(null, namespace.orNull, tableName, null)
        tables.next()
      }
    }

  /**
   * Returns existing HANA tables which names match the provided pattern and which were created
   * within any schema matching the provided schema pattern.
   *
   * @param dbschemaPattern The schema patters
   * @param tableNamePattern The table name pattern
   * @return [[Success]] with a sequence of the matching tables' names or
   *         [[Failure]] if any error occurred
   */
  def getExistingTables(dbschemaPattern: String, tableNamePattern: String): Try[Seq[String]] =
    ExecuteWithExceptions[Try[Seq[String]], Exception, HANAJdbcException] (
      new HANAJdbcException(s"Retrieving exisiting VORA tables failed matching schema $dbschemaPattern and table $tableNamePattern failed")) { () =>
      WithCloseables(getConnection) { conn =>
        WithCloseables(conn.createStatement()) { stmt =>
          Try {
            val rs = stmt.executeQuery(s"SELECT TABLE_NAME FROM M_TABLES " +
              s"WHERE SCHEMA_NAME LIKE '$dbschemaPattern' AND TABLE_NAME LIKE '$tableNamePattern'")
            val tablesNames = Stream.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
            rs.close()
            tablesNames
          }
        }
      }
    }

  /**
    * Retrieves existing schema information for the given database and table name.
    *
    * @param dbschemaPattern The database schema to filter for.
    * @param tableNamePattern The table name to filter for.
    * @return The [[ColumnRow]]s returned by the query.
    */
  def getExistingSchemas(dbschemaPattern: String, tableNamePattern: String): Seq[ColumnRow] =
    ExecuteWithExceptions[Seq[ColumnRow], Exception, HANAJdbcException] (
      new HANAJdbcException(s"retrieving schema for database $dbschemaPattern and table name $tableNamePattern failed")) { () =>
      WithCloseables(getConnection) { conn =>
        WithCloseables(conn.createStatement()) { stmt =>
          WithCloseables(stmt.executeQuery(
            s"""SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DATA_TYPE_NAME,
              |LENGTH, SCALE, POSITION, IS_NULLABLE
              |FROM TABLE_COLUMNS
              |WHERE SCHEMA_NAME LIKE '$dbschemaPattern' AND TABLE_NAME LIKE '$tableNamePattern'
           """.stripMargin)) { rs =>
              val iterator = new Iterator[ResultSet] {
              def hasNext: Boolean = rs.next()

                def next(): ResultSet = rs
            }
            // scalastyle:off magic.number of the following numbers.
            iterator.map { rs =>
              ColumnRow(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3),
                rs.getString(
                  4),
                rs.getInt(5),
                rs.getInt(6),
                rs.getInt(7),
                rs.getBoolean(8))
            }.toList
            // scalastyle:on magic.number
          }
        }
      }
    }


  /**
    *
    * @param tableNameOrSubquery table name or subquery in HANA
    */
  def getQueryStatistics(tableNameOrSubquery: String): Int = {
    ExecuteWithExceptions[Int, Exception, HANAJdbcException] (
      new HANAJdbcException(s"cannot fetch query statistics for table or subquery $tableNameOrSubquery")) { () =>
      WithCloseables(getConnection) { conn =>
        WithCloseables(conn.createStatement()) { stmt =>
          WithCloseables(stmt.executeQuery(s"select count(*) as NUM_ROWS from $tableNameOrSubquery")) {
            resultSet =>
              resultSet.next()
              resultSet.getInt("NUM_ROWS")
          }
        }
      }
    }
  }

  /**
    * Executes a SQL query on HDB tables.
    * @param schema Kafka schema for SourceRecord
    * @param queryString query string to be executed on HANA
    * @param offset offset for the query string
    * @param limit limit for the query string
    * @param isColumnar if the table is columnar
    */
  def executeQuery(schema: Schema, queryString: String, offset: Int, limit: Int,
                   isColumnar: Boolean = false): Option[List[GenericRecord]] = {
    ExecuteWithExceptions[Option[List[GenericRecord]], Exception, HANAJdbcException] (
      new HANAJdbcException(s"execution of query $queryString failed")) { () =>
      WithCloseables(getConnection) { conn =>
        WithCloseables(conn.createStatement()) { stmt =>
          if (limit > 0 && offset > 0) {
            WithCloseables(stmt.executeQuery(s"$queryString LIMIT $limit OFFSET $offset")) { resultSet =>
              fetchResultSet(resultSet, schema)
            }
          } else if (limit > 0) {
            WithCloseables(stmt.executeQuery(s"$queryString LIMIT $limit")) { resultSet =>
              fetchResultSet(resultSet, schema)
            }
          } else {
            WithCloseables(stmt.executeQuery(s"$queryString")) { resultSet =>
              fetchResultSet(resultSet, schema)
            }
          }
        }
      }
    }
  }

  def commit(connection: Connection): Unit = {
    ExecuteWithExceptions[Unit, Exception, HANAJdbcConnectionException] (
      new HANAJdbcConnectionException("commit failed for current connection")) { () =>
        connection.commit()
    }
  }

  private def fetchResultSet(resultSet: ResultSet, schema: Schema): Option[List[GenericRecord]] = {
    var dm = List[GenericRecord]()

    while (resultSet.next()) {
      val metadata = resultSet.getMetaData
      val struct = new GenericData.Record(schema)

      for (i <- 1 to metadata.getColumnCount) {
        val dataType = metadata.getColumnType(i)
        val columnValue = getColumnData(dataType, resultSet, i)
        struct.put(metadata.getColumnName(i), columnValue)
      }
      dm :+= struct
    }
    Some(dm)
  }

  private def getColumnData(columnType: Int, resultSet: ResultSet, index: Int) = {
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
