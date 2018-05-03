# hana-spark-connector

Spark connector enables users to load data from HANA tables into Spark Dataframes.

use maven to build the connector code

Pass test skip flag in case if you need to skip Test case execution during install phase

 <code>mvn clean install -Dmaven.test.skip=true </code> <br/>
 
usage

<code>
 val mandtCount = ss
        .read
        .format("com.hortonworks.faas.spark.connector")
        .options(Map("query" -> ("select MANDT, count(*) from " +   dbName + "." + name+ " GROUP BY MANDT"),
          "database" -> dbName))
        .load()
</code>  
<br />
<br />

<code>

 val loadT352T_T = ss
        .read
        .format("com.hortonworks.faas.spark.connector")
        .options(Map("path" -> ( dbName + "." + name)))
        .load()
        println(s"The number of MANDT SAP table is ${loadT352T_T.count()}")
        loadT352T_T.show()
      
</code>

<br />
<br />

SIGN: NAGA JAY

