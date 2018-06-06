# hana-spark-connector

Spark connector enables users to load data from HANA tables into Spark Dataframes.

use maven to build the connector code

Pass test skip flag in case if you need to skip Test case execution during install phase

 <code>mvn clean install -Dmaven.test.skip=true </code> <br/>
 
usage

Query Mode : 

```
 val mandtCount = ss
        .read
        .format("com.hortonworks.faas.spark.connector")
        .options(Map("query" -> ("select MANDT, count(*) from " +   hanaNameSpace + "." + tableName+ " GROUP BY MANDT"),
        "database" -> hanaNameSpace))
```  

Direct Table Access Mode :
```

 val loadT352T_T = ss
        .read
        .format("com.hortonworks.faas.spark.connector")
        .options(Map("path" -> ( hanaNameSpace + "." + tableName)))
        .load()
        
 println(s"The record count for MANDT SAP table is ${loadT352T_T.count()}")
 
 loadT352T_T.show()
      
```

Test Scenario

HanaDbConnectionInfo -  JDBC URL

``` 
    var address = s"jdbc:sap://$dbHost:$dbPort"

```

HanaDbConnectionPool  - JDBC Driver

```
    newPool.setDriverClassName("com.sap.db.jdbc.Driver")
```
To Run the Docker Application
./run.sh

To Run the Application in Docker 
./run.sh 


SIGN: NAGA JAY

CICD Contributions: YOGESH {@yogeshprabhu}

