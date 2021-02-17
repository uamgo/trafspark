# trafspark
Trafodion with Spark JdbcRDD

# 在docker中创建Spark环境，可以只创建1个spark-worker  
```
docker run --name spark-master -h spark-master -e ENABLE_INIT_DAEMON=true -d bde2020/spark-master:2.4.0-hadoop2.7  
docker run --name spark-worker-1 --link spark-master:spark-master -e ENABLE_INIT_DAEMON=true -d bde2020/spark-worker:2.4.0-hadoop2.7  
docker run --name spark-worker-2 --link spark-master:spark-master -e ENABLE_INIT_DAEMON=true -d bde2020/spark-worker:2.4.0-hadoop2.7  
```

## 这里是个spark console，可以跳过，不用执行  
```docker run -it --rm bde2020/spark-scala-template sbt console```


# sqlci中创建一张表，用于spark写入操作  
```create table spark(name varchar(1000), age int, phone bigint);```

# 通过spark-shell指定需要的jar包并启动spark命令行  
```./spark/bin/spark-shell --jars /traf-spark-dialect-1.0.jar,/jdbcT4-2.7.0.jar```


# 在spark命令行中执行，这里是创建一个dataframe，产生三行数据，并写入“spark”这张表中：  
```
val df = spark.createDataFrame(Seq(
  ("ming", 20, 15552211521L),
  ("hong", 19, 13287994007L),
  ("zhi", 21, 15552211523L)
)) toDF("name", "age", "phone")

df.show()

df.write.
mode(saveMode="append").
format("org.apache.spark.sql.jdbc.esg").
option("driver","org.trafodion.jdbc.t4.T4Driver").
option("url","jdbc:t4jdbc://10.9.0.220:23400/:").
option("usessl","false").
option("truncate","false").
option("isolationLevel","NONE").
option("dbtable","spark").
option("user","trafodion").
option("password","traf123").
option("numPartitions",20).
option("batchsize","1000").
save();
```

-------
# 方言独立使用说明，此处与上述执行过程无关   
代码调用：   
```
import org.apache.spark.sql.jdbc.TrafT4Dialect
import org.apache.spark.sql.jdbc.JdbcDialects
val dialect = new TrafT4Dialect 
JdbcDialects.registerDialect(dialect)

```
