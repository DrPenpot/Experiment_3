###阶段3
---

尝试用bin/spark-sql连接Hive进行实验

将Hadoop下的`core-site.xml`,`hdfs-site.xml`以及Hive下的`hive-site.xml`复制进spark文件夹下的`conf/`，再运行`bin/spark-sql`

报错`The specified datastore driver ("com.mysql.jdbc.Driver") was not found in the CLASSPATH. Please check your CLASSPATH specification, and the name of the driver.`.0

问题在于启动的时候没有配置mysql的驱动地址，尝试用如下命令启动：`bin/spark-sql --jars /usr/local/hive/lib/mysql-connector-java-5.1.40-bin.jar`仍然失败

果断放弃

---

在`bin/spark-shell`下实验

```scala
import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

val million_user_log = spark.read.format("csv").option("sep",",").option("header","false").option("encoding","UTF-8").load("input/million_user_log.csv")
```

这样即可导入数据，问题在于这时列名为`_c0`,`_c1`等等，虽然可以继续操作，但是多有不便。可以使用.toDF命令(或者自建一个Schema，这里选用.toDF()方法)来改变列名。

```
mlog = million_user_log.toDF("user_id", "item_id", "cat_id", "merchant_id", "brand_id", "month", "day", "action", "age_range", "gender", "province")
```

使用.show()发现最后一行省份出现编码问题，在spark-shell上显示为？

```scala
scala> mlog.show()
+-------+-------+------+-----------+--------+-----+---+------+---------+------+--------+
|user_id|item_id|cat_id|merchant_id|brand_id|month|day|action|age_range|gender|province|
+-------+-------+------+-----------+--------+-----+---+------+---------+------+--------+
| 328862| 406349|  1280|       2700|    5476|   11| 11|     0|        0|     1|    ??|
| 328862| 406349|  1280|       2700|    5476|   11| 11|     0|        7|     1|  ???|
| 328862| 807126|  1181|       1963|    6109|   11| 11|     0|        1|     0|  ???|
| 328862| 406349|  1280|       2700|    5476|   11| 11|     2|        6|     0|    ??|
| 328862| 406349|  1280|       2700|    5476|   11| 11|     0|        6|     2|    ??|
| 328862| 406349|  1280|       2700|    5476|   11| 11|     0|        4|     1|    ??|
| 328862| 406349|  1280|       2700|    5476|   11| 11|     0|        5|     0|    ??|
| 328862| 406349|  1280|       2700|    5476|   11| 11|     0|        3|     2|    ??|
| 328862| 406349|  1280|       2700|    5476|   11| 11|     0|        7|     1|    ??|
| 234512| 399860|   962|        305|    6300|   11| 11|     0|        4|     1|    ??|
| 234512| 240182|    81|       3018|    4144|   11| 11|     2|        1|     2|  ???|
| 234512| 137298|  1432|       3271|    6957|   11| 11|     2|        3|     1|    ??|
| 234512| 245026|   389|        830|    6866|   11| 11|     0|        0|     1|  ???|
| 234512| 179830|  1208|        546|    2276|   11| 11|     2|        1|     0|    ??|
| 234512| 817066|  1611|       2468|    1392|   11| 11|     0|        2|     2|  ???|
| 234512| 568730|   420|       2468|    1392|   11| 11|     0|        7|     2|    ??|
| 234512|  34570|  1611|       2468|    1392|   11| 11|     0|        3|     1|    ??|
| 234512| 137298|  1432|       3271|    6957|   11| 11|     0|        6|     2|    ??|
| 234512| 719701|   389|       4655|    3157|   11| 11|     0|        2|     2|    ??|
| 234512| 137298|  1432|       3271|    6957|   11| 11|     0|        7|     0|    ??|
+-------+-------+------+-----------+--------+-----+---+------+---------+------+--------+
only showing top 20 rows
```

```scala
val mlog = million_user_log //为了方便，把名字改短一点
```

```scala
case class Log(user_id: Int, item_id: Int, cat_id: Int, merchant_id: Int, brand_id: Int, month: Int, day: Int, action: Int, age_range: Int, gender: Int, province: String)

val mlog = million_user_log.as[Log]

```
---

####查询双11那天浏览次数前十的品牌

没有解决乱码问题，只好先从这个问题入手

```
scala> mlog.createOrReplaceTempView("mlog")

scala> val sqlLog = spark.sql("SELECT brand_id,COUNT(*) FROM mlog WHERE action = 0 GROUP BY brand_id")
sqlLog: org.apache.spark.sql.DataFrame = [brand_id: string, count(1): bigint]

scala> sqlLog.sort(desc("count(1)")).show()
+--------+--------+                                                             
|brand_id|count(1)|
+--------+--------+
|    1360|   49151|
|    3738|   10130|
|      82|    9719|
|    1446|    9426|
|    6215|    8568|
|    1214|    8470|
|    5376|    8282|
|    2276|    7990|
|    1662|    7808|
|    8235|    7661|
|    6065|    5644|
|    3969|    5603|
|    4705|    5572|
|    4290|    5524|
|    6585|    5264|
|    3700|    5126|
|    6742|    4555|
|    2104|    4400|
|    5434|    4384|
|    7995|    4273|
+--------+--------+
only showing top 20 rows
```

和阶段二的结果相符

---

####
