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

####统计各省销售最好的产品类别前十（销售最多前10的产品类别）

因为spark-shell的中文显示问题，省份一直无法处理，我决定直接对数据集进行预处理，按下表将省份的中文转为一个号码。

```
11 北京市
12 天津市
13 河北
14 山西
15 内蒙古
21 辽宁
22 吉林
23 黑龙江
31 上海市
32 江苏
33 浙江
34 安徽
35 福建
36 江西
37 山东
41 河南
42 湖北
43 湖南
44 广东
45 广西
46 海南
50 重庆市
51 四川
52 贵州
53 云南
54 西藏
61 陕西
62 甘肃
63 青海
64 宁夏
65 新疆
71 台湾
81 香港
82 澳门
```

本来写了MR的程序来处理，但是因为MR好像中文识别也不是很成功，我最后用了很笨的方法，直接在文本编辑器里查找替换。

后来又发现，编码应该只是shell的问题，输出文件只有可以正常显示。

```scala
val mostPurchasedCat = spark.sql("SELECT province, cat_id, COUNT(*) AS purchase_num FROM mlog WHERE action =2 GROUP BY province, cat_id HAVING purchase_num > 57 ORDER BY province, purchase_num DESC")

val mostPurchasedCat_output = mostPurchasedCat.coalesce(1) //避免输出很多小文件，这样把可以把整个表作为一个文件输出

mostPurchasedCat_output.write.mode("overwrite").option("header","true").option("encoding","UTF-8").option("sep",",").csv("file:///usr/FBDP/mostPurchasedCat")
```

输出结果在目录的另一个文件夹下`Experiment_3/stage_3/mostPurchasedCat`

---

#### 统计各省的双十一前十热门销售产品

```scala
val mostPurchasedItemGroupedByProvince = spark.sql("SELECT province, item_id, COUNT(*) as purchase_num FROM mlog WHERE action = 2 GROUP BY province, item_id HAVING purchase_num > 3 ORDER BY province, purchase_num DESC")

val mostPurchasedItemGroupedByProvince_output = mostPurchasedItemGroupedByProvince.coalesce(1)

mostPurchasedItemGroupedByProvince_output.write.mode("overwrite").option("header","true").option("encoding","UTF-8").option("sep",",").csv("file:///usr/FBDP/mostPurchasedItem")