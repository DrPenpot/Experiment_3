```sh
val million_user_log = spark.read.format("csv").option("sep",",").option("header","false").option("inferSchema","false").load("input/million_user_log.csv")
```

---

尝试用bin/spark-sql连接Hive进行实验

将Hadoop下的`core-site.xml`,`hdfs-site.xml`以及Hive下的`hive-site.xml`复制进spark文件夹下的`conf/`，再运行`bin/spark-sql`

报错`The specified datastore driver ("com.mysql.jdbc.Driver") was not found in the CLASSPATH. Please check your CLASSPATH specification, and the name of the driver.`

问题在于启动的时候没有配置mysql的驱动地址，尝试用如下命令启动：`bin/spark-sql --jars /usr/local/hive/lib/mysql-connector-java-5.1.40-bin.jar`仍然失败

果断放弃

---

在`bin/spark-shell`下实验

```scala

import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

val million_user_log = spark.read.format("csv").option("sep",",").option("header","false").option("inferSchema","false").load("input/million_user_log.csv")

```

这样即可导入数据，问题在于这时列名为`_c0`,`_c1`等等，虽然可以继续操作，但是多有不便。秉持完成作业为主，花里胡哨为辅的原则，我决定先这么进行下去，完成任务后再考虑如何改进。


```scala
case class Log(user_id: Int, item_id: Int, cat_id: Int, merchant_id: Int, brand_id: Int, month: Int, day: Int, action: Int, age_range: Int, gender: Int, province: String)

val mlog = million_user_log.as[Log]

```