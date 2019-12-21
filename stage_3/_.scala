import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

val mlog = spark.read.format("csv").option("sep",",").option("header","false").option("encoding","UTF-8").load("input/million_user_log.csv").toDF("user_id", "item_id", "cat_id", "merchant_id", "brand_id", "month", "day", "action", "age_range", "gender", "province")

mlog.createOrReplaceTempView("mlog")
val sqlLog = spark.sql("SELECT brand_id,COUNT(*) AS click_num FROM mlog WHERE action = 0 GROUP BY brand_id").sort(desc("click_num"))

mlog.sql("SELECT province, COUNT(*) as ")