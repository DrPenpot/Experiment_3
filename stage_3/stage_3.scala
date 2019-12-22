import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

val mlog = spark.read.format("csv").option("sep",",").option("header","false").option("encoding","UTF-8").load("input/million_user_log.csv").toDF("user_id", "item_id", "cat_id", "merchant_id", "brand_id", "month", "day", "action", "age_range", "gender", "province")

mlog.createOrReplaceTempView("mlog")

val mostViewedBrand = spark.sql("SELECT brand_id,COUNT(*) AS click_num FROM mlog WHERE action = 0 GROUP BY brand_id").sort(desc("click_num"))

val mostViewedBrand_output = mostViewedBrand.coalesce(1)

mostViewedBrand_output.write.mode("overwrite").option("header","true").option("encoding","UTF-8").option("sep",",").csv("file:///usr/FBDP/mostViewedBrand")


//统计购买最多的商品类别并输出

val mostPurchasedCat = spark.sql("SELECT province, cat_id, COUNT(*) AS purchase_num FROM mlog WHERE action =2 GROUP BY province, cat_id HAVING purchase_num > 57 ORDER BY province, purchase_num DESC")

val mostPurchasedCat_output = mostPurchasedCat.coalesce(1) //避免输出很多小文件，这样把可以把整个表作为一个文件输出

mostPurchasedCat_output.write.mode("overwrite").option("header","true").option("encoding","UTF-8").option("sep",",").csv("file:///usr/FBDP/mostPurchasedCat")

//统计购买最多的商品并输出

val mostPurchasedItemGroupedByProvince = spark.sql("SELECT province, item_id, COUNT(*) as purchase_num FROM mlog WHERE action = 2 GROUP BY province, item_id HAVING purchase_num > 3 ORDER BY province, purchase_num DESC")

val mostPurchasedItemGroupedByProvince_output = mostPurchasedItemGroupedByProvince.coalesce(1)

mostPurchasedItemGroupedByProvince_output.write.mode("overwrite").option("header","true").option("encoding","UTF-8").option("sep",",").csv("file:///usr/FBDP/mostPurchasedItem")