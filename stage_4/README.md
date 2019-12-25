### 阶段四

```scala
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import spark.implicits._

val data = spark.read.format("csv").option("sep",",").load("file:///usr/FBDP/train_after.csv").toDF("user_id","age_range","gender","merchant_id","label").withColumn("label", $"label".cast(DataTypes.IntegerType))

//withColumn方法转换行的数据类型为numeric，否则分类时会报错

val test_raw = spark.read.format("csv").option("sep",",").load("file:///usr/FBDP/test_after.csv").toDF("user_id","age_range","gender","merchant_id","label").withColumn("label", $"label".cast(DataTypes.IntegerType))

val hasher = new FeatureHasher().setInputCols("gender", "age_range").setOutputCol("features")

val featurized = hasher.transform(data)

val test = hasher.transform(test_raw)

val Array(trainingData, testData) = featurized.randomSplit(Array(0.7, 0.3), seed = 20)

val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0).setFeaturesCol("features").setLabelCol("label").setPredictionCol("prediction")

val model_lr = lr.fit(trainingData) //训练模型

val lr_output = model_lr.transform(test) //预测

val lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1).setFeaturesCol("features").setLabelCol("label").setPredictionCol("prediction")

val model_lsvc = lsvc.fit(trainingData)

val lsvc_output = model_lsvc.transform(test)

//输出结果，结果就是所有人都不是回头客

lsvc_output.coalesce(1).write.mode("overwrite").option("header","true").option("encoding","UTF-8").option("sep",",").json("file:///usr/FBDP/lsvc_output") //输出时不可以用csv格式，因为表格中含有不支持的类型，<type:tinyint,size:int,indices:array<int>,values:array<double>>

lr_output.coalesce(1).write.mode("overwrite").option("header","true").option("encoding","UTF-8").option("sep",",").json("file:///usr/FBDP/lr_output")




```
---

####结果：

发现所有数据的prediction都是0.0，这可能是由于训练集中的数据每个分类中0的数量远大于1的数量，导致训练出的分类器分出的所有类别的结果都是0