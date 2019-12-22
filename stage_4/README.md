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

val model_lr = lr.fit(trainingData)

val lr_output = model_lr.transform(test)

val lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1).setFeaturesCol("features").setLabelCol("label").setPredictionCol("prediction")

val model_lsvc = lsvc.fit(trainingData)


```

发现所有数据的prediction都是0.0，不知道哪里出了问题……
