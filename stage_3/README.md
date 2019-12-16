```sh
val million_user_log = spark.read.format("csv").option("sep","\t").option("header","false").option("inferSchema","false").load("usr/FBDP/million_user_log.csv")
```