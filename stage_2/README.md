创建表
```sql
CREATE TABLE 
million_user_log
(
user_id INT,
item_id INT,
cat_id INT,
merchant_id INT,
brand_id INT,
month INT,
day INT,
action INT,
age_range INT,
gender INT,
province STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','//提供的csv文件以','作为分隔符，如果不设定会导入1,000,000*11条NULL
;
```



导入数据

```sql
LOAD DATA LOCAL INPATH '/usr/FBDP/million_user_log.csv' OVERWRITE INTO TABLE million_user_log;
```


查看导入的结果

```sql
SELECT m.* FROM million_user_log m LIMIT 5;
```

显示
```
OK
328862	406349	1280	2700	5476	11	11	0	0	1	四川
328862	406349	1280	2700	5476	11	11	0	7	1	重庆市
328862	807126	1181	1963	6109	11	11	0	1	0	上海市
328862	406349	1280	2700	5476	11	11	2	6	0	台湾
328862	406349	1280	2700	5476	11	11	0	6	2	甘肃
Time taken: 1.494 seconds, Fetched: 5 row(s)
```

说明导入数据成功了

---

###验证阶段一的结果：

```sql
SELECT item_id, 
COUNT(*) AS purchase_num 
FROM million_user_log 
WHERE action = 2
GROUP BY item_id
ORDER BY purchase_num DESC
LIMIT 20
;
```
```shell
hive> SELECT item_id, 
    > COUNT(*) AS purchase_num 
    > FROM million_user_log 
    > WHERE action = 2
    > GROUP BY item_id
    > ORDER BY purchase_num DESC
    > LIMIT 20;
Query ID = root_20191209093221_f42124d0-37a6-48eb-8b66-bd55eecba29a
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0015, Tracking URL = http://h01:8088/proxy/application_1575879510873_0015/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0015
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2019-12-09 09:32:30,587 Stage-1 map = 0%,  reduce = 0%
2019-12-09 09:32:38,960 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.65 sec
2019-12-09 09:32:47,198 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 10.15 sec
MapReduce Total cumulative CPU time: 10 seconds 150 msec
Ended Job = job_1575879510873_0015
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0016, Tracking URL = http://h01:8088/proxy/application_1575879510873_0016/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0016
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2019-12-09 09:33:01,820 Stage-2 map = 0%,  reduce = 0%
2019-12-09 09:33:07,990 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 3.02 sec
2019-12-09 09:33:14,135 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 4.83 sec
MapReduce Total cumulative CPU time: 4 seconds 830 msec
Ended Job = job_1575879510873_0016
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 10.15 sec   HDFS Read: 14821 HDFS Write: 1068207 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 4.83 sec   HDFS Read: 1075742 HDFS Write: 543 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 980 msec
OK
191499	248
353560	193
1059899	186
713695	161
514725	137
783997	132
936203	127
107407	126
823766	117
349999	117
655904	116
221663	114
181387	113
698879	107
1039919	103
81360	100
944554	99
770668	96
67897	95
315345	94
Time taken: 53.871 seconds, Fetched: 20 row(s)
```

```sql
SELECT item_id, 
COUNT(*) AS attention_num 
FROM million_user_log 
GROUP BY item_id
ORDER BY attention_num DESC
LIMIT 20
;
```

```shell
hive> SELECT item_id, 
    > COUNT(*) AS attention_num 
    > FROM million_user_log 
    > GROUP BY item_id
    > ORDER BY attention_num DESC
    > LIMIT 20;
Query ID = root_20191209093738_22bbc772-cad5-4595-81ca-34d53cd476f2
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0017, Tracking URL = http://h01:8088/proxy/application_1575879510873_0017/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0017
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2019-12-09 09:37:48,638 Stage-1 map = 0%,  reduce = 0%
2019-12-09 09:37:56,866 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.22 sec
2019-12-09 09:38:04,066 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 8.94 sec
MapReduce Total cumulative CPU time: 8 seconds 940 msec
Ended Job = job_1575879510873_0017
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0018, Tracking URL = http://h01:8088/proxy/application_1575879510873_0018/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0018
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2019-12-09 09:38:18,536 Stage-2 map = 0%,  reduce = 0%
2019-12-09 09:38:23,759 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 3.26 sec
2019-12-09 09:38:29,923 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 3.26 sec
MapReduce Total cumulative CPU time: 5 seconds 70 msec
Ended Job = job_1575879510873_0018
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 8.94 sec   HDFS Read: 13564 HDFS Write: 3513613 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 5.07 sec   HDFS Read: 3521148 HDFS Write: 562 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 10 msec
OK
67897	23813
783997	19028
636863	8481
1024557	2261
628774	1839
770668	1767
458784	1499
846404	1260
217788	1172
191499	1111
696384	1109
768758	1072
94609	1063
353560	945
436289	869
424003	849
1059899	775
215563	750
416858	730
514725	728
Time taken: 53.473 seconds, Fetched: 20 row(s)
```


和阶段一MapReduce的结果一致

---

###查询双11那天有多少人购买了商品

查询购买人数：
```sql
SELECT item_id, COUNT(*) AS attention_num FROM million_user_log  GROUP BY item_id
```
```sql
SELECT COUNT(DISTINCT user_id)  FROM million_user_log WHERE action=2;
```

结果为*37202*

```shell
hive> SELECT COUNT(DISTINCT user_id)  FROM million_user_log WHERE action=2;
Query ID = root_20191209085900_61f6754d-b73b-4034-8cac-b25bf8a3b181
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0007, Tracking URL = http://h01:8088/proxy/application_1575879510873_0007/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0007
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2019-12-09 08:59:10,382 Stage-1 map = 0%,  reduce = 0%
2019-12-09 08:59:18,601 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.36 sec
2019-12-09 08:59:25,806 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 9.47 sec
MapReduce Total cumulative CPU time: 9 seconds 470 msec
Ended Job = job_1575879510873_0007
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 9.47 sec   HDFS Read: 10463 HDFS Write: 105 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 470 msec
OK
37202
Time taken: 27.593 seconds, Fetched: 1 row(s)
```


有趣的是，对总人数的查询：

```sql
SELECT COUNT(DISTINCT user_id)  FROM million_user_log;
```

结果仍然是37202，这说明记录中的每个人都至少购买了一件商品

```shell
    > SELECT COUNT(DISTINCT user_id)  FROM million_user_log;
Query ID = root_20191209085709_930a6174-b6b3-4c4f-a58e-9a1ea9192813
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0006, Tracking URL = http://h01:8088/proxy/application_1575879510873_0006/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0006
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2019-12-09 08:57:19,316 Stage-1 map = 0%,  reduce = 0%
2019-12-09 08:57:27,521 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.89 sec
2019-12-09 08:57:34,722 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 6.44 sec
MapReduce Total cumulative CPU time: 6 seconds 440 msec
Ended Job = job_1575879510873_0006
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 6.44 sec   HDFS Read: 9646 HDFS Write: 105 SUCCESS
Total MapReduce CPU Time Spent: 6 seconds 440 msec
OK
37202
Time taken: 25.919 seconds, Fetched: 1 row(s)
```

顺便一提，action为0，1，2，3的各个条目的数量分别是：867472+1102+116856+14570=1000000

---

###查询双11那天男女买家购买商品的比例

```sql
SELECT gender, COUNT(*)
FROM million_user_log
WHERE action = 2
GROUP BY gender
;
```
结果：
|Gender		|Count|
|:---:|:---:|
|Female	|39058|
|Male	|38932|
|Unknown	|38866|

```shell
hive> SELECT gender, COUNT(*)
    > FROM million_user_log
    > WHERE action = 2
    > GROUP BY gender;
Query ID = root_20191209094446_2119bcf9-d397-408b-b853-4fad13b471f6
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0019, Tracking URL = http://h01:8088/proxy/application_1575879510873_0019/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0019
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2019-12-09 09:44:55,283 Stage-1 map = 0%,  reduce = 0%
2019-12-09 09:45:03,573 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 4.82 sec
2019-12-09 09:45:10,775 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 7.44 sec
MapReduce Total cumulative CPU time: 7 seconds 440 msec
Ended Job = job_1575879510873_0019
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 7.44 sec   HDFS Read: 15686 HDFS Write: 147 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 440 msec
OK
0	39058
1	38932
2	38866
Time taken: 26.692 seconds, Fetched: 3 row(s)
```
---

###查询双11那天浏览次数前十的品牌
这个题目的表述有点不太清晰，我决定将“浏览”理解为“点击”

```sql
SELECT brand_id, 
COUNT(*) as click_num
FROM million_user_log
WHERE action = 0
GROUP BY brand_id
ORDER BY click_num DESC
LIMIT 10
;
```

结果如下
|brand_id|Count|
|:-:|:-:|
|1360	|49151|
|3738	|10130|
|82	|9719|
|1446	|9426|
|6215	|8568|
|1214	|8470|
|5376	|8282|
|2276	|7990|
|1662	|7808|
|8235	|7661|

```shell
hive> SELECT brand_id, 
    > COUNT(*) as click_num
    > FROM million_user_log
    > WHERE action = 0
    > GROUP BY brand_id
    > ORDER BY click_num DESC
    > LIMIT 10
    > ;
Query ID = root_20191209095621_2876c837-2670-4c65-babc-68f2eac87951
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0022, Tracking URL = http://h01:8088/proxy/application_1575879510873_0022/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0022
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2019-12-09 09:56:31,262 Stage-1 map = 0%,  reduce = 0%
2019-12-09 09:56:39,724 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.92 sec
2019-12-09 09:56:47,967 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 9.95 sec
MapReduce Total cumulative CPU time: 9 seconds 950 msec
Ended Job = job_1575879510873_0022
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1575879510873_0023, Tracking URL = http://h01:8088/proxy/application_1575879510873_0023/
Kill Command = /usr/local/hadoop/bin/mapred job  -kill job_1575879510873_0023
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
2019-12-09 09:57:03,609 Stage-2 map = 0%,  reduce = 0%
2019-12-09 09:57:09,796 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 3.14 sec
2019-12-09 09:57:15,958 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 5.05 sec
MapReduce Total cumulative CPU time: 5 seconds 50 msec
Ended Job = job_1575879510873_0023
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 9.95 sec   HDFS Read: 14822 HDFS Write: 118302 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 5.05 sec   HDFS Read: 125837 HDFS Write: 307 SUCCESS
Total MapReduce CPU Time Spent: 15 seconds 0 msec
OK
1360	49151
3738	10130
82	9719
1446	9426
6215	8568
1214	8470
5376	8282
2276	7990
1662	7808
8235	7661
Time taken: 56.86 seconds, Fetched: 10 row(s)
```