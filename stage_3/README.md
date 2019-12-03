创建表
```
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

导入数据`LOAD DATA LOCAL INPATH '/usr/FBDP/million_user_log.csv' OVERWRITE INTO TABLE million_user_log;`

查看导入的结果`SELECT m.* FROM million_user_log m LIMIT 5;`

显示```
OK
328862	406349	1280	2700	5476	11	11	0	0	1	四川
328862	406349	1280	2700	5476	11	11	0	7	1	重庆市
328862	807126	1181	1963	6109	11	11	0	1	0	上海市
328862	406349	1280	2700	5476	11	11	2	6	0	台湾
328862	406349	1280	2700	5476	11	11	0	6	2	甘肃
Time taken: 1.494 seconds, Fetched: 5 row(s)

说明导入数据成功了
