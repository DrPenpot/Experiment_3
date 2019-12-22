### 阶段一

---

#### 大致思路

设置两个Job，第一个Job统计数量，第二个Job排序。

统计条目数量和求关系交集的思路类似。

排序的思路是利用setSortComparatorClass，写一个倒排的Comparator

```java
    //针对IntWritable的倒排Comparator
    private  static class IntWritableDecreasingComparator extends IntWritable.Comparator{
        public int compare(WritableComparable a, WritableComparable b){
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1 ,int l1, byte[] b2, int s2, int l2){
            return -super.compare(b1,s1,l1,b2,s2,l2);
        }
    }

```

并且在统计数量时将key设置成数量，value设置成id

---

#### 曲折的道路

一开始的时候看漏了“各”，就直接统计了所有数据中购买和点击数量最多的。


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

---

#### 意外收获

对比两次结果，发现在全国范围内表现好的商品，在各个省的表现都位于前列

间接说明了各省人民对于商品的偏好相似

