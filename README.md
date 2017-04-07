# MapReduceExample
一组数据按照年龄分区，区内按照成绩倒序排序\
#数据内容见data/person.csv,如下\
#编号，姓名，年龄，性别，成绩\
1,Alice,23,female,45\
2,Bob,34,male,89\
3,Chris,67,male,97\
4,Kristine,38,female,53\
5,Connor,25,male,27\
6,Daniel,78,male,95\
7,James,34,male,79\
8,Alex,52,male,69\
9,Nancy,7,female,98\
10,Adam,9,male,37\
11,Jacob,7,male,23\
12,Mary,6,female,93\
13,Clara,87,female,72\
14,Monica,56,female,92\
#项目要求#\
1.将数据按照年龄段分区\
 0至20岁为第一区，\
 20至50岁为第二区，\
 50以上为第三区。\
2.将各区的数据按照分数倒序排序输出\
#输出结果如下#\
分区1：part-r-00000\
9,Nancy,female,7        98\
12,Mary,female,6        93\
10,Adam,male,9  37\
11,Jacob,male,7 23\
分区2：part-r-00001\
2,Bob,male,34   89\
7,James,male,34 79\
4,Kristine,female,38    53\
1,Alice,female,23       45\
5,Connor,male,25        27\
分区3：part-r-00002\
3,Chris,male,67 97\
6,Daniel,male,78        95\
14,Monica,female,56     92\
13,Clara,female,87      72\
8,Alex,male,52  69
