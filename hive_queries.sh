#! /bin/bash

source /home/hduser/project2/configuration.config

# Filtering out US Data from Hive Table 

hive <<EOF
use practice;
insert overwrite directory "/user/hduser/ProjectFinal/us_data" select * from ${hive_partition_table} where country="US";
!hdfs dfs -get /user/hduser/ProjectFinal/us_data/000000_0 /home/hduser/project2/us_data.txt;
exit;


