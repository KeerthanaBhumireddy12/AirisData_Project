#! /bin/bash

# Hive table Creation

source /home/hduser/project2/configuration.config

hive <<EOF
use ${hive_database_name};
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
create external table if not exists ${hive_base_table}(date_entry string,country string,confirmed int,deaths int,recovered int) row format delimited fields terminated by ',' lines terminated by '\n';
load data inpath "/user/hduser/ProjectFinal/Carona.csv" into table ${hive_base_table};
create external table if not exists ${hive_partition_table}(date_entry string,confirmed int,deaths int,recovered int) partitioned by (country string) row format delimited fields terminated by ',' lines terminated by '\n';
insert overwrite table ${hive_partition_table} partition(country) select date_entry,confirmed,deaths,recovered,country from ${hive_base_table};



