#! /bin/bash

. /home/hduser/projautomate/properties.config
. /home/hduser/projautomate/hiveproperties.config
hive <<EOF
use proj;
create external table if not exists ${sales_base_data} (region_id int,product_category string,order_date string,order_id string,amount int) row format delimited fields terminated by ',' location '${output_path}/${sales_base_data}/';
create external table if not exists ${idnames_base_data} (region_id int,region_name string) row format delimited fields terminated by ',' location '${output_path}/${idnames_base_data}/';
create external table if not exists ${partition_table} (region_id int,amount int) partitioned by(product_category string);
insert into ${partition_table} partition (product_category) select region_id,amount,product_category from ${sales_base_data};
