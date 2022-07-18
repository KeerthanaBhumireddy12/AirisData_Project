#! /bin/bash
. /home/hduser/projautomate/properties.config
. /home/hduser/projautomate/hiveproperties.config

hive <<EOF

use proj;
insert overwrite local directory './1stq.txt' select region_id,sum(amount) from ${sales_base_data} group by region_id;
insert overwrite local directory './2ndq.txt' select c.product_category,sum(c.amount) as totalsales,i.region_name from ${partition_table} c join ${idnames_base_data} i on (c.region_id = i.region_id) where c.product_category = 'Cosmetics' group by c.product_category,i.region_name order by totalsales desc;

