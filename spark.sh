#! /bin/bash

# Region Id, Name and Total Sales in each year 

spark-shell <<EOF
val hive = new org.apache.spark.sql.hive.HiveContext(sc);
hive.sql("use proj");
val sales = hive.sql("select * from salesdata");
val names = hive.sql("select * from idandnames");
val joinedTable = sales.join(names,"region_id").select("amount","order_date","region_name","region_id");
val yearColumn = joinedTable.withColumn("order_year",split(joinedTable("order_date"),"-")(2).cast("Int"));
yearColumn.createOrReplaceTempView("Sales_Table")
val result = spark.sql("select sum(amount) as total_sales,region_id,region_name,order_year from Sales_Table group by order_year,region_id, region_name,order_year sort by order_year asc");
result.show(result.collect().length,false);
result.write.mode("overwrite").save("/home/hduser/question3_output.txt");
:quit
