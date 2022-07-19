#! /bin/bash

# Region with highest order amount in each category in year 2010

spark-shell <<EOF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, max, split, sum}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val hive = new org.apache.spark.sql.hive.HiveContext(sc);
hive.sql("use proj");
val sales = hive.sql("select * from salesdata");
val names = hive.sql("select * from idandnames");
val JoinedTable = sales.join(names,"region_id");
val yearColumn = JoinedTable.withColumn("order_year",split(JoinedTable("order_date"),"-")(2).cast("Int"));
val filtered2010 = yearColumn.filter(yearColumn("order_year")===2010);
val result = filtered2010.withColumn("Highest Order",rank().over(Window.partitionBy("product_category").orderBy(desc("amount"))));
result.filter(result("Highest Order")===1).select("region_name","product_category","amount").show(result.collect().length,false);
result.filter(result("Highest Order")===1).select("region_name","product_category","amount").write.mode("overwrite").format("csv").save("/home/hduser/question4_output.txt");
:quit
