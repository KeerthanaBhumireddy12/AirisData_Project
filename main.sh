#! /bin/bash

. /home/hduser/projautomate/properties.config

hdfs dfs -rm -r ${output_path}

sqoop-import-all-tables --connect "jdbc:mysql://localhost/project" --username ${username} --password ${password} --warehouse-dir ${output_path}

if[ $? -eq 0 ]
then 
  echo "sqoop import completed"
else
  echo "failed"
fi

