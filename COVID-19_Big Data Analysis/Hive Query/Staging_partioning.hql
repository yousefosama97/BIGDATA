SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.map.java.opts=-Xmx4g;
SET mapreduce.reduce.java.opts=-Xmx4g;
CREATE database covid_db;

USE covid_db;

CREATE TABLE IF NOT EXISTS covid_db.covid_staging 
(
 Country 			                STRING,
 Total_Cases   		                DOUBLE,
 New_Cases    		                DOUBLE,
 Total_Deaths                       DOUBLE,
 New_Deaths                         DOUBLE,
 Total_Recovered                    DOUBLE,
 Active_Cases                       DOUBLE,
 Serious		                  	DOUBLE,
 Tot_Cases                   		DOUBLE,
 Deaths                      		DOUBLE,
 Total_Tests                   		DOUBLE,
 Tests			                 	DOUBLE,
 CASES_per_Test                     DOUBLE,
 Death_in_Closed_Cases     	        STRING,
 Rank_by_Testing_rate 		        DOUBLE,
 Rank_by_Death_rate    		        DOUBLE,
 Rank_by_Cases_rate    		        DOUBLE,
 Rank_by_Death_of_Closed_Cases   	DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) 
STORED as TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_ds_partitioned 
(
 Country 			                STRING,
 Total_Cases   		                STRING,
 New_Cases    		                STRING,
 Total_Deaths                       STRING,
 New_Deaths                         STRING,
 Total_Recovered                    STRING,
 Active_Cases                       STRING,
 Serious		                  	STRING,
 Tot_Cases                   		STRING,
 Deaths                      		STRING,
 Total_Tests                   		STRING,
 Tests			                 	STRING,
 CASES_per_Test                     STRING,
 Death_in_Closed_Cases     	        STRING,
 Rank_by_Testing_rate 		        STRING,
 Rank_by_Death_rate    		        STRING,
 Rank_by_Cases_rate    		        STRING,
 Rank_by_Death_of_Closed_Cases   	STRING
)
PARTITIONED BY (COUNTRY_NAME STRING)

LOCATION '/user/cloudera/ds/partitioned';

INSERT OVERWRITE TABLE covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT Country,Total_Cases,New_Cases,CASE WHEN TRIM(Deaths)="" THEN "0" ELSE Deaths END,
	   New_Deaths,Total_Recovered,Active_Cases,Serious,Tot_Cases,Deaths,CASE WHEN TRIM(Tests)="" THEN "0" ELSE Tests END ,
	   Tests,CASES_per_Test,Death_in_Closed_Cases,Rank_by_Testing_rate,Rank_by_Death_rate,Rank_by_Cases_rate, 
	   Rank_by_Death_of_Closed_Cases ,Country 
FROM covid_staging WHERE Country != "World";