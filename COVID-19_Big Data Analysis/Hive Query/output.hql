SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.map.java.opts=-Xmx4g;
SET mapreduce.reduce.java.opts=-Xmx4g;

USE covid_db;

CREATE EXTERNAL TABLE covid_db.covid_final_output 
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
 Rank_by_Death_of_Closed_Cases   	DOUBLE,
 TOP_DEATH 			                STRING,
 TOP_TEST 			                STRING
)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) 
STORED as TEXTFILE
LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';

INSERT OVERWRITE TABLE covid_final_output PARTITION(COUNTRY_NAME)
SELECT Country,Total_Cases,New_Cases,Total_Deaths,New_Deaths,Total_Recovered,
		Active_Cases,Serious,Tot_Cases,Deaths,Total_Tests,Tests,CASES_per_Test,Death_in_Closed_Cases,
		Rank_by_Testing_rate,Rank_by_Death_rate,Rank_by_Cases_rate,Rank_by_Death_of_Closed_Cases,
		DENSE_RANK() OVER (ORDER BY CAST(regexp_replace(regexp_replace(TRIM(Deaths),'\\.00',''),',','') as int) DESC ),
		DENSE_RANK() OVER (ORDER BY CAST(regexp_replace(regexp_replace(TRIM(Tests),'\\.00',''),',','') as int) DESC ),
		Country  
FROM covid_ds_partitioned;

INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/covid_project_output' ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) 
select * from covid_db.covid_final_output; 