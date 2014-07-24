ADD JAR C:\Users\lester\opengeorgia\hive\csv-serde-1.0.jar;

CREATE DATABASE IF NOT EXISTS opengeorgia;
DROP TABLE IF EXISTS salary;
USE opengeorgia;

CREATE EXTERNAL TABLE salary (
   name string,
   title string,
   salary float,
   travel float,
   orgType string,
   org string,
   year int )
 ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'
 STORED AS TEXTFILE
 LOCATION '/user/lester/opengeorgia/';
