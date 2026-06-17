CREATE DATABASE IF NOT EXISTS weather_db;
USE weather_db;

CREATE EXTERNAL TABLE weather_raw (
    city STRING,
    timestamp STRING,
    temp DOUBLE,
    feels_like DOUBLE,
    humidity INT,
    pressure DOUBLE,
    weather STRING,
    `desc` STRING,
    wind DOUBLE,
    clouds INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/weather/raw/';