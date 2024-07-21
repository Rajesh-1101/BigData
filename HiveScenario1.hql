/*
  1. Scenario: Large-Scale Data Processing
Question: Your company has collected large volumes of sensor data stored in HDFS, and you need to process and analyze this data using Hive. 
The data includes sensor readings (timestamp, sensor_id, value) from various IoT devices. Design a solution to calculate the average value of each sensor per day 
for the last month.
  */

CREATE EXTERNAL TABLE sensor_data (
    timestamp STRING,
    sensor_id STRING,
    value DOUBLE,
    date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hdfs/sensor_data';

INSERT OVERWRITE TABLE sensor_data PARTITION (date)
SELECT timestamp, sensor_id, value, FROM_UNIXTIME(UNIX_TIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS date
FROM sensor_raw_data;

SELECT date, sensor_id, AVG(value) AS avg_value
FROM sensor_data
WHERE date >= FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE, 30)), 'yyyy-MM-dd')
GROUP BY date, sensor_id
ORDER BY date, sensor_id;
