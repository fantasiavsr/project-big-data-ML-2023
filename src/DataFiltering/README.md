<br />
<div align="center">
<h3 align="center">Spark Big Data</h3>

  <p align="center">
    Project Big Data 2023
  </p>
</div>

## Memulai Spark
![image](https://user-images.githubusercontent.com/86558365/228115462-3cb62086-c4e5-4cb0-ba3b-e434b9aef702.png)
menggunakan Spark versi 3.3.2 yang dijalankan di dalam container dalam docker

## Data Extraction
### Code 1 - Import
```sh
from pyspark.sql import *
import pandas as pd
import csv
```
### Code 2 - Create SparkSession
```sh
spark = SparkSession\
            .builder\
            .appName("testProject-app")\
            .getOrCreate()
```
![image](https://user-images.githubusercontent.com/86558365/232996254-9a01fe57-18a1-4020-a574-d3bb1c9ef16a.png)

### Code 3 - Read CSV
```sh
df = spark.read.csv("./data/trending.csv", header=True, inferSchema=True, multiLine=True, escape='"')
df.show()
```
![image](https://user-images.githubusercontent.com/86558365/232996400-70d5d8a1-5485-44f7-a84f-c4bbe92e02e1.png)

## Data Filtering
### Code 1 - Check Column
![image](https://user-images.githubusercontent.com/86558365/236971004-7fdd291f-be2c-48fc-8c27-c747dffacb8c.png)

### Code 2 - Counts Row & Column
![image](https://user-images.githubusercontent.com/86558365/236971101-5f20a94c-adb2-4e19-b95e-2fb5e28801d1.png)

### Code 3 - Check df2
![image](https://user-images.githubusercontent.com/86558365/236971534-42a42a47-4562-4ab4-886a-57104ec4ded5.png)

### Code 4 - Counts Row & Column df2 After Drop
![image](https://user-images.githubusercontent.com/86558365/236971637-fa78147d-356b-4a00-842d-36961506b86f.png)

### Code 5 - Check Null Values for Each Column
![image](https://user-images.githubusercontent.com/86558365/236973263-e9ed6d67-9e1d-4d1b-bd26-ec6783abfd06.png)

### Code 6 - Drop Column with Null Values with Subset & Check df3
![image](https://user-images.githubusercontent.com/86558365/236973296-6a4ef9bd-29e9-43d8-bb6d-17ef383c4410.png)

### Code 7 - Counts Row & Column After Filtering Null Values
![image](https://user-images.githubusercontent.com/86558365/236973454-412c977c-b65d-427b-865c-ceea6baca332.png)
