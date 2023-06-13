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

## Data Visualization
### Spark-Project Top 10 Category:
link colab: https://colab.research.google.com/drive/1Fd1PlTjdrPpqj8CDO78mMjP4E-sBmUlZ?usp=sharing
```sh
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
```
```sh
# Membuat sesi Spark
spark = SparkSession.builder.master("local[*]").getOrCreate()
```
```sh
# Baca dataset
data = spark.read.csv("dataset.csv", header=True, inferSchema=True)

# Konversi kolom "view" menjadi tipe data numerik
data = data.withColumn("view", data["view"].cast(DoubleType()))

# Membersihkan data dengan mengabaikan baris yang memiliki nilai-nilai yang tidak valid
data = data.dropna(subset=["category_id", "view"])

# Preprocessing data
indexer = StringIndexer(inputCol="category_id", outputCol="category_index")
data = indexer.fit(data).transform(data)

assembler = VectorAssembler(inputCols=["category_index"], outputCol="features")
data = assembler.transform(data)

# Split dataset menjadi training set dan test set
(training, test) = data.randomSplit([0.8, 0.2])

# Membangun model Random Forest Regressor
rf = RandomForestRegressor(labelCol="view", featuresCol="features")
model = rf.fit(training)

# Memprediksi jumlah view menggunakan model
predictions = model.transform(test)
```
```sh
# Mengatur font yang mendukung karakter CJK
plt.rcParams["font.family"] = "DejaVu Sans"

# Mengambil kolom publish_time dan category_id dari dataset
time_category = predictions.select("publish_time", "category_id")

# Menghitung jumlah video berdasarkan waktu dan kategori
video_count = time_category.groupBy("publish_time", "category_id").count()

# Mengubah hasil agregasi menjadi Pandas DataFrame
video_count_df = video_count.toPandas()

# Menggabungkan kolom "publish_time" dan "category_id" menjadi satu kolom "label"
video_count_df["label"] = pd.to_datetime(video_count_df["publish_time"]).dt.strftime("%Y-%m-%d") + " - " + video_count_df["category_id"].replace({
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "18": "Short Movies",
    "19": "Travel & Events",
    "20": "Gaming",
    "21": "Videoblogging",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "30": "Movies",
    "31": "Anime/Animation",
    "32": "Action/Adventure",
    "33": "Classics",
    "34": "Comedy",
    "35": "Documentary",
    "36": "Drama",
    "37": "Family",
    "38": "Foreign",
    "39": "Horror",
    "40": "Sci-Fi/Fantasy",
    "41": "Thriller",
    "42": "Shorts",
    "43": "Shows",
    "44": "Trailers"
})

# Mengurutkan berdasarkan count secara menurun
video_count_df = video_count_df.sort_values(by="count", ascending=False)

# Mengambil 10 bar teratas
top_10_video_count_df = video_count_df.head(10)

# Menampilkan grafik batang
top_10_video_count_df.plot(kind="bar", x="label", y="count")
plt.xticks(rotation=45)
plt.ylim(0, video_count_df["count"].max())
plt.show()
```
#### hasil:
![image](https://github.com/fantasiavsr/project-big-data-2023/assets/86558365/e7cb61fc-b685-4807-ad5e-223e91085d47)

### Spark-Project Top Title Wordcloud:
link colab: https://colab.research.google.com/drive/1H-tb8A10pdtO9vQzAjEBDLDGRnElk6qQ?usp=sharing
```sh
# Install required packages
!pip install pyspark
!pip install wordcloud
```
```sh
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum
from wordcloud import WordCloud
import matplotlib.pyplot as plt
```
```sh
# Create a SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Read the dataset into a DataFrame
df = spark.read.csv("dataset.csv", header=True, inferSchema=True)

# Select the columns of interest (publish_time, title, and view)
selected_df = df.select("publish_time", "title", "view")

# Group by title and sum the views
grouped_df = selected_df.groupBy("title").agg(spark_sum("view").alias("view_count"))

# Convert DataFrame to Pandas DataFrame for wordcloud generation
pandas_df = grouped_df.toPandas()

font_path = "NanumGothic.ttf"  # Path to Nanum Gothic font
wordcloud = WordCloud(width=800, height=400, background_color="white", font_path=font_path).generate_from_frequencies(pandas_df.set_index("title").to_dict()["view_count"])
```
```sh
# Plot the wordcloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()
```
#### hasil:
![image](https://github.com/fantasiavsr/project-big-data-2023/assets/86558365/342ba5c6-f5be-450d-8625-0266f36aef9d)
