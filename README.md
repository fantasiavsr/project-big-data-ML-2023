<br />
<div align="center">
<h3 align="center">Spark Big Data</h3>

  <p align="center">
    Project Big Data Machine Learning 2023
  </p>
</div>

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
![image](https://github.com/fantasiavsr/project-big-data-ML-2023/blob/main/docs/img/category.png)

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
![image](https://github.com/fantasiavsr/project-big-data-ML-2023/blob/main/docs/img/wordloud.png)
