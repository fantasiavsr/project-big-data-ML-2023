<br />
<div align="center">
<h3 align="center">Spark Big Data</h3>

  <p align="center">
    Project Big Data Machine Learning 2023
  </p>
</div>

## Data Filtering
link: https://github.com/fantasiavsr/project-big-data-ML-2023/blob/main/src/DataFiltering/README.md

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

1.	Mengimport library dan modul yang diperlukan: Kode dimulai dengan mengimpor beberapa library dan modul yang akan digunakan, termasuk matplotlib.pyplot, pyspark.sql.SparkSession, pandas, pyspark.sql.types.DoubleType, pyspark.ml.feature.StringIndexer, pyspark.ml.feature.VectorAssembler, dan pyspark.ml.regression.RandomForestRegressor.
2.	Membuat sesi Spark: Dalam langkah ini, sebuah sesi Spark dibuat menggunakan SparkSession.builder.master("local[*]").getOrCreate(). Sesuai dengan konfigurasi tersebut, sesi Spark akan berjalan secara lokal dengan menggunakan semua core yang tersedia.
3.	Membaca dataset: Dataset dibaca menggunakan spark.read.csv("dataset.csv", header=True, inferSchema=True). Dataset tersebut merupakan file CSV dengan baris header dan skema akan diinfer dari data.
4.	Konversi kolom "view" menjadi tipe data numerik: Kolom "view" pada DataFrame data dikonversi menjadi tipe data numerik menggunakan data.withColumn("view", data["view"].cast(DoubleType())).
5.	Membersihkan data dengan mengabaikan baris yang memiliki nilai-nilai yang tidak valid: Baris-baris dengan nilai-nilai yang tidak valid dalam kolom "category_id" dan "view" dihapus menggunakan data.dropna(subset=["category_id", "view"]).
6.	Preprocessing data: Pada langkah ini, dilakukan preprocessing pada data untuk persiapan pemodelan. Kolom "category_id" diindeks menjadi "category_index" menggunakan StringIndexer, dan kolom "category_index" diubah menjadi vektor fitur "features" menggunakan VectorAssembler.
7.	Split dataset menjadi training set dan test set: Dataset dibagi menjadi training set dan test set dengan perbandingan 80:20 menggunakan (training, test) = data.randomSplit([0.8, 0.2]).
8.	Membangun model Random Forest Regressor: Model RandomForestRegressor dibangun dengan label kolom "view" dan fitur kolom "features" menggunakan RandomForestRegressor(labelCol="view", featuresCol="features"). Model ini akan digunakan untuk memprediksi jumlah view.
9.	Memprediksi jumlah view menggunakan model: Model yang telah dibangun digunakan untuk memprediksi jumlah view pada test set dengan menggunakan model.transform(test).
10.	Mengatur font yang mendukung karakter CJK: Font yang mendukung karakter CJK diatur menggunakan plt.rcParams["font.family"] = "DejaVu Sans".
11.	Mengambil kolom publish_time dan category_id dari dataset: Kolom "publish_time" dan "category_id" diambil dari DataFrame predictions yang berisi hasil prediksi.
12.	Menghitung jumlah video berdasarkan waktu dan kategori: Jumlah video dihitung berdasarkan waktu dan kategori menggunakan groupBy pada kolom "publish_time" dan "category_id", kemudian dihitung jumlahnya dengan count().
13.	Mengubah hasil agregasi menjadi Pandas DataFrame: Hasil agregasi pada DataFrame video_count diubah menjadi Pandas DataFrame video_count_df agar dapat digunakan untuk visualisasi.
14.	Menggabungkan kolom "publish_time" dan "category_id" menjadi satu kolom "label": Kolom "publish_time" dan "category_id" digabungkan menjadi satu kolom baru "label" dengan format "YYYY-MM-DD - NAMA_KATEGORI" menggunakan pd.to_datetime dan replace.
15.	Mengurutkan berdasarkan count secara menurun: DataFrame video_count_df diurutkan berdasarkan kolom "count" secara menurun menggunakan sort_values.
16.	Mengambil 10 bar teratas: DataFrame video_count_df diambil 10 bar teratas menggunakan head(10), sehingga hanya 10 kategori dengan jumlah video terbanyak yang ditampilkan dalam visualisasi.
17.	Menampilkan grafik batang: DataFrame top_10_video_count_df digunakan untuk membuat grafik batang yang menunjukkan jumlah video untuk masing-masing kategori. Plot batang ini kemudian ditampilkan menggunakan plt.plot.


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

1.	Menginstall paket yang dibutuhkan: Pada langkah ini, beberapa paket yang diperlukan diinstal menggunakan !pip install. Paket-paket yang diinstal adalah pyspark dan wordcloud.
2.	Mengimport library dan modul yang diperlukan: Kode dimulai dengan mengimpor beberapa library dan modul yang akan digunakan, termasuk pyspark.sql.SparkSession, pyspark.sql.functions.col, pyspark.sql.functions.sum, wordcloud, dan matplotlib.pyplot.
3.	Membuat sesi Spark: Dalam langkah ini, sebuah sesi Spark dibuat menggunakan SparkSession.builder.master("local[*]").getOrCreate(). Sesuai dengan konfigurasi tersebut, sesi Spark akan berjalan secara lokal dengan menggunakan semua core yang tersedia.
4.	Membaca dataset: Dataset dibaca menggunakan spark.read.csv("dataset.csv", header=True, inferSchema=True). Dataset tersebut merupakan file CSV dengan baris header dan skema akan diinfer dari data.
5.	Memilih kolom yang dibutuhkan: Pada langkah ini, kolom-kolom yang dibutuhkan yaitu "publish_time", "title", dan "view" dipilih menggunakan df.select("publish_time", "title", "view"). DataFrame yang dihasilkan disimpan dalam variabel selected_df.
6.	Mengelompokkan berdasarkan judul dan menjumlahkan jumlah view: DataFrame selected_df dikelompokkan berdasarkan kolom "title" dan menggunakan fungsi agregasi spark_sum untuk menjumlahkan jumlah view. Hasilnya disimpan dalam DataFrame grouped_df.
7.	Mengkonversi DataFrame menjadi Pandas DataFrame: DataFrame grouped_df diubah menjadi Pandas DataFrame menggunakan toPandas(). Hal ini dilakukan agar data dapat digunakan untuk pembuatan wordcloud.
8.	Membuat wordcloud: Pandas DataFrame pandas_df diubah menjadi dictionary dengan kolom "title" sebagai kunci dan kolom "view_count" sebagai nilai. Wordcloud kemudian dibuat menggunakan WordCloud dengan mengatur ukuran, warna latar belakang, dan font yang digunakan.
9.	Menampilkan wordcloud: Wordcloud yang telah dibuat ditampilkan menggunakan plt.imshow. Ukuran gambar, pengaturan interpolasi, dan sumbu pada plot diatur. Akhirnya, wordcloud ditampilkan menggunakan plt.show().

