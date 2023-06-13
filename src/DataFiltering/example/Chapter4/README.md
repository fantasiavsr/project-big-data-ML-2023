<br />
<div align="center">
<h3 align="center">Spark Big Data</h3>

  <p align="center">
    Pengumpulan tugas Spark Big Data minggu 8
  </p>
</div>

## Memulai Spark
![image](https://user-images.githubusercontent.com/86558365/228115462-3cb62086-c4e5-4cb0-ba3b-e434b9aef702.png)
di sini saya menggunakan Spark versi 3.3.2 yang dijalankan di dalam container dalam docker

## Spark-shell
```sh
pyspark
```

## Analitik dengan DataFrames
### Code 1
```sh
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreatingDataFrames").getOrCreate()
sc = spark.sparkContext

mylist = [(50, "DataFrame"),(60, "pandas")]
myschema = ['col1', 'col2']
```
### Code 2
```sh
df1 = spark.createDataFrame(mylist, myschema)
```
### Code 3
```sh
df1 = spark.createDataFrame(mylist, myschema)
```
### Code 4
```sh
from pyspark.sql import SQLContext, Row
peopleRDD = sc.textFile("data/Chapter4/examples/src/main/resources/people.txt")
```
### Code 5
```sh
from pyspark.sql import SQLContext, Row
peopleRDD = sc.textFile("data/Chapter4/examples/src/main/resources/people.txt")
people_sp = peopleRDD.map(lambda l: l.split(","))
people = people_sp.map(lambda p: Row(name=p[0], age=int(p[1])))
df_people = spark.createDataFrame(people)
df_people.createOrReplaceTempView("people")
spark.sql("SHOW TABLES").show()
spark.sql("SELECT name,age FROM people where age > 19").show()
```
### Code 6
```sh
from pyspark.sql import SQLContext, Row
peopleRDD = sc.textFile("data/Chapter4/examples/src/main/resources/people.txt")
people_sp = peopleRDD.map(lambda l: l.split(","))
people = people_sp.map(lambda p: Row(name=p[0], age=int(p[1])))
df_people = people_sp.map(lambda p: (p[0], p[1].strip()))
schemaStr = "name age"
fields = [StructField(field_name, StringType(), True) \
for field_name in schemaStr.split()]
schema = StructType(fields)
df_people = spark.createDataFrame(people,schema)
df_people.show()
df_people.createOrReplaceTempView("people")
spark.sql("select * from people").show()
```
Screenshot: 

Penjelasan code
```sh
mylist: Merupakan variabel yang digunakan untuk menyimpan daftar data dalam bentuk list dalam bahasa pemrograman Python.

myschema: Merupakan variabel yang digunakan untuk menyimpan skema atau struktur data yang didefinisikan untuk dataframe dalam Apache Spark.

spark.createDataFrame: Merupakan perintah dalam PySpark yang digunakan untuk membuat dataframe dari sumber data tertentu seperti file CSV, JSON, atau sumber data lainnya.

parallelize: Merupakan perintah dalam PySpark yang digunakan untuk membuat RDD (Resilient Distributed Dataset) dari koleksi data dalam bahasa pemrograman Python. RDD adalah salah satu konsep dasar dalam Apache Spark untuk pemrosesan data terdistribusi.

toDF: Merupakan perintah dalam PySpark yang digunakan untuk mengubah RDD menjadi dataframe.

hadoop: Merupakan perintah yang berkaitan dengan ekosistem Hadoop, yaitu kerangka kerja sumber terbuka untuk pemrosesan data terdistribusi. Perintah ini dapat digunakan untuk mengoperasikan dan mengelola sistem file terdistribusi Hadoop.

fs: Merupakan perintah dalam Hadoop yang digunakan untuk berinteraksi dengan sistem file Hadoop, seperti Hadoop Distributed File System (HDFS), untuk melakukan operasi seperti membaca, menulis, menghapus, dan mengelola file dan direktori.

put: Merupakan perintah dalam Hadoop yang digunakan untuk mengunggah file atau direktori lokal ke sistem file terdistribusi Hadoop, seperti HDFS.

pyspark.sql: Merupakan modul dalam PySpark yang menyediakan API untuk operasi pemrosesan data terstruktur menggunakan dataframe dan SQL dalam Apache Spark.

SQLContext: Merupakan kelas dalam PySpark yang digunakan untuk membuat objek yang memungkinkan eksekusi perintah SQL dalam Apache Spark.

createOrReplaceTempView: Merupakan perintah dalam PySpark yang digunakan untuk membuat tampilan sementara (temporary view) dari dataframe atau RDD agar dapat dieksekusi menggunakan perintah SQL.

show: Merupakan perintah dalam PySpark yang digunakan untuk menampilkan isi dataframe atau hasil dari eksekusi perintah SQL dalam bentuk tabel untuk dilihat dalam output console.
```

##  Membuat DataFrame dari Database Eksternal
### Code 7
```sh
df1 = spark.read.format('jdbc').options(url='jdbc:mysql://ebt-polinema.id:3306/polinema_pln?user=ebt&password=EBT@2022@pltb', dbtable='t_wind_turbine').load()
df1.show()
```
### Code 8
```sh
df2 = spark.read.format('jdbc').options(url='jdbc:mysql://ebt-polinema.id:3306/polinema_pln', dbtable='t_wind_turbine', user='ebt', password='EBT@2022@pltb').load()
df2.show()
```
Screenshot:

Penjelasan code
```sh
spark.read.format: Merupakan perintah dalam Apache Spark yang digunakan untuk menentukan format data yang akan dibaca, seperti "jdbc" untuk membaca data dari sumber JDBC.

jdbc: Merupakan metode dalam perintah spark.read.format yang digunakan untuk membaca data dari sumber JDBC, seperti database relasional.

options: Merupakan metode dalam perintah jdbc yang digunakan untuk mengatur opsi atau konfigurasi tambahan untuk koneksi ke sumber JDBC, seperti URL koneksi, nama tabel, pengguna, kata sandi, dan opsi lainnya.

load: Merupakan metode dalam perintah jdbc yang digunakan untuk memuat data dari sumber JDBC berdasarkan konfigurasi yang telah ditentukan sebelumnya menggunakan metode options.

show: Merupakan perintah yang digunakan untuk menampilkan isi data yang telah dimuat dari sumber JDBC dalam bentuk tabel pada output console.
```

##  Mengonversi DataFrames ke RDDs
### Code 9
```sh
# Get into PySpark Shell and execute below commands. 
# Create DataFrame and convert to RDD  
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreatingDataFrames").getOrCreate()
mylist = [(1, "MuhamadAlifRizki-2041720196"),(3, "Big Data 2023")]
myschema = ['col1', 'col2']
df = spark.createDataFrame(mylist, myschema)

#Convert DF to RDD
df.rdd.collect()

df2rdd = df.rdd
df2rdd.take(2)

print(df2rdd.collect())
```
Screenshot:

Penjelasan code
```sh
collect: Merupakan perintah yang digunakan untuk mengumpulkan atau mengambil semua data dalam RDD dan mengembalikannya sebagai suatu array lokal di driver program. Perintah ini berguna untuk mengambil hasil data dari RDD untuk pengolahan lebih lanjut di driver program.

rdd: Merupakan objek dasar dalam Apache Spark yang merepresentasikan kumpulan data yang didistribusikan dan dapat diolah secara paralel di dalam cluster. RDD dapat dibuat dari sumber eksternal, seperti file teks atau data yang dihasilkan dari operasi-transformasi pada RDD lainnya.

take: Merupakan perintah yang digunakan untuk mengambil sejumlah n elemen pertama dari RDD dan mengembalikannya dalam bentuk array. Perintah ini berguna untuk mengambil sebagian data dari RDD sebagai sampel atau untuk keperluan pengujian dan debugging.
```

##  Membuat Datasets
### Code 10
```sh
//Below Scala example creates a Dataset and DataFrame from an RDD. Enter to scala shell with //spark-shell command. 

case class Dept(dept_id: Int, dept_name: String)

val deptRDD = sc.makeRDD(Seq(Dept(1,"Sales"),Dept(2,"HR")))

val deptDS = spark.createDataset(deptRDD)

val deptDF = spark.createDataFrame(deptRDD)
```
### Code 11
```sh

deptDS.rdd

//res12: org.apache.spark.rdd.RDD[Dept] = MapPartitionsRDD[5] at rdd at 
//<console>:31

deptDF.rdd

//res13: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = //MapPartitionsRDD[8] at rdd at <console>:31


//Compile time safety check is done as shown in below code. Since dept_location is not a //member of Dept case class, it will throw an error.

deptDS.filter(x => x.dept_location > 1).show()

//<console>:31: error: value dept_location is not a member of Dept
//       deptDS.filter(x => x.dept_location > 1).show()
```
Screenshot:

Penjelasan code
```sh
makeRDD: Merupakan perintah yang digunakan untuk membuat RDD (Resilient Distributed Dataset) dari suatu koleksi data yang ada di dalam driver program. Koleksi data tersebut bisa berupa array, list, atau kumpulan data lainnya.

Seq: Merupakan tipe data dalam Scala yang digunakan untuk merepresentasikan suatu urutan elemen yang dapat diiterasi. Perintah Seq digunakan sebagai input untuk membuat RDD menggunakan perintah makeRDD.

createDataset: Merupakan perintah yang digunakan untuk membuat Dataset dalam Apache Spark. Dataset adalah tipe data yang menggabungkan keuntungan RDD dan DataFrame, dan biasanya digunakan untuk pengolahan data yang memerlukan tipe data yang terstruktur.

filter: Merupakan perintah yang digunakan untuk melakukan operasi transformasi pada RDD atau Dataset dalam Apache Spark. Perintah filter digunakan untuk menghasilkan suatu RDD atau Dataset baru yang hanya berisi elemen-elemen yang memenuhi suatu kondisi atau predikat tertentu.
```

##  Mengonversi DataFrame ke Datasets dan sebaliknya
### Code 12
```sh
//A DataFrame can be converted to a Dataset by providing a class with �as� method as // shown in below example.

val newDeptDS = deptDF.as[Dept]

//newDeptDS: org.apache.spark.sql.Dataset[Dept] = [dept_id: int, dept_name: string]

//Use toDF function to convert Dataset to DataFrame.  Here is another Scala example 
//for converting the Dataset created above to DataFrame. 

newDeptDS.first()

//res27: Dept = Dept(1,Sales)

newDeptDS.toDF.first()

//res28: org.apache.spark.sql.Row = [1,Sales]

//Note that res27 is resulting a Dept case class object and res28 is resulting a Row object.


```

Screenshot:

Penjelasan code
```sh
as: Merupakan perintah yang digunakan untuk mengkonversi suatu Dataset atau RDD ke tipe data yang diinginkan. Perintah as biasanya digunakan untuk mengubah tipe data kolom dalam suatu DataFrame atau Dataset.

toDF: Merupakan perintah yang digunakan untuk mengubah suatu RDD menjadi DataFrame dalam Apache Spark. Perintah toDF biasanya digunakan untuk memberikan nama kolom pada RDD yang tidak memiliki skema.

first: Merupakan perintah yang digunakan untuk mengambil elemen pertama dari suatu RDD atau Dataset dalam Apache Spark. Perintah first menghasilkan suatu elemen tunggal yang berada pada posisi pertama dalam RDD atau Dataset, berdasarkan urutan yang ditentukan.
```

##  Mengakses Metadata menggunakan Catalog
### Code 13
```sh
// Accessing metadata information about Hive tables and UDFs is made easy with Catalog API. //Following commands explain how to access metadata. 

spark.catalog.listDatabases().select("name").show()

spark.catalog.listTables.show()

spark.catalog.isCached("sample_07")

spark.catalog.listFunctions().show() 
```

Screenshot:

Penjelasan code
```sh
listDatabases: Merupakan perintah yang digunakan untuk menampilkan daftar basis data yang tersedia dalam konteks Spark SQL.

listTables: Merupakan perintah yang digunakan untuk menampilkan daftar tabel yang tersedia dalam basis data yang aktif dalam konteks Spark SQL.

listFunctions: Merupakan perintah yang digunakan untuk menampilkan daftar fungsi atau operasi yang tersedia dalam Spark SQL.

isCached: Merupakan perintah yang digunakan untuk memeriksa apakah suatu DataFrame atau Dataset telah di-cache (disimpan dalam memori) dalam Spark SQL.

select: Merupakan perintah yang digunakan untuk melakukan operasi pemilihan kolom (proyeksi) dalam Spark SQL. Perintah select digunakan untuk memilih kolom yang ingin ditampilkan atau diproses dari suatu DataFrame atau Dataset dalam Spark SQL.
```

##  Bekerja dengan berkas teks
### Code 14
```sh
# To load text files, we use �text� method which will return a single column with column name as �value� and type as string.
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreatingDataFrames").getOrCreate()
df_txt = spark.read.text("data/Chapter4/examples/src/main/resources/people.txt")
df_txt.show()
df_txt
# DataFrame[value: string]

```

Screenshot:

Penjelasan code
```sh
Perintah Read dengan metode text dalam Apache Spark digunakan untuk membaca teks dari satu atau beberapa file dalam format teks sebagai RDD (Resilient Distributed Dataset). RDD merupakan salah satu konsep dasar dalam pemrograman dengan Spark, yang merupakan kumpulan data yang terdistribusi secara resilient (tahan banting) di dalam cluster komputasi. Perintah text digunakan untuk membaca teks dalam file-file tersebut dan menghasilkan RDD yang berisi baris-baris teks dalam format yang dapat diproses lebih lanjut dalam Spark.
```
##  Bekerja dengan JSON
### Code 15
```sh
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreatingDataFrames").getOrCreate()
df_json = spark.read.load("data/Chapter4/examples/src/main/resources/people.json", format="json")
df_json = spark.read.json("data/Chapter4/examples/src/main/resources/people.json")
df_json.printSchema()

df_json.show()

```
### Code 16
```sh
# To write data to another JSON file, use below command.

df_json.write.json("newjson_dir")
df_json.write.format("json").save("newjson_dir2")

```
### Code 17
```sh
# To write data to any other format, just mention format you want to save. Below example saves df_json DataFrame in Parquet format. 

df_json.write.parquet("parquet_dir")
df_json.write.format("parquet").save("parquet_dir2")

```

Screenshot:

Penjelasan code
```sh
load: Memuat data dari sumber eksternal ke dalam Apache Spark.

json: Membaca data dalam format JSON.

format: Metode untuk menentukan format data yang akan dibaca atau ditulis dalam Spark.

printSchema: Mencetak skema data yang dimuat dalam format JSON.

write: Menulis data ke dalam Spark.

save: Metode untuk menyimpan data ke dalam format yang ditentukan.

parquet: Format penyimpanan data yang digunakan dalam Apache Spark untuk kompresi dan pengolahan data yang efisien.
```

##  Bekerja dengan CSV
### Code 18
```sh
# Copy input dataset (cars.csv) to HDFS and then get into PySpark Shell. 

# [cloudera@quickstart spark-2.0.0-bin-hadoop2.7 ]$  wget https://raw.githubusercontent.com/databricks/spark-csv/master/src/test/resources/cars.csv --no-check-certificate

# [cloudera@quickstart spark-2.0.0-bin-hadoop2.7 ]$ hadoop fs -put cars.csv
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreatingDataFrames").getOrCreate()
csv_df = spark.read.options(header='true',inferSchema='true').csv("data/Chapter4/examples/src/main/resources/cars.csv")

csv_df.printSchema()

csv_df.select('year', 'model').write.options(codec="org.apache.hadoop.io.compress.GzipCodec").csv('newcars.csv')

```

Screenshot:

Penjelasan code
```sh
Options: Opsi yang digunakan untuk mengkonfigurasi pembacaan atau penulisan data dalam Apache Spark.

inferSchema: Opsi untuk mengizinkan Spark untuk menginfer skema data secara otomatis dari data yang dimuat.

csv: Metode untuk membaca atau menulis data dalam format CSV.

header: Opsi untuk menentukan apakah baris pertama dalam data CSV adalah header kolom.

codec: Opsi untuk menentukan jenis codec yang digunakan untuk kompresi data saat membaca atau menulis data.
```
