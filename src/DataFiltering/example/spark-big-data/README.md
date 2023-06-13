
<br />
<div align="center">

<h3 align="center">Spark Big Data</h3>

  <p align="center">
    Pengumpulan tugas Spark Big Data minggu 7
  </p>
</div>

## Memulai Spark
![image](https://user-images.githubusercontent.com/86558365/228115462-3cb62086-c4e5-4cb0-ba3b-e434b9aef702.png)
di sini saya menggunakan Spark versi 3.3.2 yang dijalankan di dalam container dalam docker

## Spark-shell
```sh
import sys.process._
val res = "ls /tmp" !
println(res)
```
![image](https://user-images.githubusercontent.com/86558365/228116243-4f9be6e9-c81d-4df7-94c5-ff262b245b68.png)
![image](https://user-images.githubusercontent.com/86558365/228116351-fbaa226e-3161-4ab1-8c36-520a2c154cdc.png)

## Memulai PySpark
### Code 1
```sh
pyspark
```
![image](https://user-images.githubusercontent.com/86558365/228116582-2777f2fb-2883-4e70-be92-23c73d14d45c.png)
```sh
myaccum = sc.accumulator(0)
myrdd = sc.parallelize(range(1,100))
myrdd.foreach(lambda value: myaccum.add(value))
print (myaccum.value)
```
![image](https://user-images.githubusercontent.com/86558365/228116918-bd1a58fa-a9f6-41e8-a17d-be89a178aa3f.png)

untuk menampilkan jobs yang telah dilakukan bisa dilihat di Spark Jobs
![image](https://user-images.githubusercontent.com/86558365/228117192-8132132a-40fe-4977-9da9-d3d423fa1cfc.png)

Penjelasan code
```sh
sc          : Sebuah SparkContext merepresentasikan koneksi ke cluster Spark, dan dapat digunakan untuk membuat variabel RDD dan broadcast pada cluster tersebut.
accumulator : Variabel yang dapat diakses secara aman dan dibagi di antara semua task yang dieksekusi pada cluster Spark,dapat digunakan untuk menghitung jumlah, menghitung rata-rata, dan menghitung varians, atau menghitung parameter lainnya.
parallelize : Metode pada Spark untuk membuat sebuah RDD (Resilient Distributed Dataset) dari sebuah koleksi data yang ada di driver program.
lambda      : Digunakan untuk mendefinisikan fungsi anonim yang akan dijalankan pada setiap elemen RDD saat method foreach dipanggil.
value       : Merupakan nilai dari setiap elemen RDD.
```

## Tugas Praktikum
### Code 2
```sh
from pyspark import *
# sc = SparkContext()
sc = SparkContext.getOrCreate();

broadcastVar = sc.broadcast(list(range(1, 100)))
# broadcastVar.value
print (broadcastVar.value)
```
![image](https://user-images.githubusercontent.com/86558365/228117652-954954bd-9868-48bc-b5dd-0656e9afc907.png)

Penjelasan code
```sh
boradcast: Digunakan untuk mendistribusikan variabel secara efisien ke setiap worker di dalam cluster Spark.
list     : Digunakan untuk membuat sebuah list.
range    : Membuat sebuah range yang dimulai dari 1 dan berakhir pada 99.
```

### Code 3
```sh
from pyspark import *
# sc = SparkContext()
sc = SparkContext.getOrCreate();

# Get the lines from the textfile, create 4 partitions
access_log = sc.textFile("code/practicum_week7/data/README.md", 4)

#Filter Lines with ERROR only
error_log = access_log.filter(lambda x: "Spark" in x)

# Cache error log in memory
cached_log = error_log.cache()

# Now perform an action -  count
print ("Total number of Spark records are %s" % (cached_log.count()))

# Now find the number of lines with 
print ("Number of product pages visited that have Spark is %s" % (cached_log.filter(lambda x: "product" in x).count()))
```
![image](https://user-images.githubusercontent.com/86558365/228118262-aa8971a7-ab64-43a1-a037-b2f7be7f13dd.png)

Penjelasan code
```sh
textFile: Membaca file teks dari Hadoop Distributed File System (HDFS), atau dari sistem file lokal.
filter  : Filter pada RDD berdasarkan suatu kondisi.
cache   : Menyimpan RDD di dalam memory atau disk, agar tidak perlu melakukan komputasi ulang saat RDD tersebut dibutuhkan lagi.
count   : Menghitung jumlah elemen pada RDD
```

### Code 4
```sh
from pyspark import *
# sc = SparkContext()
sc = SparkContext.getOrCreate();

mylist = ["my", "pair", "rdd"]
myRDD = sc.parallelize(mylist)
myPairRDD = myRDD.map(lambda s: (s, len(s)))
# myPairRDD.collect()
# myPairRDD.keys().collect()
# myPairRDD.values().collect()
print (myPairRDD.collect())
print (myPairRDD.keys().collect())
print (myPairRDD.values().collect())
```
![image](https://user-images.githubusercontent.com/86558365/228118486-cc85b369-25da-4ac4-864f-752d87ae6b42.png)

Penjelasan code
```sh
map     : Membuat RDD baru dari myRDD dengan mengaplikasikan sebuah fungsi lambda pada setiap elemen RDD.
collect : Mengumpulkan seluruh elemen pada RDD hasil dari map.
len     : Menghitung panjang dari setiap elemen RDD.
keys    : Mengambil kunci dari setiap elemen tuple pada RDD 'myPairRDD'.
values  : Mengambil nilai dari setiap elemen tuple pada RDD 'myPairRDD'.
```

### Code 5
```sh
from pyspark import *
# sc = SparkContext()
sc = SparkContext.getOrCreate();

mylist = ["my", "pair", "rdd"]
myRDD = sc.parallelize(mylist)
myPairRDD = myRDD.map(lambda s: (s, len(s)))
# myPairRDD.collect()
# myPairRDD.keys().collect()
# myPairRDD.values().collect()
print (myPairRDD.collect())
print (myPairRDD.keys().collect())
print (myPairRDD.values().collect())
```
![image](https://user-images.githubusercontent.com/86558365/228118657-89af497d-e1ab-42a3-a63a-cad4b2a15688.png)

Penjelasan code
```sh
defaultParallelism      : Menentukan jumlah partisi secara default pada RDD yang akan dibuat. Jumlah partisi dapat disesuaikan dengan besarnya cluster.
getNumPartitions        : Mendapatkan jumlah partisi pada RDD.
mapPartitionsWithIndex  : Mengaplikasikan sebuah fungsi pada setiap partisi RDD dengan menyertakan index partisi.
repartition             : Mengatur ulang partisi pada RDD dengan jumlah partisi yang baru.
coalesce                : Mengurangi jumlah partisi pada RDD dengan menggabungkan beberapa partisi ke dalam satu partisi.
toDebugString           : Mendapatkan informasi terperinci mengenai RDD seperti jumlah partisi, jumlah elemen pada setiap partisi, dan informasi lainnya.
```

### Code 6
```sh
from pyspark import *
# sc = SparkContext()
sc = SparkContext.getOrCreate();

from operator import add
lines = sc.textFile("code/practicum_week7/data/README.md")
counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))
```
![image](https://user-images.githubusercontent.com/86558365/228118841-e4ef659b-bf45-4b0d-af8b-3c9fbfff74e9.png)

Penjelasan code
```sh
flatMap      : Memecah setiap baris pada lines menjadi kata-kata terpisah menggunakan pemisah spasi, sehingga menghasilkan RDD yang berisi semua kata pada file README.md.
reduceByKey  : Menghitung jumlah kemunculan setiap kata pada RDD yang dihasilkan dari flatMap.
split        : Memecah setiap baris pada lines menjadi kata-kata terpisah menggunakan pemisah spasi, sehingga menghasilkan RDD yang berisi semua kata pada file README.md.
```