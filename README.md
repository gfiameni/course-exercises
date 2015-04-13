# WORKSHOP - "Tools and techniques for massive data analysis"

> https://events.prace-ri.eu/conferenceDisplay.py?confId=314
> http://www.hpc.cineca.it
> http://www.prace-ri.eu

Here you can find all Exercises for the course.

---

### Packages

- Hadoop 1.2.1 (2.5.1/2.5.2)
- Python 2.7
- MrJob 0.4.3-dev
- Apache Spark 1.1.0

### Examples

```bash
$ docker run -it -v ~/course-exercises:/course-exercises cineca/hadoop-mrjob:1.2.1 /etc/bootstrap.sh -bash
root# source /root/mrjob0.4.2/bin/activate
```

```python
from mrjob.job import MRJob
from mrjob.step import MRStep

class job(MRJob):
    def mapper(self, _, line):
        pass
    def reducer(self, key, line):
        pass
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
        ]

if __name__ == "__main__":
    job.run()
```

---
##### MrJob

```bash
$ source /root/mrjob0.4.2/bin/activate (mrjob0.4.3-dev also available)
$ cd /course-exercises/mrjob
$ python word_count.py ../data/txt/2261.txt.utf-8
$ python word_count.py ../data/txt/2261.txt.utf-8 -r hadoop

$ python matmat.py (-r hadoop) ../data/mat/matmat_3x2_A ../data/mat/matmat_2x2_B
$ python sparse_matmat.py (-r hadoop) ../data/mat/smat_100x10_A ../data/mat/smat_10x200_B

$ python smatvec.py ../data/smat_10_5_A.txt ../data/vec_5.txt
$ python matmat.py ../data/smat_10_5_A.txt ../data/smat_5_5.txt
```
---

##### Hadoop

```bash
$ hadoop jar $HADOOP_PREFIX/hadoop-examples-1.2.1.jar pi 3 10
```
---

##### Spark

[Data for the Spark exercises](http://goo.gl/wEnUu8)

```bash
$ cd spark
$ hadoop fs -put ../data/spark/1902 /spark/1902
$ spark-submit max_temp.py /spark/1902
$ hadoop fs -get /user/root/output/part-00000
```
---

##### NGS

```bash

# Hadoop streaming: debug and hadoop run
$ bash ngs/hs/hstream.sh

# Mr job
$ source /root/mrjob0.4.2/bin/activate
$ python ngs/mrjob/runner.py < data/ngs/input.sam 1> data/ngs/out.coverage 2> data/ngs/out.log

```
---

##### Spark Shell

```bash
$ hadoop fs -put ../data/txt/divine_comedy.txt /spark/divine_comedy.txt
$ spark-shell
$ scala> val textFile = sc.textFile("/spark/divine_comedy.txt")
$ scala> textFile.count() // Number of items in this RDD
$ scala> textFile.first() // First item in this RDD
$ scala> val linesWithCanto = textFile.filter(line => line.contains("Canto"))
$ scala> textFile.filter(line => line.contains("Canto")).count()
$ scala> linesWithCanto.cache()
$ scala> linesWithCanto.count()
```

---

##### Hadoop 2.5.2

```bash
$ cd hadoop
$ yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.2.jar pi 16 10

$ hdfs dfs -mkdir /data
$ hdfs dfs -put ../data/txt/2261.txt.utf-8 /data/
$ yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.2.jar wordcount /data wordcount-output
```