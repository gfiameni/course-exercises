# create file 'hamlet.txt'
echo -e "to be\nor not to be" > /usr/local/spark/hamlet.txt

IPYTHON=1 pyspark

lines = sc.textFile('file:///usr/local/spark/hamlet.txt')

words = lines.flatMap(lambda line: line.split(' '))
w_counts = words.map(lambda word: (word, 1))
counts = w_counts.reduceByKey(lambda x, y: x + y)

counts.collect()

# descending order:
counts.sortBy(lambda (word,count): count, ascending=False).take(3)
