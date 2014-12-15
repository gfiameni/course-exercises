# 	create file 'hamlet.txt'
#	echo -e "to be\nor not to be" > /usr/local/spark/hamlet.txt
#
#	Launch pyspark:
#	IPYTHON=1 pyspark

lines = sc.textFile('file:///usr/local/spark/hamlet.txt')

'''
fill the dots:
'''
words = lines.flatMap( ... )
w_counts = words.map( ... )
counts = w_counts.reduceByKey( ... )

counts.collect()

# Select the top 3 words appearing in the text:

