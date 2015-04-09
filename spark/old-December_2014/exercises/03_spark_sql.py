'''
A SQLConext wraps the SparkContext,
and adds functions for working with structured data.
'''
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)


'''
Now we can load a set of data in that is stored in the Parquet format.
Parquet is a self-describing columnar format. Since it is self-describing,
Spark SQL will automatically be able to infer all of the column names and their datatypes. 
'''
wikiData = sqlCtx.parquetFile("file:///root/data/wiki_parquet")

'''
The result of loading in a parquet file is a SchemaRDD.
A SchemaRDD has all of the functions of a normal RDD.
For example, lets figure out how many records are in the data set.
'''
wikiData.count()

'''
In addition to standard RDD operatrions, SchemaRDDs also have
extra information about the names and types of the columns in the dataset.
This extra schema information makes it possible to run SQL queries against
the data after you have registered it as a table.
'''
wikiData.registerAsTable("wikiData")
# to describe the schema
wikiData.printSchema()

# number of record (again)
result = sqlCtx.sql("SELECT COUNT(*) AS pageCount FROM wikiData").collect()
result[0].pageCount



'''
SQL can be a powerfull tool from performing complex aggregations.
For example, the following query returns the top 10 usersnames by the number of pages
they created.
'''
sqlCtx.sql("SELECT username, COUNT(*) AS cnt FROM wikiData WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10").collect()

'''
Using the 'LIKE' operator (inside the SQL query),
select the id and title of the wiki articles
that contain both 'italy' and 'bologna' in its text
'''
#		LIKE '%WORD_TO_BE_MATCHED%'