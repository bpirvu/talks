
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

print "Spark version:",sc.version
print "Pandas version:",pd.__version__

from pandas import Series, DataFrame

from os import getenv
DATADIR = getenv("DATADIR")
SUBDIR = '/PUBLIC/movielens/ml-1m'
DATADIR += SUBDIR


# ---
# # Spark DataFrames

# In[2]:


usersDF = spark.read.csv("%s/users.csv" % DATADIR, sep=',', header=False, inferSchema=True)
usersDF =  usersDF.withColumnRenamed('_c0', 'UserID') 									.withColumnRenamed('_c1', 'Gender') 									.withColumnRenamed('_c2', 'Age') 									.withColumnRenamed('_c3', 'Occupation') 									.withColumnRenamed('_c4', 'ZipCode')
usersDF.createOrReplaceTempView("users")


# In[3]:


spark.sql("select * from users limit 5").show()


# In[4]:


ratingsDF = spark.read.csv("%s/ratings.csv" % DATADIR, sep=',', header=False, inferSchema=True)
ratingsDF =  ratingsDF.withColumnRenamed('_c0', 'UserID') 											.withColumnRenamed('_c1', 'MovieID') 											.withColumnRenamed('_c2', 'Rating') 											.withColumnRenamed('_c3', 'Timestamp')
ratingsDF.createOrReplaceTempView("ratings")


# In[5]:


spark.sql("select Rating, count(*) as Cnt from ratings group by Rating").show()


# ---
# ## Join DataFrames

# In[6]:


spark.sql("select * from users u inner join ratings r on u.UserID = r.UserID").createOrReplaceTempView("ratingsWithUserData")


# In[7]:


spark.sql('select * from ratingsWithUserData limit 10').show()


# In[8]:


spark.sql('''
	select Rating, Gender, count(*) as Cnt
	from ratingsWithUserData
	group by Rating, Gender
	order by Rating, Gender
''').show()


# In[10]:


fRatingsNr=spark.sql("select count(*) from ratingsWithUserData where Gender = 'F'").collect()[0][0]
mRatingsNr=spark.sql("select count(*) from ratingsWithUserData where Gender = 'M'").collect()[0][0]
print "Nr. of ratings by female users:",fRatingsNr
print "Nr. of ratings by male users:  ",mRatingsNr


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[26]:


from pyspark.sql.types import IntegerType, FloatType, DoubleType
from pyspark.sql.functions import udf

normalize_udf = udf(lambda cnt, gender: 1.*cnt/fRatingsNr if gender=='F' else 1.*cnt/mRatingsNr, DoubleType())

ratingsHistogram=(
	ratingsHistogram.withColumn("CntNormalized", normalize_udf(ratingsHistogram.Cnt, ratingsHistogram.Gender))
)


# In[27]:


ratingsHistogram.show(20)


# In[20]:


ratingsHistogram.groupby('Gender').sum('CntNormalized').show()


# ---
# ## Read movies data

# In[210]:


from pyspark.sql import types as T
from pyspark.sql import functions as F


# In[211]:


moviesDF = spark.read.csv("%s/movies.csv" % DATADIR, sep='+', header=False, inferSchema=True)
moviesDF =  moviesDF.withColumnRenamed('_c0', 'MovieID') 									.withColumnRenamed('_c1', 'Title') 									.withColumnRenamed('_c2', 'Genres')


# In[212]:


moviesDF.show(3, truncate=50)


# In[213]:


split_udf = udf(lambda s: s.split("|"), T.ArrayType(T.StringType()))
moviesDF=moviesDF.withColumn("Genres", split_udf(moviesDF['Genres']))


# In[214]:


moviesDF.show(3, truncate=50)


# In[216]:


# moviesDF=moviesDF.withColumn('Genre', F.explode(moviesDF.Genres))
# moviesDF = moviesDF.drop('Genres')
# moviesDF.show(10, truncate=50)


# In[215]:


ratingsWithUserAndMovieDataDF = ratingsWithUserDataDF.join(moviesDF, how='inner', on='MovieID')


# In[217]:


print "Nr. of rows:", ratingsWithUserAndMovieDataDF.count()
ratingsWithUserAndMovieDataDF.sort(['MovieID','UserID']).show(3, truncate=50)


# In[193]:


# ratingsWithUserAndMovieDataDF = (
# 	ratingsWithUserAndMovieDataDF
# 	.withColumn('Genre', F.explode(ratingsWithUserAndMovieDataDF.Genres))
# 	.drop('Genres')
# )


# In[194]:


# print "Nr. of rows:", ratingsWithUserAndMovieDataDF.count()
# ratingsWithUserAndMovieDataDF.sort(['MovieID','UserID']).show(4, truncate=50)


# In[196]:


# bubu = (
# 	ratingsWithUserAndMovieDataDF
# 	.withColumn('Genre', F.explode(ratingsWithUserAndMovieDataDF.Genres))
# 	.drop('Genres')
# 	.groupBy(['Gender','Rating','Genre'])
# 	.agg({"*":'count', "Age":'mean'})
# 	.sort(['Genre','Gender','Rating'])
# )


# In[198]:


# ratingsWithUserAndMovieDataDF.printSchema()
# ratingsWithUserAndMovieDataDF=ratingsWithUserAndMovieDataDF.withColumn(
# 'Rating', ratingsWithUserAndMovieDataDF.Rating.astype(DoubleType())
# )
# ratingsWithUserAndMovieDataDF.printSchema()


# In[218]:


ratingsWithUserAndMovieDataDF.show(5)


# In[221]:


ratingsByGenderAndGenreSF = (
	ratingsWithUserAndMovieDataDF
	.withColumn('Genre', F.explode(ratingsWithUserAndMovieDataDF.Genres))
	.drop('Genres')
	.groupBy(['Gender','Rating','Genre'])
	.agg(
		F.count('*').alias('Cnt'),
		F.mean('Age').alias('AvgAge')
	)
# 	.agg({"*":'count', "Age":'mean'})
	.sort(['Genre','Gender','Rating'])
)


# In[222]:


ratingsByGenderAndGenreSF.show(20)


# In[225]:


avgRatingsByGenderAndGenreDF = (
	ratingsWithUserAndMovieDataDF
	.withColumn('Genre', F.explode(ratingsWithUserAndMovieDataDF.Genres))
	.drop('Genres')
	.groupBy(['Gender','Genre'])
	.agg(
		F.count('*').alias('Cnt'),
		F.mean('Rating').alias('AvgRating')
	)
# 	.agg({"*":'count', "Rating":'mean'})
	.sort(['Genre','Gender'])
)


# In[226]:


avgRatingsByGenderAndGenreDF.show(20)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




