
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


# In[2]:


#--------------------------------------------------
import plotly as plotly
print "Plotly version", plotly.__version__  # version >1.9.4 required
import plotly.graph_objs as go
from plotly import tools

# plotly.offline.init_notebook_mode() # run at the start of every notebook
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot #, plot  # Difference .plot / .iplot ???
init_notebook_mode() # run at the start of every ipython notebook to use plotly.offline
                     # this injects the plotly.js source files into the notebook
#--------------------------------------------------
# %matplotlib inline
# import matplotlib.pyplot as plt
# import seaborn as sns
#--------------------------------------------------


# ---
# # Spark DataFrames

# In[3]:


usersDF = spark.read.csv("%s/users.csv" % DATADIR, sep=',', header=False, inferSchema=True)
usersDF =  usersDF.withColumnRenamed('_c0', 'UserID') 									.withColumnRenamed('_c1', 'Gender') 									.withColumnRenamed('_c2', 'Age') 									.withColumnRenamed('_c3', 'Occupation') 									.withColumnRenamed('_c4', 'ZipCode')


# In[4]:


usersDF.show(5)


# In[5]:


ratingsDF = spark.read.csv("%s/ratings.csv" % DATADIR, sep=',', header=False, inferSchema=True)
ratingsDF =  ratingsDF.withColumnRenamed('_c0', 'UserID') 											.withColumnRenamed('_c1', 'MovieID') 											.withColumnRenamed('_c2', 'Rating') 											.withColumnRenamed('_c3', 'Timestamp')


# In[6]:


# Compute Ratings Histogram:
# ratingsHistogram = ratingsDF.groupBy("Rating").agg({'Rating': 'count'})
ratingsHistogram = ratingsDF.groupBy("Rating").count().withColumnRenamed('count','Cnt')
ratingsHistogram.show()


# ---
# ## Join DataFrames

# In[7]:


ratingsWithUserDataDF = ratingsDF.join(usersDF, on='UserID', how='inner')


# In[8]:


ratingsWithUserDataDF.show(5)


# In[9]:


# Compute Ratings Histogram by Gender:
ratingsHistogram = (
	ratingsWithUserDataDF
	.groupby(['Rating','Gender']).count().withColumnRenamed('count','Cnt')
	.orderBy(["Rating", "Gender"], ascending=[1, 1])
)


# In[10]:


ratingsHistogram.show(100)


# In[11]:


fRatingsNr=ratingsWithUserDataDF.filter("Gender = 'F'").count()
mRatingsNr=ratingsWithUserDataDF.filter(ratingsWithUserDataDF['Gender'] == 'M').count()
print "Nr. of ratings by female users:",fRatingsNr
print "Nr. of ratings by male users:  ",mRatingsNr


# In[12]:


from pyspark.sql.types import IntegerType, FloatType, DoubleType
from pyspark.sql.functions import udf

normalize_udf = udf(lambda cnt, gender: 1.*cnt/fRatingsNr if gender=='F' else 1.*cnt/mRatingsNr, DoubleType())

ratingsHistogram=(
	ratingsHistogram.withColumn("CntNormalized", normalize_udf(ratingsHistogram.Cnt, ratingsHistogram.Gender))
)


# In[13]:


ratingsHistogram.show(20)


# In[14]:


ratingsHistogram.groupby('Gender').sum('CntNormalized').show()


# ---
# ## Read movies data

# In[15]:


from pyspark.sql import types as T
from pyspark.sql import functions as F


# In[16]:


moviesDF = spark.read.csv("%s/movies.csv" % DATADIR, sep='+', header=False, inferSchema=True)
moviesDF =  moviesDF.withColumnRenamed('_c0', 'MovieID') 									.withColumnRenamed('_c1', 'Title') 									.withColumnRenamed('_c2', 'Genres')


# In[17]:


moviesDF.show(3, truncate=50)


# In[18]:


split_udf = udf(lambda s: s.split("|"), T.ArrayType(T.StringType()))
moviesDF=moviesDF.withColumn("Genres", split_udf(moviesDF['Genres']))


# In[19]:


moviesDF.show(3, truncate=50)


# In[20]:


ratingsWithUserAndMovieDataDF = ratingsWithUserDataDF.join(moviesDF, how='inner', on='MovieID')


# In[21]:


print "Nr. of rows:", ratingsWithUserAndMovieDataDF.count()
ratingsWithUserAndMovieDataDF.sort(['MovieID','UserID']).show(3, truncate=50)


# In[22]:


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


# In[23]:


ratingsByGenderAndGenreSF.show(20)


# In[24]:


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


# In[25]:


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




