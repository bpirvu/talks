
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

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
# # Read data into DataFrames

# In[3]:


usersDF = pd.read_csv(
	"%s/users.csv" % DATADIR, sep=',',
	names=['UserID','Gender','Age','Occupation','ZipCode']
)


# In[4]:


ratingsDF = pd.read_csv(
	"%s/ratings.csv" % DATADIR, sep=',',
	names=['UserID','MovieID','Rating','Timestamp']
)


# In[6]:


# Compute Ratings Histogram:
ratingsHistogram = (
	ratingsDF[['Rating','UserID']].groupby(['Rating']).count()
	.rename(columns={"UserID": "Cnt"}).reset_index()
	.sort_values('Rating', ascending=True)
)
ratingsHistogram


# ---
# ## Join DataFrames

# In[7]:


# Join:
ratingsWithUserDataDF = ratingsDF.merge(usersDF, how='inner', on='UserID')


# In[8]:


ratingsWithUserDataDF.head(3)


# In[9]:


# Compute Ratings Histogram by Gender:
ratingsHistogram = (
	ratingsWithUserDataDF.groupby(['Rating','Gender']).count()
	.rename(columns={'UserID':'Cnt'}).reset_index()[['Rating','Gender','Cnt']]
	.sort_values(['Rating','Gender'])
)
ratingsHistogram.head(10)


# In[10]:


# Compute Ratings Histogram by Gender:
ratingsHistogram = (
	ratingsWithUserDataDF.groupby(['Rating','Gender']).count()
	.rename(columns={'UserID':'Cnt'})[['Cnt']]
 	.sort_index(ascending=[True,True])
)
ratingsHistogram.head(10)


# In[11]:


fRatingsNr=ratingsWithUserDataDF.groupby(['Gender']).count()['UserID']['F']
mRatingsNr=ratingsWithUserDataDF.groupby(['Gender']).count()['UserID']['M']
print "Nr. of ratings by female users:",fRatingsNr
print "Nr. of ratings by male users:  ",mRatingsNr


# In[12]:


ratingsHistogram = ratingsHistogram.reset_index()
ratingsHistogram['CntNormalized'] = (
	ratingsHistogram
	.apply(lambda x: 1.*x['Cnt']/fRatingsNr if x['Gender']=='F' else 1.*x['Cnt']/mRatingsNr, axis=1)
)


# In[13]:


ratingsHistogram


# In[14]:


ratingsHistogram.groupby(['Gender']).sum()[['Cnt','CntNormalized']]


# ---
# ## Read movies data

# In[15]:


moviesDF = pd.read_csv(
	"%s/movies.csv" % DATADIR, sep='+',
	names=['MovieID','Title','Genres']
)


# In[16]:


moviesDF.head(3)


# In[17]:


stackedGenres=moviesDF['Genres'].str.split("|",expand=True).stack()


# In[18]:


stackedGenres.reset_index().head(5)


# In[19]:


moviesDF = (
	moviesDF.reset_index()
	.merge(stackedGenres.reset_index(), how='inner', left_on='index', right_on='level_0')
	.drop(['index','Genres','level_0','level_1'], axis=1)
	.rename(columns={0:'Genre'})
)


# In[20]:


moviesDF.head(6)


# In[21]:


ratingsWithUserAndMovieDataDF = ratingsWithUserDataDF.merge(moviesDF, how='inner', on='MovieID')


# In[22]:


ratingsWithUserAndMovieDataDF.head(5)


# In[23]:


ratingsByGenderAndGenreDF=(
	ratingsWithUserAndMovieDataDF
	.groupby(['Gender','Genre'])
	.agg({
		'Rating': 'mean',
		'UserID': 'count'
	})
	.sort_index()
)


# In[24]:


ratingsByGenderAndGenreDF


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




