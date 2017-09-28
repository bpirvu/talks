
# coding: utf-8

# In[2]:


# print "Spark version:",sc.version

# import pandas as pd
# import numpy as np

# from pandas import Series, DataFrame

from os import getenv
DATADIR = getenv("DATADIR")
SUBDIR = '/PUBLIC/movielens/ml-1m'
DATADIR += SUBDIR


# In[3]:


from sframe import SFrame
from sframe import SArray
from sframe import aggregate as agg


# In[4]:


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
# # Read data into SFrames

# In[4]:


usersSF = SFrame.read_csv(
	"%s/users.dat" % DATADIR, delimiter='::', header=False, verbose=False,
	column_type_hints = [int, str, int, int, str]
)
usersSF = usersSF.rename({
	'X1': 'UserID',
	'X2': 'Gender',
	'X3': 'Age',
	'X4': 'Occupation',
	'X5': 'ZipCode',
})
usersDescSF=dict(zip(usersSF.column_names(), usersSF.column_types()))
print usersDescSF


# In[5]:


ratingsSF = SFrame.read_csv(
	"%s/ratings.dat" % DATADIR, delimiter='::', header=False, verbose=False,
	column_type_hints = [int, int, int, int]
)
ratingsSF = ratingsSF.rename({
	'X1': 'UserID',
	'X2': 'MovieID',
	'X3': 'Rating',
	'X4': 'Timestamp'
})


# In[6]:


# Compute Ratings Histogram:
ratingsHistogram = ratingsSF.groupby(['Rating'], {'Cnt': agg.COUNT()}).sort('Rating')
ratingsHistogram.print_rows()


# ---
# ## Join SFrames

# In[7]:


# Join:
ratingsWithUserDataSF = ratingsSF.join(usersSF, how='inner', on='UserID')


# In[8]:


ratingsWithUserDataSF.head(3)


# In[9]:


# Compute Ratings Histogram by Gender:
ratingsHistogram = ratingsWithUserDataSF.groupby(['Rating','Gender'], {'Cnt': agg.COUNT()}).sort(['Rating','Gender'])
ratingsHistogram.print_rows()


# In[10]:


data = []
data.append(go.Bar(
	x = ratingsHistogram.filter_by('M','Gender')['Rating'].to_numpy(),
	y = ratingsHistogram.filter_by('M','Gender')['Cnt'].to_numpy(),
	name = 'Male'
))
data.append(go.Bar(
	x = ratingsHistogram.filter_by('F','Gender')['Rating'].to_numpy(),
	y = ratingsHistogram.filter_by('F','Gender')['Cnt'].to_numpy(),
	name = 'Female'
))
layout = go.Layout(
  title='Distribution of ratings by gender',
  xaxis=dict(
    title='Rating',
  ),
  yaxis=dict(
    title='Cnt'
  )
)
fig = go.Figure(data=data, layout=layout)
iplot(fig)


# In[11]:


fRatingsNr,mRatingsNr=ratingsWithUserDataSF.groupby(['Gender'], {'Cnt': agg.COUNT()}).sort('Gender')['Cnt']
print "Nr. of ratings by female users:",fRatingsNr
print "Nr. of ratings by male users:  ",mRatingsNr


# In[12]:


ratingsHistogram['CntNormalized'] = 	ratingsHistogram.apply(lambda x: 1.*x['Cnt']/fRatingsNr if x['Gender']=='F' else 1.*x['Cnt']/mRatingsNr)


# In[13]:


ratingsHistogram.print_rows()


# In[14]:


ratingsHistogram.groupby('Gender', agg.SUM('CntNormalized'))


# In[15]:


data = []
data.append(go.Bar(
	x = ratingsHistogram.filter_by('M','Gender')['Rating'].to_numpy(),
	y = ratingsHistogram.filter_by('M','Gender')['CntNormalized'].to_numpy(),
	name = 'Male'
))
data.append(go.Bar(
	x = ratingsHistogram.filter_by('F','Gender')['Rating'].to_numpy(),
	y = ratingsHistogram.filter_by('F','Gender')['CntNormalized'].to_numpy(),
	name = 'Female'
))
layout = go.Layout(
  title='Normalized distribution of ratings by gender',
  xaxis=dict(
    title='Rating',
  ),
  yaxis=dict(
    title='Pct'
  )
)
fig = go.Figure(data=data, layout=layout)
iplot(fig)


# ---
# ## Read movies data

# In[16]:


moviesSF = SFrame.read_csv(
	"%s/movies.dat" % DATADIR, delimiter='::', header=False, verbose=False,
	column_type_hints = [int, str, str]
)
moviesSF = moviesSF.rename({
	'X1': 'MovieID',
	'X2': 'Title',
	'X3': 'Genres'})


# In[17]:


moviesSF.print_rows(4)


# In[18]:


moviesSF['Genres'] = moviesSF['Genres'].apply(lambda x: x.split("|"))


# In[19]:


moviesSF.print_rows(4, max_column_width=50, max_row_width=130)


# In[20]:


moviesLongSF=moviesSF.stack('Genres', new_column_name='Genre')
print 'Number of distinct genres:',moviesLongSF['Genre'].unique().size()


# In[21]:


moviesLongSF.print_rows(10)


# In[22]:


ratingsWithUserAndMovieDataSF = ratingsWithUserDataSF.join(moviesSF, how='inner', on='MovieID')


# In[23]:


ratingsByGenderAndGenreSF=ratingsWithUserAndMovieDataSF['Gender','Rating','Genres']													.stack('Genres', new_column_name='Genre')													.groupby(['Gender','Rating','Genre'], {'Cnt': agg.COUNT()})													.sort(['Genre','Gender','Rating'])


# In[24]:


avgRatingsByGenderAndGenreSF=ratingsWithUserAndMovieDataSF['Gender','Rating','Genres']													.stack('Genres', new_column_name='Genre')													.groupby(['Gender','Genre'], {'AvgRating': agg.AVG('Rating')})													.sort(['Genre','Gender'])


# In[25]:


genres = ratingsByGenderAndGenreSF['Genre'].unique().sort()

for g in genres[:]:
	r = ratingsByGenderAndGenreSF.filter_by(g,'Genre')
	fRatingsNr,mRatingsNr=r.groupby(['Gender'], {'Cnt': agg.SUM('Cnt')}).sort('Gender')['Cnt']
	r['CntNormalized'] = r.apply(lambda x: 1.*x['Cnt']/fRatingsNr if x['Gender']=='F' else 1.*x['Cnt']/mRatingsNr)
	
# 	print fRatingsNr,mRatingsNr
# 	r.print_rows(5)
	fRatingsAvg,mRatingsAvg = avgRatingsByGenderAndGenreSF.filter_by(g,'Genre')['AvgRating']

	data = []
	data.append(go.Bar(
		x = r.filter_by('M','Gender')['Rating'].to_numpy(),
		y = r.filter_by('M','Gender')['CntNormalized'].to_numpy(),
		name = 'Male, Avg=%f'%mRatingsAvg
	))
	data.append(go.Bar(
		x = r.filter_by('F','Gender')['Rating'].to_numpy(),
		y = r.filter_by('F','Gender')['CntNormalized'].to_numpy(),
		name = 'Female, Avg=%f'%fRatingsAvg
	))
	layout = go.Layout(
		title='%s - Normalized distribution of ratings by gender'%g,
		xaxis=dict(
			title='Rating',
		),
		yaxis=dict(
			title='Pct'
		)
	)
	fig = go.Figure(data=data, layout=layout)
	iplot(fig)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




