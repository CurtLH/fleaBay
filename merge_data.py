
# coding: utf-8

# # Customize Enviornment

# In[1]:

import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


# In[ ]:




# # Connect to Database

# In[2]:

# create a connection to get data from database
engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')


# In[ ]:




# # Get API Data

# In[3]:

# get API data from the database
api_df = pd.read_sql('ebay_api', con=engine)


# In[4]:

# zap column names into lowercase
api_df.columns = [col.lower() for col in api_df.columns]


# In[5]:

# reduce df to variables of interest
api_df = api_df[['itemid', 'condition_conditiondisplayname', 'listinginfo_endtime', 'sellingstatus_currentprice_value', 'sellingstatus_sellingstate']]


# In[6]:

# rename columns
api_df = api_df.rename(columns={'condition_conditiondisplayname' : 'condition',
                                'listinginfo_endtime' : 'endtime',
                                'sellingstatus_currentprice_value' : 'price',
                                'sellingstatus_sellingstate' : 'status'})


# In[7]:

# show a sample of the data
api_df.head()


# In[ ]:




# # Get Website Data

# In[8]:

# get item attribute info from the database
item_df = pd.read_sql('ebay_item_attr', con=engine)


# In[9]:

# reduce df to variables of interest
item_df = item_df[['itemId', 'Mileage', 'Year', 'CYL', 'TITLE', 'TRANS', 'TRIM', 'EXT_COLOR']]


# In[10]:

# reduce df to the years of interests
item_df = item_df[item_df['Year'] >= 2010]
print("Number of Camaros between 2010 and 2017: {}".format(len(item_df)))


# In[11]:

# convert column names to lowercase
item_df.columns = [x.lower() for x in item_df.columns]


# In[12]:

# show a sample of the data
item_df.head()


# In[ ]:




# # Merge API Data and Website Data

# In[13]:

# join api and attribute data
df = pd.merge(item_df, api_df, how='inner', on='itemid')
print len(df)


# In[14]:

# for some reason, ads before Jan 18 were all sold ads
df.drop(df[df['endtime'] < '2017-01-18'].index, inplace=True)


# In[15]:

# for some reason, the selling price in the API data ($9,000) doesn't match the actual price ($18,700)
df[df['itemid'] == 122319430122]


# In[16]:

# so I'll drop it from the dataset
df.drop(df[df['itemid'] == 122319430122].index, inplace=True)
print len(df)


# In[17]:

# this auction is just for a set of wheels...not the actual car...
df[df['itemid'] == 322439374219]


# In[18]:

# so I'll drop it from the dataset
df.drop(df[df['itemid'] == 322439374219].index, inplace=True)
print len(df)


# In[19]:

# this auction is for a totaled car, but the title says clear...?
df[df['itemid'] == 192121219541]


# In[20]:

# so I'll drop it from the dataset
df.drop(df[df['itemid'] == 192121219541].index, inplace=True)
print len(df)


# In[21]:

# this auction is for a totaled car, but the title says clear...?
df[df['itemid'] == 222418277892]


# In[22]:

# so I'll drop it from the dataset
df.drop(df[df['itemid'] == 222418277892].index, inplace=True)
print len(df)


# In[23]:

# because there are some problems with auctions that ended in November or December, we will remove them
df.drop(df[df.endtime.dt.year <= 2016].index, inplace=True)
print len(df)


# In[24]:

# show a sample of the data
df.head(5)


# In[25]:

# create a connection to write df to database
engine = create_engine('postgresql://postgres:apassword@localhost:5432/postgres')

# load the dataframe into the database
df.to_sql(name='ebay_merged', con=engine, if_exists = 'replace', chunksize=2500, index=False)


# In[ ]:




# # Initial Data Inspection

# In[26]:

df.head(10)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



