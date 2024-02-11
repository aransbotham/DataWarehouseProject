#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[3]:


# Python Modules
import boto3
import json
import getpass
import configparser
import pandas as pd
import psycopg2

# Local Functions
import utils.redshift_utils as redshift_utils
from queries.sql_queries import staging_tables, dimm_tables
from scripts.create_tables import run as create_tables
from scripts.etl import run as etl


# # AWS Setup

# ## Config

# In[4]:


# Enter AWS KEY and Secret
KEY = getpass.getpass(prompt='Enter AWS Access key ID:')
SECRET = getpass.getpass(prompt='Enter AWS Secret access key')


# In[5]:


# Load Parameters from Config File
CONFIG_FILE = 'utils/dwh.cfg'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

# Provisioning
REGION = config.get("AWS", "REGION")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME = config.get("IAM", "DWH_IAM_ROLE_NAME")

# Querying
DWH_ENDPOINT = config.get("CLUSTER", "HOST")
DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")


# In[6]:


# S3 Client
s3 = boto3.resource(
    's3', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET
)


# In[7]:


# Confirm Access to Sample Data
sampleDbBucket = s3.Bucket("udacity-dend")

for obj in sampleDbBucket.objects.filter(Prefix="log_data"):
    print(obj)


# ## Role Provisioning

# In[8]:


# Create Role
redshift_utils.create_role(KEY, SECRET, REGION)


# ## Cluster Creation

# In[9]:


# Create Cluster
redshift_utils.create_cluster(KEY, SECRET, REGION)


# # ETL

# ## Create Tables

# In[10]:


# Create staging, fact and dim tables
create_tables()


# ## Load Tables

# In[11]:


# Load data into staging, fact and dim tables
etl()


# # QA

# In[12]:


# Establish Connection to Database
conn = psycopg2.connect(
    "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
)

# Create cursor object
cur = conn.cursor()

# Establish conection to the cluster
conn_string = "postgresql://{}:{}@{}:{}/{}".format(
    DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB
)

get_ipython().run_line_magic('sql', '$conn_string')


# ## Staging Tables

# In[13]:


redshift_utils.execute_qa_count_queries(cur, staging_tables)
redshift_utils.execute_qa_row_queries(cur, staging_tables)


# ## Fact & Dim Tables

# In[14]:


redshift_utils.execute_qa_count_queries(cur, dimm_tables)
redshift_utils.execute_qa_row_queries(cur, dimm_tables)


# # Analysis

# ### What timeframe of data do we have?

# In[15]:


get_ipython().run_cell_magic('sql', '', 'SELECT distinct month, year from time;')


# ### Which songs are getting the most users listening?

# In[16]:


get_ipython().run_cell_magic(
    'sql',
    '',
    'SELECT a.name as artist, s.title AS song, count(distinct user_id) AS users_listening\nFROM songplays sp \nJOIN songs s ON (s.song_id = sp.song_id) \nJOIN artists a ON (a.artist_id = sp.artist_id)\nGROUP BY a.name, song\nORDER BY count(distinct user_id) desc\nLIMIT 5;',
)


# ### Which songs are getting the most plays?

# In[17]:


get_ipython().run_cell_magic(
    'sql',
    '',
    'SELECT a.name as artist, s.title AS song, count(*) AS users_listening\nFROM songplays sp \nJOIN songs s ON (s.song_id = sp.song_id) \nJOIN artists a ON (a.artist_id = sp.artist_id)\nGROUP BY a.name, song\nORDER BY count(*) desc\nLIMIT 5;',
)


# ### When is the highest usage time of day by hour for songplays?

# In[18]:


get_ipython().run_cell_magic(
    'sql',
    '',
    'SELECT t.hour, count(*) AS frequency\nFROM songplays sp\nJOIN time t\n    ON sp.time_id = t.time_id\nGROUP BY hour\nORDER BY count(*) desc\nLIMIT 5;',
)


# ### What is the highest usage time of day per day of week for songplays?

# In[19]:


get_ipython().run_cell_magic(
    'sql',
    '',
    'SELECT t.weekday, t.hour, count(*) AS frequency\nFROM songplays sp\nJOIN time t\n    ON sp.time_id = t.time_id\nGROUP BY weekday, hour\nORDER BY count(*) desc',
)


# # Clean Up

# In[20]:


redshift_utils.redshift_cleanup(KEY, SECRET, REGION)
