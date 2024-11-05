#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T
)
import pyspark
from pyspark import StorageLevel
import datetime

spark = (
    SparkSession.builder
    #.remote("sc://localhost:15002")
    .appName("Wikipedia Mutual Links with Redirects")
    .getOrCreate()
)


# In[2]:


def parquet(prefix):
    files = spark.read.parquet(f"s3://bsu-c535-fall2024-commons/arjun-workspace/{prefix}/")
    files.save(prefix)


# In[3]:


def save(self, name):
    t = datetime.datetime.now()
    (
        self
        # We use the (serialized) memory and disk storage level without
        # replication. Replication helps us in long-running processes if
        # one our nodes in the cluster dies.
        .persist(StorageLevel.MEMORY_AND_DISK)
        .createOrReplaceTempView(name)
    )
    rows = spark.table(name).count()
    print(f"{name} - {rows} rows - elapsed {datetime.datetime.now() - t}")
    spark.table(name).printSchema()

from pyspark.sql import DataFrame
DataFrame.save = save


# In[4]:


# for interactive EMR through Spark Connect


# In[5]:


parquet("linktarget")


# In[6]:


parquet("page")


# In[7]:


parquet("redirect")


# In[8]:


parquet("pagelinks")


# In[10]:


spark.table("redirect") \
    .join(
        spark.table("page"),
        (F.col("page_title") == F.col("rd_title")) & 
        (F.col("page_namespace") == F.col("rd_namespace"))
    ) \
    .select(
        F.col("rd_from").alias("redirect_source_id"),  
        F.col("page_id").alias("redirect_target_id")
    ) \
    .distinct() \
    .createOrReplaceTempView("lt_page")

# Show the first 10 rows of the lt_page view
spark.table("lt_page").show(10)


# In[11]:


# Step 2: Join page and linktarget to create page_linktarget view
spark.table("linktarget")\
    .join(
        spark.table("page"),
        (F.col("lt_title") == F.col("page_title")) & 
        (F.col("lt_namespace") == F.col("page_namespace")),
        how='inner'
    )\
    .select(
        F.col("page_id").alias("page_id"),
        F.col("page_namespace").alias("page_namespace"),
        F.col("page_title").alias("page_title"),
        F.col("lt_id").alias("link_target_id")
    )\
    .createOrReplaceTempView("page_linktarget")


# In[14]:


# Step 3: Join page_linktarget with PageLink to create page_linktarget_with_pagelink view
spark.table("pagelinks")\
    .join(
        spark.table("page_linktarget"),
        F.col("pl_target_id") == F.col("link_target_id"),
        how='inner'
    )\
    .select(
        F.col("pl_from").alias("pl_from_source_id"),  # Source page ID
        F.col("page_id").alias("page_id_target_id")    # Target page ID from Page table
    )\
    .filter(F.col("page_namespace") == '0') \
    .createOrReplaceTempView("page_linktarget_with_pagelink") 

spark.table("page_linktarget_with_pagelink").show(10)


# In[23]:


# Step 4: Join page_linktarget_with_pagelink with lt_page to resolve both source and target IDs, creating resolved_links view
spark.table("page_linktarget_with_pagelink").alias("pl") \
    .join(
        spark.table("lt_page").alias("r1"),
        F.col("pl.pl_from_source_id") == F.col("r1.redirect_source_id"),
        how='left'
    ) \
    .join(
        spark.table("lt_page").alias("r2"),
        F.col("pl.page_id_target_id") == F.col("r2.redirect_source_id"),
        how='left'
    ) \
    .select(
        F.coalesce(F.col("r1.redirect_target_id"), F.col("pl.pl_from_source_id")).alias("resolved_source_id"),  # Final resolved source (A)
        F.coalesce(F.col("r2.redirect_target_id"), F.col("pl.page_id_target_id")).alias("resolved_target_id")   # Final resolved target (C)
    ) \
    .filter(
        F.col("resolved_source_id") != F.col("resolved_target_id")  # Exclude pairs where source == target
    ) \
    .distinct() \
    .createOrReplaceTempView("resolved_links")

# Show the first 10 rows of the resolved_links view
spark.table("resolved_links").show(10)


# In[ ]:


# Step 5: Find mutual links where A links to C and C links back to A, creating mutual_links view
spark.table("resolved_links").alias("df1") \
    .join(
        spark.table("resolved_links").alias("df2"),
        (F.col("df1.resolved_source_id") == F.col("df2.resolved_target_id")) & 
        (F.col("df1.resolved_target_id") == F.col("df2.resolved_source_id")),
        how="inner"
    ) \
    .select(
        F.col("df1.resolved_source_id").alias("PageA"),
        F.col("df1.resolved_target_id").alias("PageB")
    ) \
    .distinct() \
    .createOrReplaceTempView("mutual_links")

# Step 6: Filter for unique pairs, ensuring that each pair (A, C) only appears once, creating unique_mutual_links view
spark.table("mutual_links") \
    .filter(
        F.col("PageA") < F.col("PageB")  # Ensure that PageA < PageB for uniqueness
    ) \
    .distinct() \
    .createOrReplaceTempView("unique_mutual_links")

# Show the final unique mutual links
spark.table("unique_mutual_links").show(20)


# In[ ]:


import os 
spark.table("unique_mutual_links").write.mode("OVERWRITE").parquet(os.environ['PAGE_PAIRS_OUTPUT']) 


# In[ ]:


#get_ipython().system('jupyter nbconvert --to script largescale.ipynb')
