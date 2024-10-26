#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas
import datetime
from pyspark.storagelevel import StorageLevel
from pyspark.sql import ( functions as F,
      types as T,
      SparkSession,
)


# In[2]:


spark =(
    SparkSession.builder
    #.remote('sc://localhost:15002')
    .appName("Wikipedia Mutual Links with Redirects")
    .getOrCreate()
)


# In[3]:


def parquet(prefix):
    files = spark.read.parquet(f"s3://bsu-c535-fall2024-commons/arjun-workspace/{prefix}/")
    files.save(prefix)


# In[4]:


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


# In[5]:


# for interactive EMR through Spark Connect
from pyspark.sql.connect.dataframe import DataFrame
DataFrame.save = save


# In[6]:


parquet("linktarget")


# In[7]:


parquet("page")


# In[8]:


parquet("redirect")


# In[9]:


parquet("pagelinks")


# In[10]:


# Load data into DataFrames using spark.table
page_df = spark.table("page")  # Page table
linktarget_df = spark.table("linktarget")  # LinkTarget table
pagelink_df = spark.table("pagelinks")  # PageLink table
redirect_df = spark.table("redirect")  # Redirect table
page_df.show()
linktarget_df.show()
pagelink_df.show()
redirect_df.show()


# In[13]:


# Step 1: Join Page and LinkTarget to resolve link targets to actual pages
page_linktarget_df = linktarget_df.join(
    page_df,
    (linktarget_df.lt_title == page_df.page_title) & (linktarget_df.lt_namespace == page_df.page_namespace),
    how='inner'
).select(
    page_df.page_id.alias("page_id"),
    page_df.page_namespace.alias("page_namespace"),
    page_df.page_title.alias("page_title"),
    linktarget_df.lt_id.alias("link_target_id")
) 


# In[14]:


# Step 2: Join page_linktarget_df with PageLink to get source_id and resolved target_id
page_linktarget_with_pagelink_df = pagelink_df.join(
    page_linktarget_df,
    (pagelink_df.pl_target_id == page_linktarget_df.link_target_id),
    how='inner'
).select(
    pagelink_df.pl_from.alias("pl_from_source_id"),      # Source page ID
    page_linktarget_df.page_id.alias("page_id_target_id") #target page ID from Page table 
).filter(page_linktarget_df["page_namespace"] == '0')
page_linktarget_with_pagelink_df.show()


# In[15]:


# Step 3: Join to match page_title and rd_title, and include namespace
redirects_with_targets = redirect_df.join(
    page_linktarget_df, 
    (page_linktarget_df["page_title"] == redirect_df["rd_title"]) & (page_linktarget_df["page_namespace"] == redirect_df["rd_namespace"]),
).select(
    redirect_df["rd_from"].alias("redirect_source_id"),  
    page_linktarget_df["page_id"].alias("redirect_target_id")  
)

# Remove duplicates by taking distinct pairs of redirect_source_id and redirect_target_id
redirects_with_targets = redirects_with_targets.distinct()

# Show the result with IDs
redirects_with_targets.show()


# In[16]:


# Step 4: Join `page_linktarget_with_pagelink_df` with `redirects_with_targets` to resolve both source and target IDs
resolved_links_df = page_linktarget_with_pagelink_df.alias("pl").join(
    redirects_with_targets.alias("r1"),
    F.col("pl.pl_from_source_id") == F.col("r1.redirect_source_id"),
    how='left'
).join(
    redirects_with_targets.alias("r2"),
    F.col("pl.page_id_target_id") == F.col("r2.redirect_source_id"),
    how='left'
).select(
    F.coalesce(F.col("r1.redirect_target_id"), F.col("pl.pl_from_source_id")).alias("resolved_source_id"),  # Final resolved source (A)
    F.coalesce(F.col("r2.redirect_target_id"), F.col("pl.page_id_target_id")).alias("resolved_target_id")   # Final resolved target (C)
).filter(
    F.col("resolved_source_id") != F.col("resolved_target_id")  # Exclude pairs where source == target
).distinct()  # Ensure unique pairs

# Show the final result
resolved_links_df.show(10)


# In[21]:


# Step 5: Find mutual links where A links to C and C links back to A
mutual_links_df = resolved_links_df.alias("df1").join(
    resolved_links_df.alias("df2"),
    (F.col("df1.resolved_source_id") == F.col("df2.resolved_target_id")) & 
    (F.col("df1.resolved_target_id") == F.col("df2.resolved_source_id")),
    how="inner"
).select(
    F.col("df1.resolved_source_id").alias("PageA"),
    F.col("df1.resolved_target_id").alias("PageB")
).distinct()  # Ensure unique pairs

# Step 6: Filter for unique pairs, ensuring that each pair (A, C) only appears once
unique_mutual_links_df = mutual_links_df.filter(
    F.col("PageA") < F.col("PageB")  # Ensure that source_id < target_id for uniqueness
).distinct()  # Ensure unique pairs

# Show the final unique mutual links
unique_mutual_links_df.show(10)


# In[ ]:


import os 
spark.table("unique_mutual_links_df").write.mode("OVERWRITE").parquet(os.environ['PAGE_PAIRS_OUTPUT']) 


# In[ ]:


#get_ipython().system('jupyter nbconvert --to script LSA_Project.ipynb')


# In[ ]:




