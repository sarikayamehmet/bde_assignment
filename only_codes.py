# Databricks notebook source
website_dataset_filepath = "dbfs:/FileStore/tables/datasets/website_dataset.csv"
facebook_dataset_filepath = "dbfs:/FileStore/tables/datasets/facebook_dataset.csv"
google_dataset_filepath = "dbfs:/FileStore/tables/datasets/google_dataset.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # website data

# COMMAND ----------

website_data = spark.read.option("delimiter", ";").option("encoding","UTF-8").option("multiline",True).csv(website_dataset_filepath, header='true', inferSchema='true')
website_data.count()

# COMMAND ----------

website_data.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
# calculate missing percentage for columns
website_missing = website_data.select([F.round(F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))/F.count(F.lit(1)),5).alias(c) for c in website_data.columns])
website_missing.count()
display(website_missing)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 55% of "Legal Name" is missing so we need to use "Site Name" which has only 4% null value. <br>
# MAGIC Also when "Site Name" is null then we can use "Root Domain" as company name for keeping all information. <br>
# MAGIC We keep only important columns and discarding [domain_suffix, language, tld]

# COMMAND ----------

from pyspark.sql import functions as F
# calculate distinct percentage for columns
website_distinct = website_data.select([F.round(F.countDistinct(c)/(F.count(F.lit(1))-F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))),5).alias(c) for c in website_data.columns])
website_distinct.count()
display(website_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC All values for "Root Domain" is unique. 98% of "Site Name" is also unique. <br>
# MAGIC Unique percentage of "Legal Name" is 95%, that means 5% of company names have more than 1 website. <br>
# MAGIC We can concat domain names if we group data by "Legal Name". However, For getting more information from this data we should use "Root Domain" and "Site Name".

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # facebook data

# COMMAND ----------

facebook_data = spark.read.option("delimiter", ",").option("encoding","UTF-8").option("multiline",True).csv(facebook_dataset_filepath, header='true', inferSchema='true')
facebook_data.count()

# COMMAND ----------

facebook_data.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
# calculate missing percentage for columns
facebook_missing = facebook_data.select([F.round(F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))/F.count(F.lit(1)),5).alias(c) for c in facebook_data.columns])
facebook_missing.count()
display(facebook_missing)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In facebook_data, "Domain" column has 0% null values, and "Name" column has roughly 0% null values. <br>
# MAGIC "Categories" column includes rich information but 23% of that column's values are null.

# COMMAND ----------

from pyspark.sql import functions as F
# calculate distinct percentage for columns
facebook_distinct = facebook_data.select([F.round(F.countDistinct(c)/(F.count(F.lit(1))-F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))),5).alias(c) for c in facebook_data.columns])
facebook_distinct.count()
display(facebook_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In facebook data, all domain names are unique, also their adress, email, name, and phone informations have unique values more than 99%. <br> 
# MAGIC We will keep important columns (domain, name, categories, country_name, region_name, city, address, zip_code, email, phone, page_type) for merging with website data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # google data

# COMMAND ----------

google_data = spark.read.option("delimiter", ",").option("encoding","UTF-8").option("multiline",True).csv(google_dataset_filepath, header='true', inferSchema='true')
google_data.count()

# COMMAND ----------

google_data.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
# calculate missing percentage for columns
google_missing = google_data.select([F.round(F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))/F.count(F.lit(1)),5).alias(c) for c in google_data.columns])
google_missing.count()
display(google_missing)

# COMMAND ----------

# MAGIC %md
# MAGIC In google data, domain and name columns have less missing data. <br>
# MAGIC However, "domain" column includes aggregator domains like facebook, instagram.

# COMMAND ----------

from pyspark.sql import functions as F
# calculate distinct percentage for columns
google_distinct = google_data.select([F.round(F.countDistinct(c)/(F.count(F.lit(1))-F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))),5).alias(c) for c in google_data.columns])
google_distinct.count()
display(google_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC "domain" column has only 20% percent unique values because of aggregator domains. <br>
# MAGIC Therefore, we need also to use "phone" and/or "name" column for joining with website and facebook datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Some of company has more than one store, so there is multiple address and phone for same domain. <br>
# MAGIC We can group it according to domain or we can think that every stores are different business.

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preparation

# COMMAND ----------

website_columns_old = ["root_domain", "legal_name", "site_name", "s_category", "main_city", "main_country", "main_region", "phone"]
website_columns = ["web_domain", "web_name", "web_site_name", "web_category", "web_city", "web_country", "web_region", "web_phone"]
web_data = website_data.select(*website_columns_old).toDF(*website_columns)
web_data.count()

# COMMAND ----------

fb_columns_old = ["domain", "name", "categories", "country_name", "region_name", "city", "address", "zip_code", "email", "phone", "page_type"]
fb_columns = ["fb_domain", "fb_name", "fb_categories", "fb_country", "fb_region", "fb_city", "fb_address", "fb_zip_code", "fb_email", "fb_phone", "fb_page_type"]
fb_data = facebook_data.select(*fb_columns_old).toDF(*fb_columns)
fb_data.count()

# COMMAND ----------

gg_columns_old = ["domain", "name", "category", "country_name", "region_name", "city", "address", "zip_code", "phone"]
gg_columns = ["gg_domain", "gg_name", "gg_category", "gg_country", "gg_region", "gg_city", "gg_address", "gg_zip_code", "gg_phone"]
gg_data = google_data.select(*gg_columns_old).toDF(*gg_columns)
gg_data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Join data

# COMMAND ----------

web_fb_inner = web_data.join(fb_data, on = [web_data.web_domain == fb_data.fb_domain], how = "inner")
web_fb_left = web_data.join(fb_data, on = [web_data.web_domain == fb_data.fb_domain], how = "left")
print("join ratio:",web_fb_inner.count()/web_fb_left.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Join ratio is more than 99%, so domain selection is good for these two datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Google data does not have proper domain name so we will use company names for merging google data with others. <br>
# MAGIC First we will look for name columns in "web_fb_data"

# COMMAND ----------

from pyspark.sql import functions as F
# calculate missing percentage for columns
web_fb_data = web_fb_left
web_fb_name_columns = ["web_name","web_site_name","fb_name","web_phone","fb_phone"]
web_fb_missing = web_fb_data.select([F.round(F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))/F.count(F.lit(1)),5).alias(c) for c in web_fb_name_columns])
web_fb_missing.count()
display(web_fb_missing)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC fb_name columns has very small amount of null values but web_name has 55% null values. <br>
# MAGIC Therefore, we will use fb_name for matching with google. <br>
# MAGIC I will use MinHashLSHModel for finding similarity between google_data names and web_fb_data names. <br>
# MAGIC Making cross join is not effective and cause a lot cost and increase data size. <br>
# MAGIC Therefore, I need to join two dataset with a common column to decrase data size.<br>
# MAGIC So i will join using phone then if there is no match i will use cross join to calculate distance metrics

# COMMAND ----------

from pyspark.sql import functions as F
# FB phone has well format, if it is available use it. I try both version and fb_phone has bigger coverage rate
web_fb_data = web_fb_data.withColumn('fb_phone', F.regexp_replace(F.col("fb_phone"), '[^0-9]+', ''))
web_fb_data = web_fb_data.withColumn('web_phone', F.regexp_replace(F.col("web_phone"), '[^0-9]+', ''))
web_fb_data = web_fb_data.withColumn('new_phone', F.when(F.isnan("fb_phone") | F.col("fb_phone").isNull(), F.col("web_phone")).otherwise(F.col("fb_phone")))

gg_data = gg_data.withColumn('gg_phone', F.regexp_replace(F.col("gg_phone"), '[^0-9]+', ''))

# COMMAND ----------

web_fb_gg_inner = web_fb_data.join(gg_data, on = [web_fb_data.new_phone == gg_data.gg_phone], how = "inner")
web_fb_gg_left = web_fb_data.join(gg_data, on = [web_fb_data.new_phone == gg_data.gg_phone], how = "left")
print("join ratio:",web_fb_gg_inner.count()/web_fb_gg_left.count())
print("domain coverage ratio:",web_fb_gg_inner.select("web_domain").distinct().count()/web_fb_data.select("web_domain").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now we have join all three dataset. <br>
# MAGIC Web - Facebook join ratio is 99%. <br>
# MAGIC (Web-Facebook) and Google join ratio is 70%. <br>
# MAGIC For other 30%, i will use levensthein distance.

# COMMAND ----------

remaining_web_fb = web_fb_gg_left.filter(F.isnan("gg_phone") | F.col("gg_phone").isNull())
remaining_web_fb = remaining_web_fb.select(*web_fb_data.columns)
remaining_web_fb.count()

# COMMAND ----------

remaining_web_fb = remaining_web_fb.withColumn('new_name', 
  F.when((F.isnan("fb_name") | F.col("fb_name").isNull()) & (F.isnan("web_name") | F.col("web_name").isNull()), F.col("web_site_name"))
  .when(F.isnan("fb_name") | F.col("fb_name").isNull(), F.col("web_name"))
  .otherwise(F.col("fb_name"))
)

# COMMAND ----------

gg_web_fb_left = gg_data.join(web_fb_data, on = [gg_data.gg_phone == web_fb_data.new_phone], how = "left")
remaining_gg = gg_web_fb_left.filter(F.isnan("new_phone") | F.col("new_phone").isNull())
remaining_gg = remaining_gg.select(*gg_data.columns)
remaining_gg.count()

# COMMAND ----------

# convert to lower
remaining_web_fb = remaining_web_fb.withColumn('new_name', F.lower(F.col("new_name")))
remaining_gg = remaining_gg.withColumn('gg_name', F.lower(F.col("gg_name")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Pipeline and MinHashLSHModel for finding similar names

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.sql import types as T
from pyspark.ml.feature import Tokenizer, NGram, HashingTF, MinHashLSH

df = remaining_web_fb
df = df.withColumn("company_name", F.col('new_name'))
df = df.filter(F.length("company_name")>0)

pipeline = Pipeline(stages=[
        Tokenizer(inputCol="company_name", outputCol="tokens"),
        NGram(n=2, inputCol="tokens", outputCol="ngrams"),
        HashingTF(inputCol="ngrams", outputCol="vectors")
    ])

lsh = MinHashLSH(inputCol="vectors", outputCol="lsh")

pipeline_model = pipeline.fit(df)
df = pipeline_model.transform(df)
# i add filter becasue empty vectors cause an error
df = df.filter(~F.col('vectors').cast(T.StringType()).contains('[]'))
model = lsh.fit(df)
df = model.transform(df)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I convert words to tokens then using ngram I take combinations of words. <br>
# MAGIC HashingTF is converting words to numbers which is called as vector-space model. <br>
# MAGIC Finally we use MinHashLSH with approxSimilarityJoin to join two datasets to approximately find all pairs of rows whose distance are smaller than the threshold.

# COMMAND ----------

df2 = remaining_gg
df2 = df2.withColumn("company_name", F.col('gg_name'))
df2 = df2.filter(F.length("company_name")>0)

df2 = pipeline_model.transform(df2)
df2 = df2.filter(~F.col('vectors').cast(T.StringType()).contains('[]'))
df2 = model.transform(df2)
display(df2)

# COMMAND ----------

matched_df = model.approxSimilarityJoin(df, df2, 0.5, "confidence")
print(matched_df.count())
display(matched_df)

# COMMAND ----------

display(matched_df.select("datasetA.new_name","datasetB.gg_name"))

# COMMAND ----------

colnamesA = ["datasetA."+c for c in web_fb_data.columns]
colnamesB = ["datasetB."+c for c in gg_data.columns]
remaining_web_fb_gg = matched_df.select(*[colnamesA+colnamesB])
remaining_web_fb_gg.count()

# COMMAND ----------

print("domain coverage ratio for remaining:",remaining_web_fb_gg.select("web_domain").distinct().count()/remaining_web_fb.select("web_domain").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We use 0.5 threshold for string matching, so we cover 41% of remaining data. <br>
# MAGIC Now we increase (Web-Facebook) and Google join ratio from 70% to 82%. <br>

# COMMAND ----------

# Merge all of it

web_fb_gg_full = web_fb_gg_inner.union(remaining_web_fb_gg)
web_fb_gg_full.count()

# COMMAND ----------

print("final coverage ratio:",web_fb_gg_full.select("web_domain").distinct().count()/web_fb_data.select("web_domain").distinct().count())

# COMMAND ----------

write_file_path = "dbfs:/FileStore/datasets/merged.csv"
web_fb_gg_full.repartition(1).write.format("com.databricks.spark.csv").mode('overwrite').option("header", "true").save(write_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Final thoughts
# MAGIC <br>
# MAGIC I dont merge categories, address and others info because all of them can be used in a different columns for ml models. <br>

# COMMAND ----------


