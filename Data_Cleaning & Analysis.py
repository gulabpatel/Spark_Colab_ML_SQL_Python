# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/LoanStats_2018.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "LoanStats"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `LoanStats`

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

temp = "loanstats"
df.createOrReplaceTempView(temp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanstats

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from loanstats

# COMMAND ----------

df.describe().show()

# COMMAND ----------

df_sel = df.select("term","home_ownership","grade","purpose","int_rate","addr_state","loan_status","application_type","loan_amnt","emp_length","annual_inc","dti","delinq_2yrs","revol_util","total_acc","num_tl_90g_dpd_24m","dti_joint")

# COMMAND ----------

df_sel.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Still the dataframe is not much clear let's select more less columns

# COMMAND ----------

df_sel.describe("term","emp_length","annual_inc","dti","delinq_2yrs","revol_util","total_acc").show()

# COMMAND ----------

# DBTITLE 1,Cache the dataset means loading into the executer memory so that it can process even more fast
df_sel.cache()

# COMMAND ----------

df_sel.describe("loan_amnt","emp_length","annual_inc","dti","delinq_2yrs","revol_util","total_acc").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct emp_length from loanstats limit 50

# COMMAND ----------

# DBTITLE 1,There are string variable in the columns, replace/extract them
from pyspark.sql.functions import regexp_replace, regexp_extract
from pyspark.sql.functions import col

regex_string = 'years|year|\\+|\\<'
df_sel.select(regexp_replace(col("emp_length"), regex_string,"").alias("emplength_cleaned"), col("emp_length")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ######The other way of cleaning is extracting only decimal character, use regex_extract

# COMMAND ----------

regex_string = "\\d+"
df_sel.select(regexp_extract(col("emp_length"), regex_string, 0).alias("emplength_cleaned"), col("emp_length")).show(10)

# COMMAND ----------

df_sel.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ######We still see the string variables this is because we didn't assign the filtered df to new dataframe.
# MAGIC 
# MAGIC ######Assign to a dataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC select term from loanstats

# COMMAND ----------

df_sel = df_sel.withColumn("term_cleaned", regexp_replace(col("term"),"months","")).withColumn("emplen_cleaned", regexp_extract(col("emp_length"),"\\d+", 0))

# COMMAND ----------

df_sel.select('term','term_cleaned','emp_length','emplen_cleaned').show(15)

# COMMAND ----------

df_sel.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ######If you see the last two column they are string although it is numeric so manually need to convert it

# COMMAND ----------

table_name = "loanstats_sel"
df_sel.createOrReplaceTempView(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loanstats_sel

# COMMAND ----------

df_sel.stat.cov('annual_inc',"loan_amnt")

# COMMAND ----------

df_sel.stat.corr('annual_inc',"loan_amnt")

# COMMAND ----------

# MAGIC %md
# MAGIC ######Correalation can also be calculated using sql, if it is closer to -1 means negatively related and if closer to +1 means positively related and if closer to 0, we can say very weak relation

# COMMAND ----------

# MAGIC %sql
# MAGIC select corr(loan_amnt, term_cleaned) as corr from loanstats_sel

# COMMAND ----------

# DBTITLE 1,Frequency count
df_sel.stat.crosstab('loan_status','grade').show()

# COMMAND ----------

# DBTITLE 1,We can use "freqItems()" too for frequency count
freq = df_sel.stat.freqItems(['purpose','grade'],0.3)

# COMMAND ----------

freq.show()

# COMMAND ----------

freq.collect()

# COMMAND ----------

df_sel.groupby('purpose').count().show()

# COMMAND ----------

df_sel.groupby('purpose').count().orderBy(col('count').desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### If you see here the major purpose is debt_consolidation and debit_card what we saw in the freqItem() function.
# MAGIC so you have several ways to look at it, you can use crosstab(), freqItems(), groupby(),sql code.

# COMMAND ----------

from pyspark.sql.functions import count, mean, stddev_pop, min, max, avg

# COMMAND ----------

# DBTITLE 1,Quantiles
quantileProbs = [0.25, 0.5, 0.75, 0.9]
relError = 0.05
df_sel.stat.approxQuantile("loan_amnt", quantileProbs, relError)

# COMMAND ----------

quantileProbs = [0.25, 0.5, 0.75, 0.9]
relError = 0.0
df_sel.stat.approxQuantile("loan_amnt", quantileProbs, relError)

# COMMAND ----------

quantileProbs = [0.25, 0.5, 0.75, 0.9]
relError = 0.5
df_sel.stat.approxQuantile("loan_amnt", quantileProbs, relError)

# COMMAND ----------

# MAGIC %md
# MAGIC #####If you see the runtime of above three codes, that depends on the allowed relError. If you are okay with the relError it will give you fast result. You can tune it, see the accuracy and go accordingly.

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Handle missing values

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Almost every column has 4 null values

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_status, count(*) from loanstats group by loan_status order by 2 desc

# COMMAND ----------

df_sel = df_sel.na.drop("all", subset=["loan_status"])

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ######see all the four null values are gone but still there are someother column where there is null, let's see further how to remove them out. 

# COMMAND ----------

df_sel.count()

# COMMAND ----------

df_sel.describe("dti","revol_util").show()

# COMMAND ----------

# MAGIC %md
# MAGIC There are some null vaues in revol_util column, now let's use sql query to replace the string or null values

# COMMAND ----------

# MAGIC %sql
# MAGIC select ceil(REGEXP_REPLACE(revol_util,"\%","")), count(*) from loanstats_sel group by ceil(REGEXP_REPLACE(revol_util, "\%", ""))

# COMMAND ----------

df_sel.describe("dti","revol_util").show()

# COMMAND ----------

# MAGIC %md
# MAGIC we are getting absurd results here as it is giving max value in revol_util column is 99.90 but this is not the case, and this is happening because there are string values in revol_util column

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from loanstats_sel where revol_util is null

# COMMAND ----------

df_sel = df_sel.withColumn("revolutil_cleaned", regexp_extract(col("revol_util"), "\\d+", 0))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(revol_util) from loanstats_sel where revol_util is null

# COMMAND ----------

df_sel.describe('revol_util', 'revolutil_cleaned').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC still it is not converted into double or integer, there are nullin that column let's first fix them up

# COMMAND ----------

def fill_avg(df, colname):
  return df.select(colname).agg(avg(colname))

# COMMAND ----------

rev_avg = fill_avg(df_sel, 'revolutil_cleaned')

# COMMAND ----------

from pyspark.sql.functions import lit

rev_avg = fill_avg(df_sel, 'revolutil_cleaned').first()[0]
df_sel = df_sel.withColumn('rev_avg', lit(rev_avg))

# COMMAND ----------

from pyspark.sql.functions import coalesce
df_sel = df_sel.withColumn('revolutil_cleaned', coalesce(col('revolutil_cleaned'), col('rev_avg')))

# COMMAND ----------

df_sel.describe('revol_util', 'revolutil_cleaned').show()

# COMMAND ----------

# MAGIC %md
# MAGIC still it  is showing maximum 99 because we have string values

# COMMAND ----------

df_sel = df_sel.withColumn("revolutil_cleaned", df_sel["revolutil_cleaned"].cast("double"))

# COMMAND ----------

df_sel.describe('revol_util', 'revolutil_cleaned').show()

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC There are still some columns with null values, let's analyse them

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from loanstats_sel where dti is null

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select application_type, dti, dti_joint from loanstats where dti is null

# COMMAND ----------

df_sel = df_sel.withColumn("dti_cleaned", coalesce(col("dti"), col("dti_joint")))

# COMMAND ----------

df_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_sel.columns]).show()

# COMMAND ----------

df_sel.groupby('loan_status').count().show()

# COMMAND ----------

df_sel.where(df_sel.loan_status.isin(["Late (31-120 days)", "Charged off", "In Grace Period", "Late (16-30 days)"])).show()

# COMMAND ----------

df_sel = df_sel.withColumn("bad_loan", when(df_sel.loan_status.isin(["Late (31-120 days)", "Charged off", "In Grace Period", "Late (16-30 days)"]), 'Yes').otherwise('No'))

# COMMAND ----------

df_sel.groupby('bad_loan').count().show()

# COMMAND ----------

df_sel.filter(df_sel.bad_loan == 'Yes').show()

# COMMAND ----------

df_sel.printSchema()

# COMMAND ----------

df_sel_final = df_sel.drop('revol_util','dti','dti_joint')

# COMMAND ----------

df_sel_final.printSchema()

# COMMAND ----------

df_sel.crosstab('bad_loan', 'grade').show()

# COMMAND ----------

df_sel.describe('dti_cleaned').show()

# COMMAND ----------

df_sel.filter(df_sel.dti_cleaned > 100).show()

# COMMAND ----------

permanent_table_name = "lc_loan_data"

df_sel.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lc_loan_data

# COMMAND ----------


