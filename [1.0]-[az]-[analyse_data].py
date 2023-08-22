# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row, DataFrame, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from time import gmtime, strftime
import numpy as np
import datetime

# COMMAND ----------

MOUNT_NAME = "vcl"

# COMMAND ----------

# MAGIC %md ### Load Data

# COMMAND ----------

# df = spark.read.csv("/mnt/%s/O2 2023 Q2/data/raw/*" % MOUNT_NAME,  sep =',', header = True)
# dfQual = spark.read.csv("/mnt/%s/local/Data/Original/2023/6-26/94i2ad/vcl_advances_finance_202301.csv.gz" % MOUNT_NAME,  sep =',', header = True)
column_headers = ["msidn", "advanceId", "eventDate", "amount", "transactionId", "otherParty", "operationType", "profileEligibility"]
df2 = spark.read.csv("/mnt/%s/O2 2023 Q2/data/raw/recharges_only/VCL_APF_Apr-Aug2023_v2.csv.gz" % MOUNT_NAME,  sep =',', header = True, inferSchema = True)
df2 = df2.toDF(*column_headers)
# df2.show()
# s3://vcl-av/O2 2023 Q2/data/raw/recharges_only/VCL_APF_Apr_Aug2023.csv.gz

# COMMAND ----------

counts = df2.count()

df = df2.withColumn('Dates', F.date_format(col("eventDate"), "yyyy-MM-dd").cast(TimestampType()))
df = df.groupby('Dates').agg(count('Dates'))
display(df)

# COMMAND ----------

# MAGIC %md ### Basic Data Processing

# COMMAND ----------

 df = df.drop(
#  'msisdn',
#  'advanceId',
#  'eventDate',
#  'amount',
#  'transactionId',
 'createdOn', 
#  'otherParty',
#  'operationType',
 'loanAmount',
 'serviceFeeAmount',
 'INLoanBalance',
 'INServiceFeeBalance',
 'fileName',
#  'profileEligibility',
#  'rawRecord'
 )

# COMMAND ----------

# change column types to correct data type
df = df.withColumn('advanceId', col('advanceId').cast(IntegerType()))\
       .withColumn('eventDate', col('eventDate').cast(TimestampType()))\
       .withColumn('amount', col('amount').cast(DoubleType()))\
       .withColumn('operationType', col('operationType').cast(IntegerType()))\
       .withColumn('profileEligibility', col('profileEligibility').cast(IntegerType()))


# Update data types for advances from finance
dfQual = dfQual.withColumn('advancedate', col('advancedate').cast(TimestampType()))\
       .withColumn('advancestatus', col('advancestatus').cast(IntegerType()))\
       .withColumn('qualifiedamount', col('qualifiedamount').cast(DoubleType()))\
       .withColumn('chosenamount', col('amount').cast(IntegerType()))\
       .withColumn('duedate', col('duedate').cast(TimestampType()))\
       .withColumn('repaydate', col('repaydate').cast(TimestampType())) 

dfQual.dtypes

# COMMAND ----------

# extract year month data
df = df.withColumn('year', F.date_format(col("eventDate"), 'yyyy'))\
       .withColumn('month', F.date_format(col("eventDate"), 'MM'))

# COMMAND ----------

# string processing
df = df.withColumn('transaction_text', F.split(df['rawRecord'], ',').getItem(3))\
       .withColumn('entity',           F.split(df['rawRecord'], ',').getItem(6))\
       .withColumn('entity_name',      F.split(df['rawRecord'], ',').getItem(10))

df = df.withColumn('entity',           F.split(df['entity'],'-').getItem(1))\
       .withColumn('entity_name',      F.split(df['entity_name'],'-').getItem(1))

df = df.drop('rawRecord')

# remove fintect move over
df = df.filter(col('eventDate')<'2023-04-23')

# COMMAND ----------


# filterdown = df.filter((df.eventDate >= '2022-12-01') & (df.eventDate < '2023-02-01'))
# filterdown = filterdown.drop('advanceId', 'transactionId', 'entity', 'entity_name')
# display(filterdown)

# COMMAND ----------

# df = df.filter(df.msisdn == '26659522896')
# dfQual = dfQual.filter(dfQual.msisdn == '26659522896')
# display(df)
# display(dfQual)

# COMMAND ----------

# MAGIC %md ### Analyses

# COMMAND ----------

# df = df.withColumn('Dates', F.date_format(col("eventDate"), "yyyy-MM-dd").cast(TimestampType()))

# COMMAND ----------

# MAGIC %md #### Loans

# COMMAND ----------

# find the qualified amount in M0 
dfQual = dfQual.filter((dfQual.advancedate >= '2023-01-01') & (dfQual.advancedate < '2023-02-01'))
# find the MSISDNs and amounts in M0
M0_Qualified = dfQual.select('msisdn', 'qualifiedamount')
# display(M0_Qualified)

# find the buckets each MSISDN falls into
M0_Qualified = M0_Qualified.withColumn('M0_qualified_buckets', F.when(col('qualifiedamount') <= 50, '50')\
                                                                                .when((col('qualifiedamount') > 50) & (col('qualifiedamount') <= 100), '50-100')\
                                                                                    .when((col('qualifiedamount') > 100) & (col('qualifiedamount') <= 150), '100-150')\
                                                                                      .when((col('qualifiedamount') > 150) & (col('qualifiedamount') <= 200), '150-200')\
                                                                                        .when(col('qualifiedamount') > 200, '> 200').otherwise(None))
# display(M0_Qualified)

# COMMAND ----------

###### M0 Recharges per Qualified Bucket #########
user = M0_Qualified.select('msisdn')
df = df.filter((df.eventDate >= '2023-01-01') & (df.eventDate < '2023-02-01')) # what is the purpose of this line? It does not fit in
users = user.join(df, on="msisdn", how="inner")
users = users.dropDuplicates()



users = users.where((users.operationType == 0)\
   & (users.otherParty == 'Customer')\
      & (users.transaction_text != 'B2C Reversal'))


users = users.groupby('msisdn').agg(sum('amount')) # potentially summing all recharges ever for a user....

# COMMAND ----------

### M + 1 Recharges####

###### M0 Recharges per Qualified Bucket #########
user = M0_Qualified.select('msisdn')
df = df.filter((df.eventDate >= '2023-02-01') & (df.eventDate < '2023-03-01')) # what is the purpose of this line? It does not fit in
users = user.join(df, on="msisdn", how="inner")
users = users.dropDuplicates()



users = users.where((users.operationType == 0)\
   & (users.otherParty == 'Customer')\
      & (users.transaction_text != 'B2C Reversal'))


users = users.groupby('msisdn').agg(sum('amount')) # potentially summing all recharges ever for a user....

# COMMAND ----------

### M + 2 Recharges####

###### M0 Recharges per Qualified Bucket #########
user = M0_Qualified.select('msisdn')
df = df.filter((df.eventDate >= '2023-03-01') & (df.eventDate < '2023-04-01')) # what is the purpose of this line? It does not fit in
users = user.join(df, on="msisdn", how="inner")
users = users.dropDuplicates()



users = users.where((users.operationType == 0)\
   & (users.otherParty == 'Customer')\
      & (users.transaction_text != 'B2C Reversal'))


users = users.groupby('msisdn').agg(sum('amount')) # potentially summing all recharges ever for a user....

# COMMAND ----------

# Sum Recharges for all msisdns in bucket above m-1
user = M0_Qualified.select('msisdn')

#################################################################
###### I disagree with belows join ################################
# I would have used the advance ID and MSISDN to link the qualified amount to the advance taken
# I would then df.join(users, on =['msisdn','advanceID'], how = 'left')
###########################################################################
df = df.filter((df.eventDate >= '2022-12-01') & (df.eventDate < '2023-01-01')) # what is the purpose of this line? It does not fit in
users = user.join(df, on="msisdn", how="inner")
users = users.dropDuplicates()

###########################################################################
###########################################################################



# filter operating types from df to get only incoming money
users = users.where((users.operationType == 0)\
   & (users.otherParty == 'Customer')\
      & (users.transaction_text != 'B2C Reversal'))

# find the total the total recharges for? Not sure how this links to a distinct m-1 period?????
users = users.groupby('msisdn').agg(sum('amount')) # potentially summing all recharges ever for a user....
display(users)
# Qual = users.join(user, on='msisdn', how="inner")
# Qual = Qual.dropDuplicates() #didn#'t  drop the duplicates before summing 
# display(Qual)

# COMMAND ----------

###M0 Bucket###
user = M0_Qualified.select('msisdn', 'M0_qualified_buckets')
M_0QualBuckets = users.join(user, on='msisdn', how='left')
M_0QualBuckets = M_0QualBuckets.dropDuplicates()
display(M_0QualBuckets)

# COMMAND ----------

### M + 1 Bucket###
user = M0_Qualified.select('msisdn', 'M0_qualified_buckets')
M_1QualBuckets = users.join(user, on='msisdn', how='left')
M_1QualBuckets = M_1QualBuckets.dropDuplicates()
display(M_1QualBuckets)

# COMMAND ----------

### M + 2 Bucket###
user = M0_Qualified.select('msisdn', 'M0_qualified_buckets')
M_0QualBuckets = users.join(user, on='msisdn', how='left')
M_0QualBuckets = M_0QualBuckets.dropDuplicates()
display(M_0QualBuckets)

# COMMAND ----------

user = M0_Qualified.select('msisdn', 'M0_qualified_buckets')
M_1QualBuckets = users.join(user, on='msisdn', how='left')
M_1QualBuckets = M_1QualBuckets.dropDuplicates()
display(M_1QualBuckets)

# COMMAND ----------

display(dfQual.filter(dfQual.msisdn == '26659522896'))

# COMMAND ----------

# Identifying M0 msisdn's with qualified amounts
dfQual = dfQual.select('msisdn', 'qualifiedamount').filter((dfQual.advancedate >= '2023-01-01') & (dfQual.advancedate < '2023-02-01'))
M0_Qualified = M0_Qualified.select('qualifiedamount')
M0Qual_msisdn = M0_Qualified.join(dfQual, on="qualifiedamount", how="inner")
display(M0Qual_msisdn)

# COMMAND ----------

# Filtering msisnd in bins of 50 increments
# q50 = col('qualifiedamount') =< 50
# q100 = (col('qualifiedamount')) > q50 & (col('qualifiedamount'))

# COMMAND ----------

# def sum_valaues(loanbucket)
# wPart = Window.partitionBy('msisdn', 'year', 'month')

# df = df.filter((df.Dates >= '2023-01-01') & (df.Dates < '2023-02-01'))
# dfQual = dfQual.filter((dfQual.advancedate >= '2023-01-01') & (dfQual.advancedate < '2023-02-01'))
# joindf = dfQual.join(df, on="msisdn", how="inner")
# # display(joindf)
# joindf = joindf.where((joindf.transaction_text != 'B2C Reversal') & (joindf.otherParty != 'SP') & (joindf.operationType == 0) & (joindf.otherParty == 'Organization')).select('msisdn', 'advancedate', 'qualifiedamount')
# M0_Qualified = joindf.grouby('qualifiedamount'.agg(count('qualified amount')))
# joindf.show()
# uniqueQual =[]
# for row in joindf.groupby('msisdn').agg(countDistinct('qualifiedamount')).collect():
#   key =row[0]
#   value_count = row[1]

  # key.show()
# uniqueQual = df.select("amount").distinct().rdd.flatMap(lambda x: x).collect()
# for value in unique_buckets:
#   filBucket = df.filter(df.amount == value)
#   transformBucket = filBucket.withColumn('bucket', lit(value))

#   transformBucket = transformBucket.filter(filBucket.operationType == 1)

# display(transformBucket)

# COMMAND ----------

# January loans of 20 msisdn's
# dfQual = dfQual.filter((dfQual.amount == 20)).groupBy().agg(countDistinct('msisdn')).collect[0][0]
distinctMsisdn = dfQual.filter(dfQual.amount == 20).select(dfQual.msisdn).distinct()
distinctMsisdn.show()

# COMMAND ----------

df = df.filter((df.Dates >= '2022-12-01') & (df.Dates < '2023-02-01') & (df.otherParty == 'Customer') & (df.operationType == 0))
df = df.drop('advanceId', 'transactionId', 'year', 'month', 'entity_name', 'entity')
dfQual = dfQual.drop('amount')
joindf = dfQual.join(df, on="msisdn", how="inner")
joindf = joindf.where(~(joindf.transaction_text == 'B2C Reversal')).select('msisdn', 'Dates', 'qualifiedamount', 'amount')
joindf = joindf.drop('advancedate', 'advancestatus', 'qualifiedamount', 'dfQual.amount', 'duedate', 'repaydate', 'transaction_text')

#M0 Recharge Calculation

M0 = joindf.where((joindf.Dates >= '2023-01-01') & (joindf.Dates < '2023-02-01')).groupBy('msisdn', 'Dates').agg(sum('amount'))
display(M0)

# COMMAND ----------

# graph the number of loans per month in the system (bar)
# df = df.withColumn('date', to_date(df.eventDate, 'yyyy-MM-dd'))
df_adv =  df.filter(df.operationType == 1).groupby('Dates').agg(sum('amount')\
                      .alias("LoanBucketSum"), count('Dates').alias("LoanBucketCount"))
                                                            
# filtered_df = df.where(df.operationType == 1).where(df.eventDate >= '2022-12-01').where(df.eventDate < '2023-01-01')
display(df_adv)
# df_truncated = df_adv.select(trunc(to_timestamp(col("eventDate")), "day").cast("timestamp").alias("truncDate"))
# df_truncated.show()
# comment on the trends in number of loans per month

## Number of loans with interest
## Any loans with charge type 3

# COMMAND ----------

test =  df.filter(df.operationType == 1).groupby('Dates').agg(sum('amount')\
                      .alias("LoanBucketSum"), count('Dates').alias("LoanBucketCount"))

display(test)

# COMMAND ----------

# comment on the loan value trends
dfLoans = df.filter(df.operationType == 1).groupby('amount', 'Dates')\
  .agg(count('Dates'))\
    .alias('LoanBuckets')

display(dfLoans)


# COMMAND ----------

# graph the average loan value per month in the system (bar)
# comment on the average loan value trends
test =  df.filter(df.operationType == 1).groupby('Dates', 'amount').agg(sum('amount')\
                      .alias("LoanBucketSum"), count('Dates').alias("LoanBucketCount"))

display(test)
## Dist of loan values taken

# COMMAND ----------

# graph the number of loans per person per month as a bar graph (bar)

# comment on trends in the frequency of loan use over time


## Not rolling at first
## Consider at rolling afterwards

# COMMAND ----------

# find the time between loans and graph the distribution for the population

# comment on the distribution of time between loans

# Period between each loan

## Revisit at later stage possibly 

# COMMAND ----------

# MAGIC %md #### Loans and Payments

# COMMAND ----------

# otherParty vs Recharge Types to determine feasiblity of otherParty in Reacharge Profiles

dframe = df.where(df.operationType == 0)\
            .groupby("otherParty", "transaction_text", "profileEligibility")\
              .agg(sum("amount"))
dframe = dframe.drop('eventDate', 'msisdn', 'advanceId', 'operationType', 'year', 'month', 'entity', 'entity_name')
dframe = dframe.orderBy(dframe.profileEligibility)
display(dframe)
display(dframe.select("otherParty", "transaction_text").show())

# COMMAND ----------

# graph the distribution in time from loan to payment
recharges = df.where(((df.operationType == 0) | (df.operationType == 2)))\
              .groupby('Dates', 'operationType')\
                .agg(count('amount').alias('Counts'),
                     sum("amount").alias('Value'))
display(recharges)
# rechargeSum = df.where(((df.operationType == 0) | (df.operationType == 2)))\
#               .groupby('Dates', 'operationType')\
#                 .agg(sum('amount').alias('rechargeSums'))

# rechargesApp = recharges.join(rechargeSum, on= "Dates", how="left")
# colDups = ['']
# RecPay = rechargesApp.filter((rechargesApp.rechargesCounts.isNotNull()) | (rechargesApp.rechargeSums.isNotNull())).dropDuplicates()
# display(RecPay)


# comment on the recovery distribution in the system

## December, Jan, Feb to make sure it's falling in 30 days

# COMMAND ----------


# review the size of payments and when they occur (graph temporally)
# comment on how payments are occuring

## Most are sweeps

# COMMAND ----------

# graph the size of the loan vs the size of the associated payment
rechargeType = df.where((df.operationType == 0) | (df.operationType == 2)).groupby('transaction_text', 'Dates', 'profileEligibility').agg(sum('amount').alias('rechargeTypeSum'), count('transaction_text').alias('rechargeTypeFreq'))
eligibleRecharges = rechargeType.filter(rechargeType.profileEligibility == 1)
NonEligbelRecharges = rechargeType.filter(rechargeType.profileEligibility == 0)
display(eligibleRecharges)
display(NonEligbelRecharges)
## size of payment vs source (check later only in finance)

# COMMAND ----------

# graph the temporal trend in pyaments - when they occur in a month / day etc..

## sweep starts at 1am - 7pm (check lizani)

# COMMAND ----------

# graph the average number of payments per loan per month (bar)

## H0: interest due will have smaller payments vs no interest

# COMMAND ----------

# MAGIC %md #### Recharges

# COMMAND ----------

# graph the average recharge amount per person per month

## exclusive 
## eligible or non eligble
## UFone analysis to take a look at (Dennis)

# COMMAND ----------

# graph the average recharge number per person per month

## Count on recharges
## excludeing payemnts

# COMMAND ----------

# graph the distribution of recharge amounts for the system



# COMMAND ----------

# graph the distribution of recharge number for the system

## Dist of recharges denoms

# COMMAND ----------

# Per person find the M-1, M-2, M-3 (M0 being the month a loan was taken) total recharge amounts, recharge counts, recharge averages - plot these as lines per month

## Dennis UFone analysis could assist with this 

# COMMAND ----------

# graph the days a person recharges during a month - plot temporally

## Days of week, month frequency
## Keep promotions in mind

# COMMAND ----------


