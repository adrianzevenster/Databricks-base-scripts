# Databricks notebook source
# MAGIC %md 
# MAGIC #### Extract specs 
# MAGIC
# MAGIC Good afternoon Adrian, 
# MAGIC
# MAGIC I hope you are well! 
# MAGIC
# MAGIC As per our discussion on Skype, would it please be possible to obtain the following extract of Cell C advances, payments and recharges: 
# MAGIC
# MAGIC Please note that we are filtering by suffix for this extract and not by tier. 
# MAGIC
# MAGIC Suffixes  
# MAGIC 06,08,22,39,42,49,57,63,65,73,92
# MAGIC  
# MAGIC Loan data  
# MAGIC Start date	01 08 2022  
# MAGIC End date	01 09 2022  
# MAGIC
# MAGIC Historical data  
# MAGIC Start date	01 04 2022  
# MAGIC End date	01 10 2022  
# MAGIC
# MAGIC Please remember that the ‘loan data’ dates are the dates that we use to get unique subs, i.e. subs that took loans in that period. 
# MAGIC The ‘Historical data’ dates are used to get the advance, recharge and payment data for those unique subs.
# MAGIC
# MAGIC Tiers for loan data
# MAGIC 	All tiers as we have credit and ML suffixes 
# MAGIC  
# MAGIC  
# MAGIC Tiers for historical data
# MAGIC ALL TIERS
# MAGIC
# MAGIC Please In addition to the usual columns, would it please be possible for you to add the following columns to the advance data: 
# MAGIC Qualifying amount
# MAGIC Service type – the numerical index 
# MAGIC Channel_type – the numerical index

# COMMAND ----------

#V3: Removed working code to the bottom and ensured that functions were at the top

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Imports

# COMMAND ----------

# library imports
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row, DataFrame, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from time import gmtime, strftime
import datetime
from functools import reduce
from pyspark.sql import SQLContext
import numpy as np
#import pandas as pd
import configparser as cfpg
import io 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Functions

# COMMAND ----------

# function to add leading zero to MSISDNs
def add_leading_zero(df):
  #   Adding a leading zero to MSISDN's that only have 9 characters
  df = df.withColumn('MSISDN', F.when( F.length(F.col('MSISDN')) ==  9, F.concat(F.lit("0"), F.col('MSISDN'))).otherwise(F.col('MSISDN')))
  df = df.withColumn('LEN MSISDN', F.length(F.col('MSISDN')))
  # filtering for instances where there is a valid MSISDN
  df = df.filter(F.col('LEN MSISDN')== 10)
  # dropping the intermediate column 
  df = df.drop('LEN MSISDN')
  return(df)

# COMMAND ----------

# function to reconcile the max loan amount
# input is the loan or model loan dataframe
def max_loan_amount_rec(loan_df, pay_df):
  # Extract year, month, day, hour, min to get to the minute loan granting on date
  loan_df = loan_df.withColumn("MONTH", F.month('LRA_DATE'))
  loan_df = loan_df.withColumn("YEAR", F.year('LRA_DATE'))
  loan_df = loan_df.withColumn("DOM"  , F.date_format(col("LRA_DATE"), 'd'))
  loan_df = loan_df.withColumn("HOUR" , F.date_format(col("LRA_DATE"), 'H'))
  loan_df = loan_df.withColumn("MIN"  , F.date_format(col("LRA_DATE"), 'm'))
  
  # partition by unique msisdn and within minute loan granting to find max designation
  w = Window.partitionBy('MSISDN','YEAR','MONTH','DOM','HOUR','MIN').orderBy(desc('LRA_DATE'))
  
  # Do the same thing but order differently for rank functionality (filter on value == one)
  w1 = Window.partitionBy('MSISDN','YEAR','MONTH','DOM','HOUR','MIN').orderBy(asc('LRA_DATE'))
  
  # Find the sum of loans over the window specified
  loan_df = loan_df.withColumn('CUM_LOAN_AMOUNT',F.sum(col('LRA_LOAN_AMOUNT')).over(w))
  
  # Get the total paid to a specific loan ID (these can be the sum paid to a single loan and not a group of loans in max)
  pay_df = pay_df.withColumn("TOTAL_PAID", F.sum("LRAP_RECHARGE_AMOUNT").over(Window.partitionBy("LRAP_LRA_UID"))) \
                 .withColumn("RNK", F.row_number().over(Window.partitionBy("LRAP_LRA_UID").orderBy(F.desc("LRAP_DATE")))) \
                 .filter(col("RNK") == 1).drop("RNK") \
                 .withColumnRenamed("LRAP_DATE", "LAST_PAYMENT_DATE") \
                 .drop("LRAP_RECHARGE_AMOUNT")
  
  # join loans and payments
  loan_df = loan_df.join(pay_df.drop('MSISDN'),  (loan_df.LRA_UID==pay_df.LRAP_LRA_UID), 'left')

  # Find the sum of payments to the multiple loans granted by max
  loan_df = loan_df.withColumn('CUM_PAID_AMOUNT',F.sum(col('TOTAL_PAID')).over(w))

  # Rank the loans by order 
  loan_df = loan_df.withColumn('RANK',F.dense_rank().over(w1))

  # Filter rank to get the total loan amount taken in max and the total paid to the maximum value
  loan_df = loan_df.filter(col('RANK')==1)

  # Select pertinent columns
  loan_df = loan_df.select(['MSISDN',
                            'LRA_UID',
                            'LRA_CS_UID',
                            'LRA_DATE',
                            'SERVICE_FEE',
                            'LRA_SS_UID',
                            'LRA_LR_UID',
                            'CUM_LOAN_AMOUNT',
                            'LAST_PAYMENT_DATE',
                            'LRAP_LRA_UID',
                            'CUM_PAID_AMOUNT'
                         ])
  
  # Rename columns to reflect 
  loan_df = loan_df.withColumnRenamed('CUM_LOAN_AMOUNT','LRA_LOAN_AMOUNT')\
                   .withColumnRenamed('CUM_PAID_AMOUNT','LRAP_RECHARGE_AMOUNT').withColumnRenamed('LAST_PAYMENT_DATE','LRAP_DATE')
  
  
  ####################################################################################################################
  ####################################################################################################################
  ####################################################################################################################
  
#   loan_df = loan_df.withColumn('DAYS_TO_PAY', F.datediff(loan_df.LRAP_DATE,loan_df.LRA_DATE))
  
#   do for windows.........
  
#   loan_df = loan_df.withColumn('PAID_30',  F.when((col("DAYS_TO_PAY") <= 30), 1).otherwise(0)) 
  
#   loan_df = loan_df.withColumn('TARGET_30', F.when( (col('PAID_30')==1) & (col('LRA_LOAN_AMOUNT')<=col('LRAP_RECHARGE_AMOUNT')), 1).otherwise(0))
  
  ####################################################################################################################
  ####################################################################################################################
  ####################################################################################################################  

  

  # Separate payments, nulls (from no payment on the left join) will not affect the algorithm so no need to handle here
  pay_df = loan_df.select(['MSISDN',
                           'LRAP_DATE',
                           'LRAP_LRA_UID',
                           'LRAP_RECHARGE_AMOUNT' ###### CHECK THIS!!! AGG USED LATER ON IN FEATURE CREATION !!!!!!! 
                           ])

  # Separate model loan values
  loan_df = loan_df.select(['MSISDN',
                            'LRA_UID',
                            'LRA_CS_UID',
                            'LRA_DATE', 
#                             'LRAP_DATE',
                            'SERVICE_FEE',
                            'LRA_SS_UID',
                            'LRA_LR_UID',
                            'LRA_LOAN_AMOUNT',
#                             'LRAP_RECHARGE_AMOUNT',
#                             'DAYS_TO_PAY' ,
#                             'PAID_30',
#                             'TARGET_30'
                           ])
  
  return (loan_df,pay_df)
  
  

# COMMAND ----------

# below is a function that outputs the windows of aggregation possible given the dates 
# and target available as well as min data points desired for modelling

# THIS FUNCTION ASSUMES THAT THE 3 DFS START AND END ON THE SAME DAYS 
# ALSO ASSUMES THAT model_loan_dates is included in loan_history_days 

def get_valid_window(windowList, payments_dates, recharges_dates, loan_history_dates, model_loans_dates, model_loans, target, min_datapoints):
  # the day periods over which features and targets will be calculated 
  # this is determined by the amount of data available

  # define the time(days) windowList for feature creation 
  # this is telco/model specific and will be used to determine the date range
  # checks are employed to ensure that sufficient data exists to allow
  # for the selected windowList 

  # filtering the data to ensure that sufficient data exists to allow for 
  # the appropriate windows to be used 
  # Recharges must look BACK (max(windowList)) days from the first day of the loan
  # Payments must look FORWARD (max(windowList)) days from the last day of the loan
  # model_loans is the key df - we want to include as much of it as recharges and payments allow 
  # calculating the bounds of the dat
  
  start_payment_date = payments_dates[0].date()
  end_payment_date = payments_dates[1].date()
  start_recharge_date = recharges_dates[0].date()
  end_recharge_date = recharges_dates[1].date()
  start_loans_date = loan_history_dates[0].date()
  end_loans_date = loan_history_dates[1].date()
  start_model_loans_date = model_loans_dates[0].date()
  end_model_loans_date =model_loans_dates[1].date()

    # checking if our assumption is true about model loans being within historical loans
  if( (start_model_loans_date <  start_loans_date) or (end_model_loans_date > end_loans_date) ):
    print("The dates for model loans must be within the date range of loan history")
    return("_", "_", "_")
  
  # checking if the 3 dfs start and end on the same days 
  if( (start_recharge_date != start_loans_date) or (start_recharge_date != start_payment_date) or
      (end_recharge_date != end_loans_date) or (end_recharge_date != end_payment_date)):
    print("The historical loan, recharge and payment dataframes must all start and end on the same dates for this function to work")
    return("_", "_", "_")
  
  common_start_date = __builtin__.max(start_payment_date, start_recharge_date, start_loans_date)
  common_end_date = __builtin__.min(end_payment_date, end_recharge_date, end_loans_date)
  # model_data = model_data.filter( (col("LRA_DATE") >= "2021-08-16" ) & (col("LRA_DATE") < "2021-09-20" )) 
  # window list needs to be auto adjusted based on the meta-file time-stamp data
  maximum_window = __builtin__.max(windowList)
  # extra days to add onto the filter 
  tolerance_days = 1
  dates_valid = False
  data_points_valid = False
  number_of_datapoints = 0
  
  
  while(data_points_valid == False): 
    # we look w days back from the 'new_loan_start_date' to be able to calc features
    new_loan_start_date = start_recharge_date + datetime.timedelta(days=maximum_window + tolerance_days)
    # we look target days forward from the 'new_loan_end_date' to be able to calc targets 
    new_loan_end_date = end_payment_date - datetime.timedelta(days=target + tolerance_days)

    if((new_loan_start_date >= start_loans_date) and (new_loan_end_date <= end_loans_date) and ( new_loan_start_date <  new_loan_end_date) and \
       (end_recharge_date >= new_loan_end_date) and (start_recharge_date < new_loan_start_date - datetime.timedelta(days=maximum_window) ) and \
       (end_payment_date >= new_loan_end_date + datetime.timedelta(days=target)) and (start_payment_date < new_loan_start_date - datetime.timedelta(days=maximum_window)) ):
      
      # if dates are valid, the following code will calculate the resulting number of datapoints 
      # this number of datapoints must be larger than or equal to the min specified
    
      start_date_str = new_loan_start_date.strftime("%Y-%m-%d")
      end_date_str = new_loan_end_date.strftime("%Y-%m-%d")
      model_data = model_loans.filter( (col("LRA_DATE") >= start_date_str ) & (col("LRA_DATE") < end_date_str ))
      number_of_datapoints = model_data.count()
      
    if(number_of_datapoints >= min_datapoints):
      print('SUCCESS')
      print('------------------------------')
      print('Input parameters satisfied')
      print('Datapoints: ', number_of_datapoints, ' Min required: ', min_datapoints)
      print('WindowList: ', windowList)
      print('Start date: ', new_loan_start_date)
      print('End date:', new_loan_end_date)
      data_points_valid = True
      
    # if the number of datapoints or dates are not valid, max_window is removed from the list
    elif(len(windowList)!=1):
      windowList.remove(maximum_window)
      maximum_window = __builtin__.max(windowList)
      
    elif(len(windowList)==1):
      print('FAILED')
      print('------------------------------')
      print('Input parameters not satisfied')
      print('Datapoints: ', number_of_datapoints, ' Min required: ', min_datapoints)
      print('WindowList: ', windowList)
      print('Start date: ', new_loan_start_date)
      print('End date:', new_loan_end_date)
      break
  
  return(windowList, new_loan_start_date, new_loan_end_date)

# COMMAND ----------

# function that generates model features
# create function that takes the inputs model loans, loan history_df, recharges_df,windowList as inputs
# added future loan spend x
# added future loan count x
def calc_attr(model_loans_df = None, loanDF=None, rechargeDF=None, windowList=None):
   
    #renaming columns match rechargeDF and loanDF in order to be able to union join
    #recharges operation type set to "R" and advances operation type set to "L"
    
    rechargeDF = rechargeDF.withColumnRenamed("TRANSACTIONAMOUNT", "LRA_AMOUNT") \
                           .withColumnRenamed("TIMESTAMP", "LRA_DATE") \
                           .withColumn("LRA_UID", F.lit(0).cast('long')) \
                           .withColumn("OPERATION_TYPE", F.lit("R")) \
                           .select("MSISDN", "LRA_UID", "LRA_DATE", "LRA_AMOUNT", "OPERATION_TYPE")
    
    loanDF = loanDF.withColumnRenamed("LRA_LOAN_AMOUNT", "LRA_AMOUNT") \
                   .withColumn("OPERATION_TYPE", F.lit("L")) \
                   .select("MSISDN", "LRA_UID", "LRA_DATE", "LRA_AMOUNT", "OPERATION_TYPE")
    #combine the advances
    result = loanDF.union(rechargeDF)
    
    days = lambda i: i * 86400  #????
    #filter through the different specified day windows for both Loans and recharges. 
    #loan data for loan features and recharges for spend features
    cols = ["MSISDN", "LRA_UID", "LRA_DATE", "LRA_AMOUNT"]
    # loops over different targets stored in windowList
    # dependant on available data
    for window in windowList :
        for typeOver in ["LOAN", "SPEND"]:
            if(typeOver == "LOAN"):
                filterList = ["L"]
            elif(typeOver == "SPEND"):
                filterList = ["R"] # Do we have recharges that denote loan repayment and just general recharging? 
            else:
                None
                
            #split result by msisdn. Order by date and filter by range of the number of days of selected windows
            #cast -convert field type from string to long    
            w = Window.partitionBy(result.MSISDN).orderBy(col("LRA_DATE").cast('long')).rangeBetween(-days(window), -1)
            
            #create column NAMES for the COUNT FEATURES
            #COUNT_SPEND_WINDOW OR COUNT_LOAN_WINDOW
            cntCol = "{}_{}_{}".format("COUNT", typeOver.upper(), window)
            
            #ADD THE NEW COUNT FEATURE COLUMNS TO THE EXISTING COLUMN LIST
            cols = cols + [cntCol]
            #CREATE DF WITH THE COUNT FEATURES FOR THE DIFFERENT TIME/DAY WINDOWS.THIS IS DONE BY ASSIGNING A 1 TO EACH LOAN AND RECHARGE
            #AND performing a count
            result = result.withColumn(cntCol, F.count(F.when(col("OPERATION_TYPE").isin(filterList), 1).otherwise(None)).over(w))
            
            #CREATE THE OTHER FEATURES FOR MEAN,SUM,MAX,MIN. THIS WILL LOOP FOR EACH TIME WINDOW IN THE WINDOW LIST
            for aggFunName in ["sum", "mean", "max", "min"]:
                attrColName = "{}_{}_{}".format(aggFunName.upper(), typeOver.upper(), window)
                cols = cols + [attrColName]
                aggType = aggTypeDict[aggFunName]
                #CREATE COLUMN BY PERFOMING THE AGGREGATIONS FROM AGGRAGATION LIST .VALUES CONSIDERED WHEN THE OPERATION TYPE IS "L" or "R"
                result = result.withColumn(attrColName, aggType(F.when(col("OPERATION_TYPE").isin(filterList), col("LRA_AMOUNT")).otherwise(None)).over(w))
        
        # delete me if I break things
        w_lead = Window.partitionBy(result.MSISDN).orderBy(col("LRA_DATE").cast('long')).rangeBetween(0, days(window))
        targetCol = "{}_{}".format("TARGET", window)
        future_attr = "FUTURE_SPEND_SUM_{}".format(window) 
        if(targetCol not in result.columns ):
            cols = cols + [future_attr]
            result = result.withColumn(future_attr, F.sum(F.when(col("OPERATION_TYPE").isin("R"), col("LRA_AMOUNT")).otherwise(0)).over(w_lead))
#             Below is supposed to set target = 1 for guys who would have been able to pay loans off. This is catered for in sample creation.
#             result = result.withColumn(targetCol, F.when(F.col("LRA_AMOUNT") + 1 > F.col(future_attr), "1").otherwise("0"))
    
        # FUTURE LOAN SUM
        # delete me if I break things
        w_lead = Window.partitionBy(result.MSISDN).orderBy(col("LRA_DATE").cast('long')).rangeBetween(0, days(window))
        targetCol = "{}_{}".format("TARGET", window)
        future_attr = "FUTURE_LOAN_SUM_{}".format(window) 
        if(targetCol not in result.columns ):
            cols = cols + [future_attr]
            result = result.withColumn(future_attr, F.sum(F.when(col("OPERATION_TYPE").isin("L"), col("LRA_AMOUNT")).otherwise(0)).over(w_lead))
        
        # FUTURE LOAN COUNT
        # delete me if I break things
        w_lead = Window.partitionBy(result.MSISDN).orderBy(col("LRA_DATE").cast('long')).rangeBetween(0, days(window))
        targetCol = "{}_{}".format("TARGET", window)
        future_attr = "FUTURE_LOAN_COUNT_{}".format(window) 
        if(targetCol not in result.columns ):
            cols = cols + [future_attr]
            result = result.withColumn(future_attr, F.count(F.when(col("OPERATION_TYPE").isin("L"), col("LRA_AMOUNT")).otherwise(0)).over(w_lead))
            
    #From the features created, select data the relates to loans/advances           
    result = result.filter(col("OPERATION_TYPE") == "L").select(cols) # We only care about the feature created on a loan.
    result = result.select(cols)
    
  # Below line is for other telcos replaced with code in the block below
#     model_loans = model_loans_df.select("MSISDN", "LRA_UID").distinct()
    
    #inner join to keep only the loan features for subscribers who took advances under the ML tier
    ##############################################################################################
    ## SPECIFICALLY FOR CELL C WE NEED TO KEEP THE AGGREGATED TOTAL LOAN VALUE
    ## THE ADVANCE ID OF THE AGGREGATED LOAN, WILL CORRELATE TO A SINGLE CONTRIBUTING LOAN 
    ## FROM THE ORIGINAL LOAN DF IN THAT WAY WE CAN JOIN ON MSISDN AND LRA_UID
    ## WE MUST JUST ENSURE TO KEEP THE AGGREGATED LOAN VALUE
    model_loans = model_loans_df.select("MSISDN", "LRA_UID", "LRA_LOAN_AMOUNT").distinct()
    model_loans = model_loans.withColumnRenamed("LRA_LOAN_AMOUNT", "TOTAL_LOAN_AMOUNT")
    
    result = result.join(broadcast(model_loans), ["MSISDN", "LRA_UID"], "inner")
    
    result = result.withColumn("LRA_AMOUNT", F.col("TOTAL_LOAN_AMOUNT"))
    result = result.drop(F.col("TOTAL_LOAN_AMOUNT"))
        
    ##############################################################################################
    
      # Below line is for other telcos replaced with code in the block above
      #Only keep the targets for subscribers who took advances under ML tier by performing intersection join
#     result = result.join(broadcast(model_loans), ["MSISDN", "LRA_UID"], "inner") # This is going to require some explaination
      
    return result

# COMMAND ----------

# function that is used to generate pay day features
def calc_pay_days_attr(model_loans_df = None, loanDF=None, paymentDF=None, windowList=None):
    paymentDF = paymentDF.withColumnRenamed("LRAP_RECHARGE_AMOUNT", "LRA_AMOUNT") \
                           .withColumnRenamed("LRAP_LRA_UID", "LRA_UID") \
                           .select("MSISDN", "LRA_UID", "LRAP_DATE", "LRA_AMOUNT")
    
    # calculates the total amount that a user paid for a specific laon (LRA_UID)
    # selects the most recent pay date and drops the rest 
    # this allows us to obtain the date and total amount paid for a specific advance 
    payment_df = paymentDF.withColumn("TOTAL_PAID", F.sum("LRA_AMOUNT").over(Window.partitionBy("LRA_UID"))) \
                         .withColumn("RNK", F.row_number().over(Window.partitionBy("LRA_UID").orderBy(F.desc("LRAP_DATE")))) \
                         .filter(col("RNK") == 1).drop("RNK") \
                         .withColumnRenamed("LRAP_DATE", "LAST_PAYMENT_DATE") \
                         .drop("LRA_AMOUNT")
    
    model_loans = model_loans_df.withColumnRenamed("LRA_LOAN_AMOUNT", "LRA_AMOUNT") \
                            .select("MSISDN", "LRA_UID", "LRA_DATE", "LRA_AMOUNT")
    
    days = lambda i: i * 86400
    # join the loan DF with payment DF on msisdn and advance id. 
    # this serves to link advances to their payments which is essential in determing payback periods
    # Left -join, only add payment data for loan present in loan DF 
    result = loanDF.join(payment_df, ['MSISDN', 'LRA_UID'], "left")\
                    .drop("PMT_LRA_UID") 
    
    # determine the number of days take to payback each loan   
    result = result.withColumn("DAYS_TO_PAY", F.datediff(result.LAST_PAYMENT_DATE, result.LRA_DATE))
                    
    for window in windowList :  
        interval = "INTERVAL {} DAYS".format(window)
        for aggFunName in ["mean", "max", "min", "std"] : #no longer relevant features
            payDayCol = "{}_PAY_DAYS_{}".format(aggFunName.upper(), window)
            aggType = aggTypeDict[aggFunName]
            result = result.withColumn(payDayCol, aggType(result.DAYS_TO_PAY).over(Window.partitionBy(result.MSISDN).orderBy(col("LRA_DATE").cast('long')).rangeBetween(-days(window), -1)))
         
        #create the target columns based on the chosen window periods 
        targetCol = "{}_{}".format("TARGET", window) 
        # Label target as 1 if paid in time else for default let target = 0 . 
        # If susbscriber paid their advance within the particular window period then assign a 1 to that target , otherwise 0
        result = result.withColumn(targetCol, F.when((col("DAYS_TO_PAY") <= window), 1).otherwise(0))
    
      # Below line is for other telcos replaced with code in the block below
#     model_loans = model_loans_df.select("MSISDN", "LRA_UID").distinct()
    
    #inner join to keep only the loan features for subscribers who took advances under the ML tier
    ##############################################################################################
    ## SPECIFICALLY FOR CELL C WE NEED TO KEEP THE AGGREGATED TOTAL LOAN VALUE
    ## THE ADVANCE ID OF THE AGGREGATED LOAN, WILL CORRELATE TO A SINGLE CONTRIBUTING LOAN 
    ## FROM THE ORIGINAL LOAN DF IN THAT WAY WE CAN JOIN ON MSISDN AND LRA_UID
    ## WE MUST JUST ENSURE TO KEEP THE AGGREGATED LOAN VALUE
    model_loans = model_loans_df.select("MSISDN", "LRA_UID", "LRA_LOAN_AMOUNT").distinct()
    model_loans = model_loans.withColumnRenamed("LRA_LOAN_AMOUNT", "TOTAL_LOAN_AMOUNT")
    
    result = result.join(broadcast(model_loans), ["MSISDN", "LRA_UID"], "inner")
    
    result = result.withColumn("LRA_AMOUNT", F.col("TOTAL_LOAN_AMOUNT"))
    result = result.drop(F.col("TOTAL_LOAN_AMOUNT"))
        
    ##############################################################################################
    
      # Below line is for other telcos replaced with code in the block above
      #Only keep the targets for subscribers who took advances under ML tier by performing intersection join
#     result = result.join(broadcast(model_loans), ["MSISDN", "LRA_UID"], "inner") # This is going to require some explaination

    return result
# The above function looks like we are not explicitly checking if payment > loan amt

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Target functions

# COMMAND ----------

# function to create a corrected future sum spend
def future_spend_correction(df, windowList=None):
  
  # extracting the columns 
  columns = df.columns
  
  for window in windowList: 
    # service fee sum will be WRONG if we run the max_loan_amount_reconcilliation first
    # need to check if this is necessary 
    # calculating the service fee that is owed 
    df = df.withColumn("FUTURE_SERVICE_FEE_SUM_" + str(window), 1.1 * F.col("FUTURE_LOAN_COUNT_" + str(window)))
    # calculating the corrected spend which is the spend minus what is owed 
    df = df.withColumn("FUTURE_SPEND_SUM_CORRECTED_" + str(window), ( F.col("FUTURE_SPEND_SUM_" + str(window)) -
                                                                      F.col("FUTURE_LOAN_SUM_" + str(window)) -
                                                                      F.col("FUTURE_SERVICE_FEE_SUM_" + str(window))))
  
  return(df)

# COMMAND ----------

#########################################
### NB TO CHECK THE MAX LOAN AMOUNT RECONCILLIATION 
# the undersample_daily_sum_df function might negate the need
########################################################

# COMMAND ----------

# function to drop repeated instances on a single day per MSISDN
def undersample_daily_sum_df(df):
  
  columns = df.columns
  # creating a date column
  df = df.withColumn('DATE', F.to_date(F.col('LRA_DATE')))
  
  # adding all the loans taken in a particular day
  w = Window.partitionBy(['MSISDN', 'DATE']).orderBy(col("LRA_DATE"))
  df = df.withColumn('TOTAL_LRA_AMOUNT', F.sum(F.col("LRA_AMOUNT")).over(w))
  
  # add a window function to multiply all the targets or take the min over the date  - was the initial idea 
  # multiplying all targets for all loans summed per day
  # this implies that the user would have to pay all loans off in TARGET_X days for that day to be valid 
  # we can just take the min, if they didn't pay off a single loan the min will be ZERO 
  # looping through the columns for the target columns
  for i in range(0, len(columns)):
    if ("TARGET" in columns[i]):
      print("Manipulating column:", columns[i])
      df = df.withColumn(columns[i] + "_MIN",  F.min(columns[i]).over(w))
  
  # either the above or you have to drop the current target as it will be misleading
  df = df.orderBy(F.col('MSISDN'), F.col('DATE').asc(), F.col('TOTAL_LRA_AMOUNT').desc())
  # selecting only the instance per day with the maximum loans taken for that day 
  df = df.drop_duplicates(subset=['MSISDN', 'DATE'])
  
#   print(df.count())
  
  return(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### End of Functions

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Start of operational code

# COMMAND ----------

# S3 bucket in which data is stored from extract
MOUNT_NAME = "av_automated_ml"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Data Pre-processing

# COMMAND ----------



metafile = sc.textFile("/mnt/{MOUNT_NAME}/training_request_files/aws_info_file_2022-05-31.txt".format(MOUNT_NAME=MOUNT_NAME)).collect() 
metadata = io.StringIO("\n".join(metafile)) 
parse_str = cfpg.ConfigParser() 
parse_str.read_file(metadata) 
 
# create variables from metadata file
telco=parse_str.get('ml_training_info','telco_name') 
model_target=parse_str.get('ml_training_info','model_target')
model_start_date=parse_str.get('ml_training_info','model_start_date')
model_end_date=parse_str.get('ml_training_info','model_end_date')
history_start_date=parse_str.get('ml_training_info','history_start_date')
history_end_date=parse_str.get('ml_training_info','history_end_date')
account_name=parse_str.get('ml_training_info','account_name')


# get the list of filenames to load - files from the SQL extract
filenames=parse_str.get('ml_training_info','filenames')
filenames=filenames.replace('[','').replace(']','').replace("'","").split(',')

advances_filename=filenames[0].strip()
payments_filename=filenames[1].strip()
recharges_filename=filenames[2].strip()

# get the list of the model tiers selected in extraction
model_tiers=parse_str.get('ml_training_info','model_tiers')
# reformatting the filter for model loans
model_tiers=model_tiers.strip(")").strip("(").split(',')
model_tier_filter=[]

for i in model_tiers:
  model_tier_filter.append(int(float(i)))
  


# COMMAND ----------

telco = "ZA_CELLC"
model_target = 30 
model_start_date = "2022-08-01"
model_end_date = "2022-08-31"
history_start_date = "2022-04-01"
history_end_date = "2022-10-01"
advances_filename = "/mnt/cellc/Q4 O1 2022 KR1/Qualified-amount/data/processed/Advances/2022-11-01_all-advances_cleaned.parquet"
payments_filename = "/mnt/cellc/Q4 O1 2022 KR1/Qualified-amount/data/processed/Payments/2022-11-01_all-payments_cleaned.parquet"
recharges_filename = "/mnt/cellc/Q4 O1 2022 KR1/Qualified-amount/data/processed/Recharges/2022-11-01_all-recharges_cleaned.parquet"
# model_tiers = 
# model_tier_filter = 

# COMMAND ----------

# load all the files from S3 based on parsed filenames
all_advances = spark.read.parquet(advances_filename,inferSchema=True,header=True)
payments=spark.read.parquet(payments_filename,inferSchema=True,header=True)
recharges=spark.read.parquet(recharges_filename,inferSchema=True,header=True)

# COMMAND ----------

# rename column names to match template

all_advances = all_advances.withColumnRenamed("ADVANCE_ID","LRA_UID") \
                           .withColumnRenamed( "CHANNEL_ID","LRA_CS_UID") \
                           .withColumnRenamed( "LOAN_DATE","LRA_DATE") \
                            .withColumnRenamed( "LOAN_AMOUNT","LRA_LOAN_AMOUNT") \
                            .withColumnRenamed( "STATUS_ID","LRA_SS_UID") \
                            .withColumnRenamed( "PROFILE_TIER_ID","LRA_LR_UID").drop('_c0')

# Changing the column names to standard
payments= payments.withColumnRenamed("IN_DATE","LRAP_DATE") \
                           .withColumnRenamed( "PAYMENT_AMOUNT","LRAP_RECHARGE_AMOUNT") \
                            .withColumnRenamed("ADVANCE_ID","LRAP_LRA_UID").drop('_c0')

recharges = recharges.withColumnRenamed("DP_DATE","TIMESTAMP") \
                           .withColumnRenamed( "RECHARGE_AMOUNT","TRANSACTIONAMOUNT").drop('_c0')

dfs = [all_advances, payments, recharges]

  
for i in range(0, len(dfs)):
  dfs[i] = add_leading_zero(dfs[i])

# COMMAND ----------

# dropping any duplicates
payments = payments.dropDuplicates()
recharges = recharges.dropDuplicates()
all_advances = all_advances.dropDuplicates()

# COMMAND ----------

# remove any leading spaces within the columns and setting data types
loan_history_df = all_advances.withColumn('MSISDN_',F.trim(col("MSISDN")).cast(LongType())).drop("MSISDN").withColumnRenamed('MSISDN_','MSISDN')\
                                 .withColumn('LRA_UID_',F.trim(col('LRA_UID')).cast(DoubleType())).drop("LRA_UID").withColumnRenamed('LRA_UID_','LRA_UID')\
                                 .withColumn('LRA_CS_UID_',F.trim(col('LRA_CS_UID')).cast(IntegerType())).drop("LRA_CS_UID").withColumnRenamed('LRA_CS_UID_','LRA_CS_UID')\
                                 .withColumn('LRA_LOAN_AMOUNT_',col('LRA_LOAN_AMOUNT').cast(DoubleType())).drop("LRA_LOAN_AMOUNT").withColumnRenamed('LRA_LOAN_AMOUNT_','LRA_LOAN_AMOUNT')\
                                 .withColumn('SERVICE_FEE_',col('SERVICE_FEE').cast(DoubleType())).drop("SERVICE_FEE").withColumnRenamed('SERVICE_FEE_','SERVICE_FEE')\
                                 .withColumn('LRA_DATE_',col("LRA_DATE").cast(TimestampType())).drop("LRA_DATE").withColumnRenamed('LRA_DATE_','LRA_DATE')\
                                 .withColumn('LRA_SS_UID_',F.trim(col('LRA_SS_UID')).cast(IntegerType())).drop("LRA_SS_UID").withColumnRenamed('LRA_SS_UID_','LRA_SS_UID')\
                                 .withColumn('LRA_LR_UID_',F.trim(col('LRA_LR_UID')).cast(IntegerType())).drop("LRA_LR_UID").withColumnRenamed('LRA_LR_UID_','LRA_LR_UID')


payments_df=payments.withColumnRenamed('PAYMENT_DATE','LRAP_DATE').withColumn('LRAP_DATE_',col("LRAP_DATE").cast(TimestampType())).drop("LRAP_DATE").\
         withColumnRenamed('LRAP_DATE_','LRAP_DATE').withColumn('MSISDN_',F.trim(col("MSISDN")).cast(LongType())).drop("MSISDN").\
         withColumnRenamed('MSISDN_','MSISDN').withColumn('LRAP_LRA_UID_',col("LRAP_LRA_UID").cast(LongType())).drop("LRAP_LRA_UID").\
         withColumnRenamed('LRAP_LRA_UID_','LRAP_LRA_UID')


recharges_df = recharges.withColumn('MSISDN_',F.trim(col("MSISDN")).cast(LongType())).drop("MSISDN").withColumnRenamed('MSISDN_','MSISDN')\
            .withColumn('TIMESTAMP_',col("TIMESTAMP").cast(TimestampType())).drop("TIMESTAMP").withColumnRenamed('TIMESTAMP_','LRA_DATE')

# COMMAND ----------

# the advances used for model are those that were taken under model_tiers

# 'model_loans_df' represents the loans granted in the requested period 
# this correlates to the distinct MSISDN's on which the extract is based
# these MSISDN's would have qualified in that period and taken loans on a specific set of tiers (those specified
# in the SQL extract). The reason for filter on these tiers is to ensure that we are extracting 
# the correct data for retraining purposes
model_loans_df=loan_history_df.filter( (col('LRA_DATE') >= model_start_date) & (col('LRA_DATE') <= model_end_date))
# model_loans_df=model_loans_df.filter(col('LRA_LR_UID').isin(model_tier_filter))
# the reason we don't do a tier filter on 'loan_history_df' is because MSISDN's can move between tier groups
# since we look over a large date period we cannot ensure that this did not happen in the past

# COMMAND ----------

# manual data validation of all datasets 
# this is done to ensure that they all conform with requested dates and that 'model_loans_df'
# conforms with the loan period requested
loan_history_dates = loan_history_df.select(min('LRA_DATE'),max('LRA_DATE')).collect()[0]
print(loan_history_df.select(min('LRA_DATE'),max('LRA_DATE')).collect()[0])

model_loans_dates = model_loans_df.select(min('LRA_DATE'),max('LRA_DATE')).collect()[0]
print(model_loans_df.select(min('LRA_DATE'),max('LRA_DATE')).collect()[0])

payments_dates = payments_df.select(min('LRAP_DATE'),max('LRAP_DATE')).collect()[0]
print(payments_df.select(min('LRAP_DATE'),max('LRAP_DATE')).collect()[0])

recharges_dates = recharges_df.select(min('LRA_DATE'),max('LRA_DATE')).collect()[0]
print(recharges_df.select(min('LRA_DATE'), max('LRA_DATE')).collect()[0])

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Joining recharges - Not necessary as recharges are split in the profiler
# MAGIC In the recharges_df users' recharges are split into payments and residual.  
# MAGIC I.e. if they have a R10 loan outstanding, with a R1.1 service fee and they make a R20 recharge  
# MAGIC R11.1 will be deducted and stored with a 01:00:00 timestamp.  
# MAGIC The remaining R8.90 will be stored with a 00:00 timestamp as a recharge
# MAGIC
# MAGIC ** We need to check this for people that might be doing multiple true recharges in one day ** 

# COMMAND ----------

# this is NOT required as the profiler data is also split into payments and recharges
# validate this with Adrian and Doc
# recharges_df = recharges_df.withColumn( "LRA_DATE", F.to_date(F.col("LRA_DATE")))
# recharges_df = recharges_df.groupBy(['MSISDN', 'DP_ENTITY_TYPE', 'LRA_DATE']).sum('TRANSACTIONAMOUNT').withColumnRenamed('sum(TRANSACTIONAMOUNT)', 'TRANSACTIONAMOUNT')

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Max loan amount reconcilliation

# COMMAND ----------

model_loans_df, payments_df = max_loan_amount_rec(model_loans_df, payments_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Feature Creation

# COMMAND ----------

 # obtaining the valid windowList based on pre-entered list and target
windowList = [7, 14, 30, 45, 60, 75, 90, 120, 180]
windowList = [7, 14, 30, 45, 60]
target = 30
# setting min data points required
min_datapoints = 60000 

windowList, start_date, end_date = get_valid_window(windowList, payments_dates, recharges_dates, loan_history_dates, model_loans_dates,  model_loans_df, target, min_datapoints)
# formatting start and end dates 
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")
windowList, start_date_str, end_date_str

# COMMAND ----------

#dictionary with the arithmetic functions to be performed
aggTypeDict = dict( mean = F.mean, 
                    max = F.max, 
                    min = F.min, 
                    avg = F.avg, 
                    std = F.stddev,
                    sum = F.sum,
                    count = F.count
                   )

# COMMAND ----------

resultDF_rest = calc_attr(model_loans_df, loan_history_df, recharges_df, windowList)
resultDF_paydays = calc_pay_days_attr(model_loans_df, loan_history_df, payments_df, windowList).drop('LRA_DATE','LRA_AMOUNT', 'LAST_PAYMENT_DATE', 'TOTAL_PAID', 'DAYS_TO_PAY')

model_data = resultDF_rest.join(resultDF_paydays, ['MSISDN', 'LRA_UID'], "left")

# COMMAND ----------

model_data.display()

# COMMAND ----------

model_data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Testing a new function

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# display(undersample_df(model_data.filter(col("MSISDN")==606438763)))
display(undersample_daily_sum_df(model_data).orderBy(['MSISDN', 'LRA_DATE']))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### End of testing

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Manually checking the features for a single MSISDN

# COMMAND ----------

# manually checking the features for a single MSISDN
display(model_data.filter(col("MSISDN")==606438763))

# COMMAND ----------

display(model_data.filter(col("MSISDN")==606438763))

# COMMAND ----------

display(all_advances.filter(col("MSISDN")==606438763))

# COMMAND ----------

display(payments_df.filter(col("MSISDN")==606438763))

# COMMAND ----------

display(recharges_df.filter(col("MSISDN")==606438763))

# COMMAND ----------

#distinct_model_tiers=
model_tier=model_loans_df.select('LRA_LR_UID').distinct().collect()

# COMMAND ----------

model_tier

# COMMAND ----------

from datetime import datetime
date = datetime.now().date()

# COMMAND ----------

# write data to S3
model_data.repartition(1)\
          .write.format("parquet")\
          .mode("overwrite")\
          .option("header", "true")\
          .option("timestampFormat", "yyyy-MM-dd")\
          .save("/mnt/%s/%s/feature_data/%s_%s.parquet" % (MOUNT_NAME,telco,telco ,date))


# COMMAND ----------


