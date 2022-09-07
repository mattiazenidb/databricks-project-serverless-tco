# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Imports and variables/functions definition

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from datetime import datetime
import plotly.express as px
import pandas as pd

# COMMAND ----------

start_time = datetime.now()

# COMMAND ----------

startDate = dbutils.widgets.get('startDate')
endDate = dbutils.widgets.get('endDate')
customerName = dbutils.widgets.get('customerName')
costPerDbuClassic = float(dbutils.widgets.get('costPerDbuClassic'))
vmDiscountPercentage = int(dbutils.widgets.get('vmDiscountPercentage'))
serverlessDbuDiscountPercentage = int(dbutils.widgets.get('serverlessDbuDiscountPercentage'))
autoStopSeconds = int(dbutils.widgets.get('autoStopSeconds'))
serverlessDbuPriceIfMissingRegion = float(dbutils.widgets.get('serverlessDbuPriceIfMissingRegion'))

# COMMAND ----------

serverless_dbu_prices = {}
serverless_dbu_prices['us-east-1'] = 0.70
serverless_dbu_prices['us-east-2'] = 0.70
serverless_dbu_prices['us-west-2'] = 0.70
serverless_dbu_prices['eu-central-1'] = 0.91
serverless_dbu_prices['eu-west-1'] = 0.91
serverless_dbu_prices['ap-southeast-2'] = 0.95
serverless_dbu_prices['eastus'] = 0.70
serverless_dbu_prices['eastus2'] = 0.70
serverless_dbu_prices['westus'] = 0.70
serverless_dbu_prices['westeurope'] = 0.91
serverless_dbu_prices['northeurope'] = 0.91

# COMMAND ----------

vm_prices = {}
vm_prices['westeurope'] = {}
vm_prices['westeurope']['Standard_E8ds_v4'] = 0.692
vm_prices['westeurope']['Standard_E16ds_v4'] = 1.384
vm_prices['westeurope']['Standard_E32ds_v4'] = 2.768
vm_prices['westeurope']['Standard_E64ds_v4'] = 5.536
vm_prices['northeurope'] = {}
vm_prices['northeurope']['Standard_E8ds_v4'] = 0.640
vm_prices['northeurope']['Standard_E16ds_v4'] = 1.280
vm_prices['northeurope']['Standard_E32ds_v4'] = 2.560
vm_prices['northeurope']['Standard_E64ds_v4'] = 5.120
vm_prices['westus'] = {}
vm_prices['westus']['Standard_E8ds_v4'] = 0.648
vm_prices['westus']['Standard_E16ds_v4'] = 1.296
vm_prices['westus']['Standard_E32ds_v4'] = 2.592
vm_prices['westus']['Standard_E64ds_v4'] = 5.184
vm_prices['eastus2'] = {}
vm_prices['eastus2']['Standard_E8ds_v4'] = 0.576
vm_prices['eastus2']['Standard_E16ds_v4'] = 1.134
vm_prices['eastus2']['Standard_E32ds_v4'] = 2.268
vm_prices['eastus2']['Standard_E64ds_v4'] = 4.608
vm_prices['eastus'] = {}
vm_prices['eastus']['Standard_E8ds_v4'] = 0.576
vm_prices['eastus']['Standard_E16ds_v4'] = 1.134
vm_prices['eastus']['Standard_E32ds_v4'] = 2.268
vm_prices['eastus']['Standard_E64ds_v4'] = 4.608

vm_prices['us-east-1'] = {}
vm_prices['us-east-1']['i3.2xlarge'] = 0.624
vm_prices['us-east-1']['i3.4xlarge'] = 1.248
vm_prices['us-east-1']['i3.8xlarge'] = 2.496
vm_prices['us-east-1']['i3.16xlarge'] = 4.992
vm_prices['us-east-2'] = {}
vm_prices['us-east-2']['i3.2xlarge'] = 0.624
vm_prices['us-east-2']['i3.4xlarge'] = 1.248
vm_prices['us-east-2']['i3.8xlarge'] = 2.496
vm_prices['us-east-2']['i3.16xlarge'] = 4.992
vm_prices['us-west-2'] = {}
vm_prices['us-west-2']['i3.2xlarge'] = 0.624
vm_prices['us-west-2']['i3.4xlarge'] = 1.248
vm_prices['us-west-2']['i3.8xlarge'] = 2.496
vm_prices['us-west-2']['i3.16xlarge'] = 4.992
vm_prices['eu-central-1'] = {}
vm_prices['eu-central-1']['i3.2xlarge'] = 0.744
vm_prices['eu-central-1']['i3.4xlarge'] = 1.488
vm_prices['eu-central-1']['i3.8xlarge'] = 2.976
vm_prices['eu-central-1']['i3.16xlarge'] = 5.952
vm_prices['eu-west-1'] = {}
vm_prices['eu-west-1']['i3.2xlarge'] = 0.688
vm_prices['eu-west-1']['i3.4xlarge'] = 1.376
vm_prices['eu-west-1']['i3.8xlarge'] = 2.752
vm_prices['eu-west-1']['i3.16xlarge'] = 5.504
vm_prices['ap-southeast-2'] = {}
vm_prices['ap-southeast-2']['i3.2xlarge'] = 0.748
vm_prices['ap-southeast-2']['i3.4xlarge'] = 1.496
vm_prices['ap-southeast-2']['i3.8xlarge'] = 2.992
vm_prices['ap-southeast-2']['i3.16xlarge'] = 5.984

# COMMAND ----------

#maybe replace with broadcast join?

@udf('double')
def return_serverless_cost(etlRegion):
  if etlRegion in serverless_dbu_prices:
    return serverless_dbu_prices[etlRegion] * (100-serverlessDbuDiscountPercentage)/100
  else:
    return serverlessDbuPriceIfMissingRegion * (100-serverlessDbuDiscountPercentage)/100

# COMMAND ----------

@udf('double')
def compute_vm_cost(clusterDriverNodeType, clusterWorkerNodeType, clusterWorkers, etlRegion):
  clusterWorkers = 1 if clusterWorkers is None else clusterWorkers
  
  if etlRegion in vm_prices:
    vm_prices_per_region = vm_prices[etlRegion]
    if clusterDriverNodeType in vm_prices_per_region:
      return vm_prices_per_region[clusterDriverNodeType] * (100-vmDiscountPercentage)/100 + vm_prices_per_region[clusterWorkerNodeType] * (100-vmDiscountPercentage)/100 * clusterWorkers
    else:
        if etlRegion.contains('-'):
          return vm_prices_per_region['i3.8xlarge'] * (100-vmDiscountPercentage)/100 + vm_prices_per_region['i3.2xlarge'] * (100-vmDiscountPercentage)/100 * clusterWorkers
        else:
          return vm_prices_per_region['Standard_E32ds_v4'] * (100-vmDiscountPercentage)/100 + vm_prices_per_region['Standard_E8ds_v4'] * (100-vmDiscountPercentage)/100 * clusterWorkers
  else:
    vm_prices_per_region = vm_prices['us-east-1']
    if clusterDriverNodeType in vm_prices_per_region:
      return vm_prices_per_region[clusterDriverNodeType] * (100-vmDiscountPercentage)/100 + vm_prices_per_region[clusterWorkerNodeType] * (100-vmDiscountPercentage)/100 * clusterWorkers
    else:
        return vm_prices_per_region['i3.8xlarge'] * (100-vmDiscountPercentage)/100 + vm_prices_per_region['i3.2xlarge'] * (100-vmDiscountPercentage)/100 * clusterWorkers

# COMMAND ----------

from datetime import timedelta

def split_date(start, stop, date, endpointID):   
                                                                                    
    # Same day case                                                                                 
    if start.date() == stop.date():  
        return [(start.replace(year=1970, month=1, day=1), stop.replace(year=1970, month=1, day=1), date, endpointID)]                                                                      
                                                                                                                                                                                      
    # Several days split case                                                                       
    stop_split = start.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return [(start.replace(year=1970, month=1, day=1), stop_split.replace(year=1970, month=1, day=1, hour=23, minute=59, second=59, microsecond=999), date, endpointID)] + split_date(stop_split, stop, date + timedelta(days=1), endpointID)

# COMMAND ----------

def visualize_plot_warehouses(dataframe):
  new_dates = [
    elt for _, row in dataframe.select('queryStartDateTime', 'queryEndDateTimeWithAutostop', 'date', 'endpointID').toPandas().iterrows() for elt in split_date(row["queryStartDateTime"], row["queryEndDateTimeWithAutostop"], row["date"], row["endpointID"])
  ]      
  dataframe_serverless = pd.DataFrame(new_dates, columns=["queryStartDateTime", "queryEndDateTimeWithAutostop", "date", "endpointID"])

  fig = px.timeline(dataframe_serverless, x_start="queryStartDateTime", x_end="queryEndDateTimeWithAutostop", y="date", color="endpointID", opacity=0.5)
  fig.update_yaxes(autorange="reversed")
  fig.update_layout(
                    xaxis = dict(
                        title = 'Timestamp', 
                        tickformat = '%H:%M:%S',
                    ),
                    xaxis_range=['1970-01-01T00:00:00', '1970-01-01T23:59:59'],
                    yaxis = dict(
                        title = 'Date', 
                        tickformat = '%m-%d',
                    )
  )
  fig.show()

# COMMAND ----------

def visualize_plot_queries(dataframe):
  new_dates = [
    elt for _, row in dataframe.withColumn("date", col('queryStartDateTime').cast('date')).select('queryStartTimeDisplay', 'queryEndDateTimeWithAutostopDisplay', 'date', 'endpointID', 'clusterID', 'workspaceId').toPandas().iterrows() for elt in split_date(row["queryStartDateTime"], row["queryEndDateTimeWithAutostop"], row["date"], row["endpointID"])
  ]      
  dataframe_serverless = pd.DataFrame(new_dates, columns=["queryStartDateTime", "queryEndDateTimeWithAutostop", "date", "endpointID"])
  
  fig = px.timeline(dataframe_serverless, x_start="queryStartDateTime", x_end="queryEndDateTimeWithAutostop", y="date", color="endpointID", opacity=0.5)
  fig.update_yaxes(autorange="reversed")
  fig.update_layout(
                    xaxis = dict(
                        title = 'Timestamp', 
                        tickformat = '%H:%M:%S',
                    ),
                    xaxis_range=['1970-01-01T00:00:00', '1970-01-01T23:59:59'],
                    yaxis = dict(
                        title = 'Date', 
                        tickformat = '%m-%d',
                    )
  )
  fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Prepare Data

# COMMAND ----------

thrift_statements = (spark.read.table("prod.thrift_statements")
      .filter(col('date').between(startDate, endDate))
      .select('clusterID', 'workspaceId', 'queryDurationSeconds', 'fetchDurationSeconds', 'date', 'timestamp', 'thriftStatementId')
      .withColumnRenamed('timestamp', 'queryEndDateTime')
      .fillna(value=0, subset=["fetchDurationSeconds"])
)

# COMMAND ----------

cluster_endpoint_mapping = spark.read.table("prod_ds.cluster_endpoint_mapping")

# COMMAND ----------

workspaces = spark.read.table('prod.workspaces').select('workspaceId', 'canonicalCustomerName').filter(lower(col('canonicalCustomerName')) == customerName.lower())

# COMMAND ----------

if workspaces.rdd.isEmpty():
  raise Exception("Sorry, wrong customer name!")

# COMMAND ----------

workloads = spark.read.table('prod.workloads').select('date', 'approxDBUs', 'nodeHours', 'clusterId', 'clusterDriverNodeType', 'clusterWorkerNodeType', 'clusterWorkers', 'containerPricingUnits', 'etlRegion', 'clusterWorkers')

# COMMAND ----------

# List of queries for selected customer in selected date range

all_queries = (thrift_statements
             .join(cluster_endpoint_mapping.hint("broadcast"), [cluster_endpoint_mapping['clusterID'] == thrift_statements['clusterID'], cluster_endpoint_mapping['workspaceId'] == thrift_statements['workspaceId']])
             .join(workspaces.hint("broadcast"), [cluster_endpoint_mapping['workspaceId'] == workspaces['workspaceId']])
             .withColumn('queryStartDateTime', to_timestamp((((unix_timestamp('queryEndDateTime') + date_format(col("queryEndDateTime"), "SSS").cast('float') / 1000) * 1000) - thrift_statements['queryDurationSeconds'] * 1000 - thrift_statements['fetchDurationSeconds'] * 1000) / 1000)) \
             .select(thrift_statements['date'], 'queryStartDateTime', thrift_statements['queryEndDateTime'], thrift_statements['workspaceId'], cluster_endpoint_mapping['endpointID'], thrift_statements['clusterID'], thrift_statements['thriftStatementId'], thrift_statements['queryDurationSeconds'], thrift_statements['fetchDurationSeconds'])
             .drop('date')
             .orderBy('queryStartDateTime')
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Transform Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Classic

# COMMAND ----------

all_warehouses_grouped = all_queries \
  .groupBy('clusterID', 'endpointID', 'workspaceId') \
  .agg(
    min('queryStartDateTime').alias('queryStartDateTime'),
    max('queryEndDateTime').alias('queryEndDateTime')
  ) \
  .withColumn("date", col('queryStartDateTime').cast('date')) \
  .orderBy('queryStartDateTime')

# COMMAND ----------

all_queries_grouped_autostop_with_dbu_classic = all_warehouses_grouped.join(workloads, [workloads.date == all_warehouses_grouped.date, workloads.clusterId == all_warehouses_grouped.clusterID]) \
         .select(workloads.date, workloads.clusterId, workloads.clusterDriverNodeType, workloads.clusterWorkerNodeType, workloads.clusterWorkers, workloads.etlRegion, workloads.containerPricingUnits, all_warehouses_grouped.endpointID, all_warehouses_grouped.queryStartDateTime, all_warehouses_grouped.queryEndDateTime, workloads.nodeHours, workloads.approxDBUs, all_warehouses_grouped.workspaceId) \
         .withColumn('queryEndDateTimeWithAutostop', to_timestamp((((unix_timestamp('queryStartDateTime')) + workloads.nodeHours * 60 * 60))))\
         .groupBy('date', 'clusterID', 'endpointID', 'workspaceId', 'clusterDriverNodeType', 'clusterWorkerNodeType', 'etlRegion') \
         .agg(
            max('queryStartDateTime').alias('queryStartDateTime'),
            max('queryEndDateTime').alias('queryEndDateTime'),
            max('queryEndDateTimeWithAutostop').alias('queryEndDateTimeWithAutostop'),
            max('nodeHours').alias('nodeHours'),
            max('containerPricingUnits').alias('maxContainerPricingUnits'),
            max('clusterWorkers').alias('maxClusterWorkers')
          ) \
         .withColumn('totalDBUs', (col('maxContainerPricingUnits') + col('maxClusterWorkers') * 2) * col('nodeHours')) \
         .withColumn('totalDollarDBUs', col('totalDBUs') * costPerDbuClassic) \
         .withColumn('totalDollarVM', compute_vm_cost('clusterDriverNodeType', 'clusterWorkerNodeType', 'maxClusterWorkers', 'etlRegion') * col('nodeHours')) \
         .withColumn('totalDollar', col('totalDollarVM') + col('totalDollarDBUs')) \
         .cache()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Serverless

# COMMAND ----------

all_previous_rows_window = Window \
  .orderBy('queryStartDateTime') \
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# COMMAND ----------

all_queries_grouped_autostop_serverless = all_queries \
  .withColumn('max_previous_end', max('queryEndDateTime').over(all_previous_rows_window)) \
  .withColumn('interval_change', when(
    col('queryStartDateTime') - expr('INTERVAL {} SECONDS'.format(autoStopSeconds)) > lag('max_previous_end').over(Window.orderBy('queryStartDateTime')), 
    lit(1)
  ).otherwise(lit(0))) \
  .withColumn('interval_id', sum('interval_change').over(all_previous_rows_window)) \
  .drop('interval_change', 'max_previous_end') \
  .groupBy('interval_id', 'clusterID', 'endpointID', 'workspaceId') \
  .agg(
    min('queryStartDateTime').alias('queryStartDateTime'),
    max('queryEndDateTime').alias('queryEndDateTime')
  ) \
  .withColumn('queryEndDateTimeWithAutostop', to_timestamp(unix_timestamp('queryEndDateTime') + autoStopSeconds)) \
  .withColumn("date", col('queryStartDateTime').cast('date')) \
  .drop('interval_id')

# COMMAND ----------

all_queries_grouped_autostop_with_dbu_serverless = all_queries_grouped_autostop_serverless.join(workloads, [workloads.date == all_queries_grouped_autostop_serverless.date, workloads.clusterId == all_queries_grouped_autostop_serverless.clusterID]) \
         .select(workloads.date, workloads.clusterId, workloads.containerPricingUnits, workloads.etlRegion, workloads.clusterWorkers, all_queries_grouped_autostop_serverless.endpointID, all_queries_grouped_autostop_serverless.queryStartDateTime, all_queries_grouped_autostop_serverless.queryEndDateTimeWithAutostop, all_queries_grouped_autostop_serverless.workspaceId) \
         .groupBy('date', 'clusterID', 'endpointID', 'workspaceId', 'etlRegion', 'queryStartDateTime') \
         .agg(
            max('queryEndDateTimeWithAutostop').alias('queryEndDateTimeWithAutostop'),
            max('containerPricingUnits').alias('maxContainerPricingUnits'),
            max('clusterWorkers').alias('maxClusterWorkers')
          ) \
          .withColumn('totalDBUs', (col('maxContainerPricingUnits') + col('maxClusterWorkers') * 2) / (60 * 60) * (unix_timestamp('queryEndDateTimeWithAutostop') - unix_timestamp('queryStartDateTime'))) \
          .withColumn('totalDollarDBUs', col('totalDBUs') * return_serverless_cost('etlRegion')) \
          .cache()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Visualize Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Classic

# COMMAND ----------

display(all_queries_grouped_autostop_with_dbu_classic)

# COMMAND ----------

visualize_plot_warehouses(all_queries_grouped_autostop_with_dbu_classic)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Serverless

# COMMAND ----------

visualize_plot_warehouses(all_queries_grouped_autostop_with_dbu_serverless)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Debug

# COMMAND ----------

display(all_queries)

# COMMAND ----------

visualize_plot_queries(all_queries)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Results

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Classic

# COMMAND ----------

display(all_queries_grouped_autostop_with_dbu_classic)

# COMMAND ----------

results_classic = all_queries_grouped_autostop_with_dbu_classic.groupBy('endpointID').agg(sum('totalDollar').alias('totalDollarClassic'), sum('totalDollarDBUs').alias('totalDollarDBUs'), sum('totalDollarVM').alias('totalDollarVM')).orderBy('totalDollarClassic')

# COMMAND ----------

display(results_classic)

# COMMAND ----------

display(results_classic.groupBy().sum())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Serverless

# COMMAND ----------

display(all_queries_grouped_autostop_with_dbu_serverless)

# COMMAND ----------

results_serverless = all_queries_grouped_autostop_with_dbu_serverless.groupBy('endpointID').agg(sum('totalDollarDBUs').alias('totalDollarServerless')).orderBy('totalDollarServerless')

# COMMAND ----------

display(results_serverless)

# COMMAND ----------

display(results_serverless.groupBy().sum())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Comparison

# COMMAND ----------

#all_queries_grouped_autostop_with_dbu_classic_filtered = all_queries_grouped_autostop_with_dbu_classic.select(col('date').alias('date_classic'), col('clusterID').alias('clusterID_classic'), col('endpointID').alias('endpointID_classic'), col('workspaceId').alias('workspaceId_classic'), col('etlRegion').alias('etlRegion_classic'), col('totalDollarDBUs').alias('totalDollarDBUs_classic'), col('totalDollarVM').alias('totalDollarVM_classic'), col('totalDollar').alias('totalDollar_classic'))

# COMMAND ----------

#all_queries_grouped_autostop_with_dbu_serverless_filtered = all_queries_grouped_autostop_with_dbu_serverless.select(col('date').alias('date_serverless'), col('clusterID').alias('clusterID_serverless'), col('endpointID').alias('endpointID_serverless'), col('workspaceId').alias('workspaceId_serverless'), col('etlRegion').alias('etlRegion_serverless'), col('totalDBUs').alias('totalDBUs_serverless'), col('totalDollarDBUs').alias('totalDollarDBUs_serverless'))

# COMMAND ----------

#all_queries_comparison =  all_queries_grouped_autostop_with_dbu_classic_filtered.join(all_queries_grouped_autostop_with_dbu_serverless_filtered, [all_queries_grouped_autostop_with_dbu_classic_filtered.date_classic == all_queries_grouped_autostop_with_dbu_serverless_filtered.date_serverless, all_queries_grouped_autostop_with_dbu_classic_filtered.clusterID_classic == all_queries_grouped_autostop_with_dbu_serverless_filtered.clusterID_serverless])

# COMMAND ----------

#display(all_queries_comparison)

# COMMAND ----------

print(start_time)
print(datetime.now())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Next steps
# MAGIC 
# MAGIC - Add autoscaling (I believe it is already added for Classic, need to add it for Serverless)
# MAGIC - Understand why NoneType can be present in compute_vm_cost
# MAGIC - Is cluster restart at midnight an issue?
# MAGIC - What happens if a region is not available with serverless?
# MAGIC - Exclude serverless SQL queries
# MAGIC - Same clusterID on different days? 0101-011330-4vkdywl0 2022-01-01
# MAGIC - Same colors per endpoint/workspace across graphs?
# MAGIC - Performance Optimization (disk spill)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Known issues
# MAGIC 
# MAGIC - Visualization of queries than span multiple days does not work. E.g., StartTime = 2022-07-03T05:04:34.006+0000,  EndTime = 2022-07-04T06:19:04.000+0000
