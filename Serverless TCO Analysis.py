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
vm_prices['westeurope']['ondemand'] = {}
vm_prices['westeurope']['ondemand']['Standard_E8ds_v4'] = 0.692
vm_prices['westeurope']['ondemand']['Standard_E16ds_v4'] = 1.384
vm_prices['westeurope']['ondemand']['Standard_E32ds_v4'] = 2.768
vm_prices['westeurope']['ondemand']['Standard_E64ds_v4'] = 5.536
vm_prices['westeurope']['spot'] = {}
vm_prices['westeurope']['spot']['Standard_E8ds_v4'] = 0.1025
vm_prices['westeurope']['spot']['Standard_E16ds_v4'] = 0.2050
vm_prices['westeurope']['spot']['Standard_E32ds_v4'] = 0.4099
vm_prices['westeurope']['spot']['Standard_E64ds_v4'] = 0.8199
vm_prices['northeurope'] = {}
vm_prices['northeurope']['ondemand'] = {}
vm_prices['northeurope']['ondemand']['Standard_E8ds_v4'] = 0.640
vm_prices['northeurope']['ondemand']['Standard_E16ds_v4'] = 1.280
vm_prices['northeurope']['ondemand']['Standard_E32ds_v4'] = 2.560
vm_prices['northeurope']['ondemand']['Standard_E64ds_v4'] = 5.120
vm_prices['northeurope']['spot'] = {}
vm_prices['northeurope']['spot']['Standard_E8ds_v4'] = 0.2179
vm_prices['northeurope']['spot']['Standard_E16ds_v4'] = 0.4358
vm_prices['northeurope']['spot']['Standard_E32ds_v4'] = 0.8717
vm_prices['northeurope']['spot']['Standard_E64ds_v4'] = 1.7434
vm_prices['westus'] = {}
vm_prices['westus']['ondemand'] = {}
vm_prices['westus']['ondemand']['Standard_E8ds_v4'] = 0.648
vm_prices['westus']['ondemand']['Standard_E16ds_v4'] = 1.296
vm_prices['westus']['ondemand']['Standard_E32ds_v4'] = 2.592
vm_prices['westus']['ondemand']['Standard_E64ds_v4'] = 5.184
vm_prices['westus']['spot'] = {}
vm_prices['westus']['spot']['Standard_E8ds_v4'] = 0.0734
vm_prices['westus']['spot']['Standard_E16ds_v4'] = 0.1467
vm_prices['westus']['spot']['Standard_E32ds_v4'] = 0.2934
vm_prices['westus']['spot']['Standard_E64ds_v4'] = 0.5868
vm_prices['eastus2'] = {}
vm_prices['eastus2']['ondemand'] = {}
vm_prices['eastus2']['ondemand']['Standard_E8ds_v4'] = 0.576
vm_prices['eastus2']['ondemand']['Standard_E16ds_v4'] = 1.134
vm_prices['eastus2']['ondemand']['Standard_E32ds_v4'] = 2.268
vm_prices['eastus2']['ondemand']['Standard_E64ds_v4'] = 4.608
vm_prices['eastus2']['spot'] = {}
vm_prices['eastus2']['spot']['Standard_E8ds_v4'] = 0.2303
vm_prices['eastus2']['spot']['Standard_E16ds_v4'] = 0.4607
vm_prices['eastus2']['spot']['Standard_E32ds_v4'] = 0.9214
vm_prices['eastus2']['spot']['Standard_E64ds_v4'] = 1.8427
vm_prices['eastus'] = {}
vm_prices['eastus']['ondemand'] = {}
vm_prices['eastus']['ondemand']['Standard_E8ds_v4'] = 0.576
vm_prices['eastus']['ondemand']['Standard_E16ds_v4'] = 1.134
vm_prices['eastus']['ondemand']['Standard_E32ds_v4'] = 2.268
vm_prices['eastus']['ondemand']['Standard_E64ds_v4'] = 4.608
vm_prices['eastus']['spot'] = {}
vm_prices['eastus']['spot']['Standard_E8ds_v4'] = 0.2303
vm_prices['eastus']['spot']['Standard_E16ds_v4'] = 0.4606
vm_prices['eastus']['spot']['Standard_E32ds_v4'] = 0.9211
vm_prices['eastus']['spot']['Standard_E64ds_v4'] = 1.8423

vm_prices['us-east-1'] = {}
vm_prices['us-east-1']['ondemand'] = {}
vm_prices['us-east-1']['ondemand']['i3.2xlarge'] = 0.624
vm_prices['us-east-1']['ondemand']['i3.4xlarge'] = 1.248
vm_prices['us-east-1']['ondemand']['i3.8xlarge'] = 2.496
vm_prices['us-east-1']['ondemand']['i3.16xlarge'] = 4.992
vm_prices['us-east-1']['spot'] = {}
vm_prices['us-east-1']['spot']['i3.2xlarge'] = 0.26832
vm_prices['us-east-1']['spot']['i3.4xlarge'] = 0.53664
vm_prices['us-east-1']['spot']['i3.8xlarge'] = 0.7488
vm_prices['us-east-1']['spot']['i3.16xlarge'] = 1.54752
vm_prices['us-east-2'] = {}
vm_prices['us-east-2']['ondemand'] = {}
vm_prices['us-east-2']['ondemand']['i3.2xlarge'] = 0.624
vm_prices['us-east-2']['ondemand']['i3.4xlarge'] = 1.248
vm_prices['us-east-2']['ondemand']['i3.8xlarge'] = 2.496
vm_prices['us-east-2']['ondemand']['i3.16xlarge'] = 4.992
vm_prices['us-east-2']['spot'] = {}
vm_prices['us-east-2']['spot']['i3.2xlarge'] = 0.1872
vm_prices['us-east-2']['spot']['i3.4xlarge'] = 0.3744
vm_prices['us-east-2']['spot']['i3.8xlarge'] = 0.7488
vm_prices['us-east-2']['spot']['i3.16xlarge'] = 1.4976
vm_prices['us-west-2'] = {}
vm_prices['us-west-2']['ondemand'] = {}
vm_prices['us-west-2']['ondemand']['i3.2xlarge'] = 0.624
vm_prices['us-west-2']['ondemand']['i3.4xlarge'] = 1.248
vm_prices['us-west-2']['ondemand']['i3.8xlarge'] = 2.496
vm_prices['us-west-2']['ondemand']['i3.16xlarge'] = 4.992
vm_prices['us-west-2']['spot'] = {}
vm_prices['us-west-2']['spot']['i3.2xlarge'] = 0.2496
vm_prices['us-west-2']['spot']['i3.4xlarge'] = 0.47424
vm_prices['us-west-2']['spot']['i3.8xlarge'] = 0.92352
vm_prices['us-west-2']['spot']['i3.16xlarge'] = 1.69728
vm_prices['eu-central-1'] = {}
vm_prices['eu-central-1']['ondemand'] = {}
vm_prices['eu-central-1']['ondemand']['i3.2xlarge'] = 0.744
vm_prices['eu-central-1']['ondemand']['i3.4xlarge'] = 1.488
vm_prices['eu-central-1']['ondemand']['i3.8xlarge'] = 2.976
vm_prices['eu-central-1']['ondemand']['i3.16xlarge'] = 5.952
vm_prices['eu-central-1']['spot'] = {}
vm_prices['eu-central-1']['spot']['i3.2xlarge'] = 0.27528
vm_prices['eu-central-1']['spot']['i3.4xlarge'] = 0.5208
vm_prices['eu-central-1']['spot']['i3.8xlarge'] = 0.8928
vm_prices['eu-central-1']['spot']['i3.16xlarge'] = 1.7856
vm_prices['eu-west-1'] = {}
vm_prices['eu-west-1']['ondemand'] = {}
vm_prices['eu-west-1']['ondemand']['i3.2xlarge'] = 0.688
vm_prices['eu-west-1']['ondemand']['i3.4xlarge'] = 1.376
vm_prices['eu-west-1']['ondemand']['i3.8xlarge'] = 2.752
vm_prices['eu-west-1']['ondemand']['i3.16xlarge'] = 5.504
vm_prices['eu-west-1']['spot'] = {}
vm_prices['eu-west-1']['spot']['i3.2xlarge'] = 0.28896
vm_prices['eu-west-1']['spot']['i3.4xlarge'] = 0.50912
vm_prices['eu-west-1']['spot']['i3.8xlarge'] = 0.8256
vm_prices['eu-west-1']['spot']['i3.16xlarge'] = 1.6512
vm_prices['ap-southeast-2'] = {}
vm_prices['ap-southeast-2']['ondemand'] = {}
vm_prices['ap-southeast-2']['ondemand']['i3.2xlarge'] = 0.748
vm_prices['ap-southeast-2']['ondemand']['i3.4xlarge'] = 1.496
vm_prices['ap-southeast-2']['ondemand']['i3.8xlarge'] = 2.992
vm_prices['ap-southeast-2']['ondemand']['i3.16xlarge'] = 5.984
vm_prices['ap-southeast-2']['spot'] = {}
vm_prices['ap-southeast-2']['spot']['i3.2xlarge'] = 0.24684
vm_prices['ap-southeast-2']['spot']['i3.4xlarge'] = 0.50864
vm_prices['ap-southeast-2']['spot']['i3.8xlarge'] = 1.0472
vm_prices['ap-southeast-2']['spot']['i3.16xlarge'] = 1.7952

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
def compute_vm_cost(clusterDriverNodeType, clusterWorkerNodeType, clusterWorkers, etlRegion, containerIsSpot, callerFunction):
  clusterWorkers = 1 if clusterWorkers is None else clusterWorkers
  
  isSpotOrOnDemand = ''
  if containerIsSpot:
    if callerFunction == 'spot':
      isSpotOrOnDemand = 'spot'
    else:
      isSpotOrOnDemand = None
  else:
    if callerFunction == 'ondemand':
      isSpotOrOnDemand = 'ondemand'
    else:
      isSpotOrOnDemand = None
  
  if isSpotOrOnDemand is not None:
    if etlRegion in vm_prices:
      vm_prices_per_region = vm_prices[etlRegion][isSpotOrOnDemand]
      if clusterDriverNodeType in vm_prices_per_region:
        return vm_prices_per_region[clusterDriverNodeType] * (100-vmDiscountPercentage)/100 + vm_prices_per_region[clusterWorkerNodeType] * (100-vmDiscountPercentage)/100 * clusterWorkers
      else:
          if etlRegion.contains('-'):
            return vm_prices_per_region['i3.8xlarge'] * (100-vmDiscountPercentage)/100 + vm_prices_per_region['i3.2xlarge'] * (100-vmDiscountPercentage)/100 * clusterWorkers
          else:
            return vm_prices_per_region['Standard_E32ds_v4'] * (100-vmDiscountPercentage)/100 + vm_prices_per_region['Standard_E8ds_v4'] * (100-vmDiscountPercentage)/100 * clusterWorkers
    else:
      vm_prices_per_region = vm_prices['us-east-1'][isSpotOrOnDemand]
      if clusterDriverNodeType in vm_prices_per_region:
        return vm_prices_per_region[clusterDriverNodeType] * (100-vmDiscountPercentage)/100 + vm_prices_per_region[clusterWorkerNodeType] * (100-vmDiscountPercentage)/100 * clusterWorkers
      else:
          return vm_prices_per_region['i3.8xlarge'] * (100-vmDiscountPercentage)/100 + vm_prices_per_region['i3.2xlarge'] * (100-vmDiscountPercentage)/100 * clusterWorkers
  else:
    return 0.0      

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
  
  dataframe_visualize = dataframe \
         .withColumn('queryStartDateTimeDisplay', concat(lit('1970-01-01T'), date_format('queryStartDateTime', 'HH:mm:ss').cast('string'))) \
         .withColumn('queryEndDateTimeWithAutostopDisplay', concat(lit('1970-01-01T'), date_format('queryEndDateTimeWithAutostop', 'HH:mm:ss').cast('string'))) \
         .toPandas()
  
  display(dataframe_visualize)
  
  fig = px.timeline(dataframe_visualize, x_start="queryStartDateTimeDisplay", x_end="queryEndDateTimeWithAutostopDisplay", y="date", color="endpointID", opacity=0.5)
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
    elt for _, row in dataframe.withColumn("date", col('queryStartDateTime').cast('date')).select('queryStartDateTime', 'queryEndDateTime', 'date', 'endpointID', 'clusterID', 'workspaceId').toPandas().iterrows() for elt in split_date(row["queryStartDateTime"], row["queryEndDateTime"], row["date"], row["endpointID"])
  ]      
  dataframe_serverless = pd.DataFrame(new_dates, columns=["queryStartDateTime", "queryEndDateTime", "date", "endpointID"])
  
  fig = px.timeline(dataframe_serverless, x_start="queryStartDateTime", x_end="queryEndDateTime", y="date", color="endpointID", opacity=0.5)
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

thrift_statements = spark.read.table("prod.thrift_statements") \
                  .filter(col('date').between(startDate, endDate)) \
                  .filter(col('queryStatus') == 'finish') \
                  .select('clusterID', 'workspaceId', 'queryDurationSeconds', 'fetchDurationSeconds', 'date', 'timestamp', 'thriftStatementId') \
                  .withColumnRenamed('timestamp', 'queryEndDateTime') \
                  .fillna(value=0, subset=["fetchDurationSeconds"])

# COMMAND ----------

cluster_endpoint_mapping = spark.read.table("prod_ds.cluster_endpoint_mapping")

# COMMAND ----------

usage_logs = spark.read.table('prod_ds.usage_logs_optimized') \
                  .filter(col('date').between(startDate, endDate)) \
                  .filter(col('metric') == 'clusterManagerEvent') \
                  .filter(col('tags.clusterManagerEvent').isin(["createClusterByNephosPool", "createCluster"])) \
                  .withColumn('clusterId', col('tags.clusterId')) \
                  .withColumn('isNephos', col('tags.isNephos')) \
                  .select('clusterId', 'isNephos') \
                  .fillna(value='false', subset=["isNephos"])

# COMMAND ----------

workspaces = spark.read.table('prod.workspaces') \
                  .filter(lower(col('canonicalCustomerName')) == customerName.lower()) \
                  .select('workspaceId')

# COMMAND ----------

if workspaces.rdd.isEmpty():
  raise Exception("Sorry, wrong customer name!")

# COMMAND ----------

workloads = spark.read.table('prod.workloads') \
                  .select('date', 'approxDBUs', 'nodeHours', 'clusterId', 'clusterDriverNodeType', 'clusterWorkerNodeType', 'clusterWorkers', 'containerPricingUnits', 'etlRegion', 'clusterWorkers', 'containerIsSpot')

# COMMAND ----------

rich_workspaces = workspaces \
                  .join(cluster_endpoint_mapping, [cluster_endpoint_mapping.workspaceId == workspaces.workspaceId]).drop(cluster_endpoint_mapping.workspaceId)

# COMMAND ----------

#Exclude Serverless queries from the analysis

usage_logs_filtered = usage_logs \
                    .join(rich_workspaces, [rich_workspaces.clusterId == usage_logs.clusterId]) \
                    .filter(col('isNephos') == 'false')

# COMMAND ----------

# List of queries for selected customer in selected date range

all_queries = (thrift_statements
                   .join(usage_logs_filtered.hint("broadcast"), [cluster_endpoint_mapping.clusterId == thrift_statements['clusterID']])
                   .withColumn('queryStartDateTime', to_timestamp((((unix_timestamp('queryEndDateTime') + date_format(col("queryEndDateTime"), "SSS").cast('float') / 1000) * 1000) - thrift_statements['queryDurationSeconds'] * 1000 - thrift_statements['fetchDurationSeconds'] * 1000) / 1000)) \
                   .select(thrift_statements['date'], 'queryStartDateTime', thrift_statements['queryEndDateTime'], thrift_statements['workspaceId'], cluster_endpoint_mapping['endpointID'], thrift_statements['clusterID'], thrift_statements['thriftStatementId'], thrift_statements['queryDurationSeconds'], thrift_statements['fetchDurationSeconds'])
                   .drop('date')
                   .orderBy('queryStartDateTime')
                   .cache()
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
                    .withColumn("startDate", col('queryStartDateTime').cast('date')) \
                    .withColumn("endDate", col('queryEndDateTime').cast('date')) \
                    .withColumn('days', expr('sequence(startDate, endDate, interval 1 day)')) \
                    .withColumn('date', explode('days')) \
                    .withColumn('numberOfDays', size("days")) \
                    .withColumn('index', row_number().over(Window.partitionBy('clusterID').orderBy('clusterID'))) \
                    .withColumn('queryStartDateTime', when((col('startDate') == col('date')) & (col('numberOfDays') == 1), col('queryStartDateTime')).otherwise(when((col('startDate') == col('date')) & (col('numberOfDays') != 1), col('queryStartDateTime')).otherwise(to_timestamp(col('days')[col('index') - lit(1)])))) \
                    .withColumn('queryEndDateTime', when((col('startDate') == col('date')) & (col('numberOfDays') == 1), col('queryEndDateTime')).otherwise(when((col('endDate') == col('date')) & (col('numberOfDays') != 1), col('queryEndDateTime')).otherwise(from_unixtime((round(unix_timestamp('queryStartDateTime') / lit(86400)) * lit(86400)) + lit(86400) - lit(1)))).cast('timestamp')) \
                    .drop('days', 'numberOfDays', 'index', 'startDate', 'endDate') \
                    .orderBy('queryStartDateTime')

# COMMAND ----------

all_queries_grouped_autostop_with_dbu_classic = all_warehouses_grouped.join(workloads, [workloads.date == all_warehouses_grouped.date, workloads.clusterId == all_warehouses_grouped.clusterID]) \
                   .select(workloads.date, workloads.clusterId, workloads.clusterDriverNodeType, workloads.clusterWorkerNodeType, workloads.clusterWorkers, workloads.etlRegion, workloads.containerPricingUnits, workloads.containerIsSpot, all_warehouses_grouped.endpointID, all_warehouses_grouped.queryStartDateTime, all_warehouses_grouped.queryEndDateTime, workloads.nodeHours, workloads.approxDBUs, all_warehouses_grouped.workspaceId) \
                   .withColumn('queryEndDateTimeWithAutostop', to_timestamp((((unix_timestamp('queryStartDateTime')) + workloads.nodeHours * 60 * 60)))) \
                   .groupBy('date', 'clusterID', 'endpointID', 'workspaceId', 'clusterDriverNodeType', 'clusterWorkerNodeType', 'etlRegion') \
                   .agg(
                      max('queryStartDateTime').alias('queryStartDateTime'),
                      max('queryEndDateTimeWithAutostop').alias('queryEndDateTimeWithAutostop'),
                      max('containerPricingUnits').alias('maxContainerPricingUnits'),
                      max('clusterWorkers').alias('maxClusterWorkers'),
                      first('containerIsSpot').alias('containerIsSpot')
                    ) \
                   .withColumn("startDate", col('queryStartDateTime').cast('date')) \
                   .withColumn("endDate", col('queryEndDateTimeWithAutostop').cast('date')) \
                   .withColumn('days', expr('sequence(startDate, endDate, interval 1 day)')) \
                   .withColumn('date', explode('days')) \
                   .withColumn('numberOfDays', size("days")) \
                   .withColumn('index', row_number().over(Window.partitionBy('clusterID').orderBy('clusterID'))) \
                   .withColumn('queryStartDateTime', when((col('startDate') == col('date')) & (col('numberOfDays') == 1), col('queryStartDateTime')).otherwise(when((col('startDate') == col('date')) & (col('numberOfDays') != 1), col('queryStartDateTime')).otherwise(to_timestamp(col('days')[col('index') - lit(1)])))) \
                   .withColumn('queryEndDateTimeWithAutostop', when((col('startDate') == col('date')) & (col('numberOfDays') == 1), col('queryEndDateTimeWithAutostop')).otherwise(when((col('endDate') == col('date')) & (col('numberOfDays') != 1), col('queryEndDateTimeWithAutostop')).otherwise(from_unixtime((round(unix_timestamp('queryEndDateTimeWithAutostop') / lit(86400)) * lit(86400)) + lit(86400) - lit(1)))).cast('timestamp')) \
                   .withColumn('nodeHours', (unix_timestamp('queryEndDateTimeWithAutostop') - unix_timestamp('queryStartDateTime')) / 60 / 60) \
                   .withColumn('totalDBUs', (col('maxContainerPricingUnits') + col('maxClusterWorkers') * 2) * col('nodeHours')) \
                   .withColumn('totalDollarDBUs', col('totalDBUs') * costPerDbuClassic) \
                   .withColumn('totalDollarVMOndemand', compute_vm_cost('clusterDriverNodeType', 'clusterWorkerNodeType', 'maxClusterWorkers', 'etlRegion', 'containerIsSpot', lit('ondemand')) * col('nodeHours')) \
                   .withColumn('totalDollarVMSpot', compute_vm_cost('clusterDriverNodeType', 'clusterWorkerNodeType', 'maxClusterWorkers', 'etlRegion', 'containerIsSpot', lit('spot')) * col('nodeHours')) \
                   .withColumn('totalDollar', col('totalDollarVMOndemand') + col('totalDollarVMSpot') + col('totalDollarDBUs')) \
                   .drop('days', 'numberOfDays', 'index', 'startDate', 'endDate') \
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

display(all_queries_grouped_autostop_with_dbu_serverless)

# COMMAND ----------

visualize_plot_warehouses(all_queries_grouped_autostop_with_dbu_serverless)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Debug

# COMMAND ----------

#display(all_queries)

# COMMAND ----------

#visualize_plot_queries(all_queries)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Results

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Classic

# COMMAND ----------

results_classic = all_queries_grouped_autostop_with_dbu_classic.groupBy('endpointID').agg(sum('totalDollar').alias('totalDollarClassic'), sum('totalDollarDBUs').alias('totalDollarDBUs'), sum('totalDollarVMOndemand').alias('totalDollarVMOndemand'), sum('totalDollarVMSpot').alias('totalDollarVMSpot')).orderBy('totalDollarClassic')

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
# MAGIC - Exclude serverless SQL queries
# MAGIC - Same colors per endpoint/workspace across graphs?
# MAGIC - Performance Optimization (disk spill)
# MAGIC - Add .999 to every time when splitting rows at midnight

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Known issues
