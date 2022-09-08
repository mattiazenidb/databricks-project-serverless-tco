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
costPerDbuClassic = float(dbutils.widgets.get('costPerDbuClassic'))
vmDiscountPercentage = int(dbutils.widgets.get('vmDiscountPercentage'))
serverlessDbuDiscountPercentage = int(dbutils.widgets.get('serverlessDbuDiscountPercentage'))
autoStopSeconds = int(dbutils.widgets.get('autoStopSeconds'))
serverlessDbuPriceIfMissingRegion = float(dbutils.widgets.get('serverlessDbuPriceIfMissingRegion'))
region = dbutils.widgets.get('region')
topXCustomers = int(dbutils.widgets.get('topXCustomers'))

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
vm_prices['westeurope']['Standard_E8_v3'] = 0.640
vm_prices['westeurope']['Standard_E16_v3'] = 1.280
vm_prices['westeurope']['Standard_E32_v3'] = 2.560
vm_prices['westeurope']['Standard_E64_v3'] = 4.376
vm_prices['northeurope'] = {}
vm_prices['northeurope']['Standard_E8ds_v4'] = 0.640
vm_prices['northeurope']['Standard_E16ds_v4'] = 1.280
vm_prices['northeurope']['Standard_E32ds_v4'] = 2.560
vm_prices['northeurope']['Standard_E64ds_v4'] = 5.120
vm_prices['northeurope']['Standard_E8_v3'] = 0.564
vm_prices['northeurope']['Standard_E16_v3'] = 1.128
vm_prices['northeurope']['Standard_E32_v3'] = 2.256
vm_prices['northeurope']['Standard_E64_v3'] = 4.061
vm_prices['westus'] = {}
vm_prices['westus']['Standard_E8ds_v4'] = 0.648
vm_prices['westus']['Standard_E16ds_v4'] = 1.296
vm_prices['westus']['Standard_E32ds_v4'] = 2.592
vm_prices['westus']['Standard_E64ds_v4'] = 5.184
vm_prices['westus']['Standard_E8_v3'] = 0.560
vm_prices['westus']['Standard_E16_v3'] = 1.120
vm_prices['westus']['Standard_E32_v3'] = 2.240
vm_prices['westus']['Standard_E64_v3'] = 4.032
vm_prices['eastus2'] = {}
vm_prices['eastus2']['Standard_E8ds_v4'] = 0.576
vm_prices['eastus2']['Standard_E16ds_v4'] = 1.134
vm_prices['eastus2']['Standard_E32ds_v4'] = 2.268
vm_prices['eastus2']['Standard_E64ds_v4'] = 4.608
vm_prices['eastus2']['Standard_E8_v3'] = 0.532
vm_prices['eastus2']['Standard_E16_v3'] = 1.064
vm_prices['eastus2']['Standard_E32_v3'] = 2.128
vm_prices['eastus2']['Standard_E64_v3'] = 3.629
vm_prices['eastus'] = {}
vm_prices['eastus']['Standard_E8ds_v4'] = 0.576
vm_prices['eastus']['Standard_E16ds_v4'] = 1.134
vm_prices['eastus']['Standard_E32ds_v4'] = 2.268
vm_prices['eastus']['Standard_E64ds_v4'] = 4.608
vm_prices['eastus']['Standard_E8_v3'] = 0.504
vm_prices['eastus']['Standard_E16_v3'] = 1.008
vm_prices['eastus']['Standard_E32_v3'] = 2.016
vm_prices['eastus']['Standard_E64_v3'] = 3.629

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

accounts = spark.read.table('finance.dbu_dollars') \
              .filter(col('date').between(startDate, endDate)) \
              .filter(col('sfdc_region_l2').contains(region)) \
              .filter(lower('sku').contains('sql')) \
              .groupBy('accountName') \
              .agg(max('sku').alias('skus'),
                   sum('DBU').alias('dbus')) \
              .orderBy(col('dbus').desc()) \
              .drop('skus', 'dbus') \
              .limit(topXCustomers)

# COMMAND ----------

thrift_statements = (spark.read.table("prod.thrift_statements")
      .filter(col('date').between(startDate, endDate))
      .select('clusterID', 'workspaceId', 'queryDurationSeconds', 'fetchDurationSeconds', 'date', 'timestamp', 'thriftStatementId')
      .withColumnRenamed('timestamp', 'queryEndDateTime')
      .fillna(value = 0, subset=["fetchDurationSeconds"])
)

# COMMAND ----------

cluster_endpoint_mapping = spark.read.table("prod_ds.cluster_endpoint_mapping")

# COMMAND ----------

workspaces = spark.read.table('prod.workspaces') \
              .select('workspaceId', 'canonicalCustomerName') \
              .join(accounts.hint('broadcast'), lower(col('canonicalCustomerName')) == lower(accounts.accountName)) \
              .drop('accountName')

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
             .select(thrift_statements['date'], 'queryStartDateTime', thrift_statements['queryEndDateTime'], thrift_statements['workspaceId'], cluster_endpoint_mapping['endpointID'], thrift_statements['clusterID'], thrift_statements['thriftStatementId'], thrift_statements['queryDurationSeconds'], thrift_statements['fetchDurationSeconds'], workspaces['canonicalCustomerName'])
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
  .groupBy('clusterID', 'endpointID', 'workspaceId', 'canonicalCustomerName') \
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
  .withColumn('queryStartDateTime', when((col('startDate') == col('date')) & (col('numberOfDays') == 1), col('queryStartDateTime')).otherwise(when((col('startDate') == col('date')) & (col('numberOfDays') != 1), col('queryStartDateTime')).otherwise(date_add(from_unixtime(round(unix_timestamp('queryStartDateTime') / lit(86400)) * lit(86400)), col('index') - 1)))) \
  .withColumn('queryEndDateTime', when((col('startDate') == col('date')) & (col('numberOfDays') == 1), col('queryEndDateTime')).otherwise(when((col('endDate') == col('date')) & (col('numberOfDays') != 1), col('queryEndDateTime')).otherwise(from_unixtime((round(unix_timestamp('queryStartDateTime') / lit(86400)) * lit(86400)) + lit(86400) - lit(1)))).cast('timestamp')) \
  .drop('days', 'numberOfDays', 'index', 'startDate', 'endDate') \
  .orderBy('queryStartDateTime')

# COMMAND ----------

all_queries_grouped_autostop_with_dbu_classic = all_warehouses_grouped.join(workloads, [workloads.date == all_warehouses_grouped.date, workloads.clusterId == all_warehouses_grouped.clusterID]) \
         .select(workloads.date, workloads.clusterId, workloads.clusterDriverNodeType, workloads.clusterWorkerNodeType, workloads.clusterWorkers, workloads.etlRegion, workloads.containerPricingUnits, all_warehouses_grouped.endpointID, all_warehouses_grouped.queryStartDateTime, all_warehouses_grouped.queryEndDateTime, workloads.nodeHours, workloads.approxDBUs, all_warehouses_grouped.workspaceId, all_warehouses_grouped.canonicalCustomerName) \
         .withColumn('queryEndDateTimeWithAutostop', to_timestamp((((unix_timestamp('queryStartDateTime')) + workloads.nodeHours * 60 * 60))))\
         .groupBy('date', 'clusterID', 'endpointID', 'workspaceId', 'canonicalCustomerName', 'clusterDriverNodeType', 'clusterWorkerNodeType', 'etlRegion') \
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
  .select('clusterID', 'endpointID', 'workspaceId', 'canonicalCustomerName', 'queryEndDateTime', 'queryStartDateTime') \
  .withColumn('max_previous_end', max('queryEndDateTime').over(all_previous_rows_window)) \
  .withColumn('interval_change', when(
    col('queryStartDateTime') - expr('INTERVAL {} SECONDS'.format(autoStopSeconds)) > lag('max_previous_end').over(Window.orderBy('queryStartDateTime')), 
    lit(1)
  ).otherwise(lit(0))) \
  .withColumn('interval_id', sum('interval_change').over(all_previous_rows_window)) \
  .drop('interval_change', 'max_previous_end') \
  .groupBy('interval_id', 'clusterID', 'endpointID', 'workspaceId', 'canonicalCustomerName') \
  .agg(
    min('queryStartDateTime').alias('queryStartDateTime'),
    max('queryEndDateTime').alias('queryEndDateTime')
  ) \
  .withColumn('queryEndDateTimeWithAutostop', to_timestamp(unix_timestamp('queryEndDateTime') + autoStopSeconds)) \
  .withColumn("date", col('queryStartDateTime').cast('date')) \
  .withColumn('queryStartTimeDisplay', concat(lit('1970-01-01T'), date_format('queryStartDateTime', 'HH:mm:ss').cast('string'))) \
  .drop('interval_id')

# COMMAND ----------

all_queries_grouped_autostop_with_dbu_serverless = all_queries_grouped_autostop_serverless.join(workloads, [workloads.date == all_queries_grouped_autostop_serverless.date, workloads.clusterId == all_queries_grouped_autostop_serverless.clusterID]) \
         .select(workloads.date, workloads.clusterId, workloads.containerPricingUnits, workloads.etlRegion, workloads.clusterWorkers, all_queries_grouped_autostop_serverless.endpointID, all_queries_grouped_autostop_serverless.queryStartDateTime, all_queries_grouped_autostop_serverless.queryEndDateTimeWithAutostop, all_queries_grouped_autostop_serverless.workspaceId, all_queries_grouped_autostop_serverless.canonicalCustomerName) \
         .groupBy('date', 'clusterID', 'endpointID', 'workspaceId', 'canonicalCustomerName', 'etlRegion', all_queries_grouped_autostop_serverless.queryStartDateTime) \
         .agg(
            max(all_queries_grouped_autostop_serverless.queryStartDateTime).alias('queryStartDateTime'),
            max('queryEndDateTimeWithAutostop').alias('queryEndDateTimeWithAutostop'),
            max('containerPricingUnits').alias('maxContainerPricingUnits'),
            max('clusterWorkers').alias('maxClusterWorkers')
          ) \
          .withColumn('totalDBUs', (col('maxContainerPricingUnits') + col('maxClusterWorkers') * 2) / (60 * 60) * (unix_timestamp('queryEndDateTimeWithAutostop') - unix_timestamp(all_queries_grouped_autostop_serverless.queryStartDateTime))) \
          .withColumn('totalDollarDBUs', col('totalDBUs') * return_serverless_cost('etlRegion')) \
          .cache()

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

results_classic = all_queries_grouped_autostop_with_dbu_classic.groupBy('canonicalCustomerName').agg(sum('totalDollar').alias('totalDollarClassic'), sum('totalDollarDBUs').alias('totalDollarDBUs'), sum('totalDollarVM').alias('totalDollarVM')).orderBy('totalDollarClassic')

# COMMAND ----------

display(results_classic)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Serveless

# COMMAND ----------

display(all_queries_grouped_autostop_with_dbu_serverless)

# COMMAND ----------

results_serverless = all_queries_grouped_autostop_with_dbu_serverless.groupBy('canonicalCustomerName').agg(sum('totalDollarDBUs').alias('totalDollarServerless')).orderBy('totalDollarServerless')

# COMMAND ----------

display(results_serverless)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Comparison

# COMMAND ----------

results = results_serverless.join(results_classic, results_classic.canonicalCustomerName == results_serverless.canonicalCustomerName).drop(results_classic.canonicalCustomerName).withColumn('accountTeamTCOVariation', col('totalDollarServerless') - col('totalDollarDBUs')).withColumn('accountTCOVariation', col('totalDollarServerless') - col('totalDollarClassic')).withColumn('accountTCOVariationPercentage', col('accountTCOVariation') / col('totalDollarClassic') * 100)

# COMMAND ----------

display(results)

# COMMAND ----------

print(start_time)
print(datetime.now())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Next steps
# MAGIC 
# MAGIC - Add autoscaling
# MAGIC - Understand why NoneType can be present in compute_vm_cost
# MAGIC - Is cluster restart at midnight an issue?
# MAGIC - Same colors per endpoint/workspace across graphs?
# MAGIC - What happens if a region is not available with serverless?
# MAGIC - Exclude serverless SQL queries
# MAGIC - Performance Optimization (disk spill)
