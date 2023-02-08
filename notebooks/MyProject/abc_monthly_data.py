# Databricks notebook source
# MAGIC %run /exdcfr/helper/de_utils.py

# COMMAND ----------

from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import calendar
from pyspark.sql.functions import col, concat, lit
import configparser
from pytz import timezone
from pyspark.sql.functions import upper

dbutils.widgets.text("conf-file","","")
dbutils.widgets.text("date-to-process","","")
dbutils.widgets.text("frequency","","")
dbutils.widgets.text("dry_run","N","") 
dbutils.widgets.text("country","","")

def getConfigDetails(path):
    config = configparser.ConfigParser()
    config.read(path)
    print(path)
    return config

confFile=dbutils.widgets.get("conf-file")
config = getConfigDetails(confFile)
config.sections()

inputDate=dbutils.widgets.get("date-to-process")
frequency=dbutils.widgets.get("frequency")
var_dryRun=dbutils.widgets.get("dry_run")
var_country=dbutils.widgets.get("country")


# COMMAND ----------

def validateInputDate():
  if inputDate != "":
    try:
      datetime.strptime(inputDate, "%Y-%m-%d")
    except:
      print("Incorrect date-to-process input. Format should be YYYY-MM-DD")
  else:
    raise Exception("date-to-prcess is empty")

# COMMAND ----------

def getDataRange(dt):

  if frequency.upper() == 'MONTHLY':
    dt1 = date.fromisoformat(dt).replace(day=1)
    dt2 = ((date.fromisoformat(dt) +  relativedelta(months = 1)).replace(day=1)) - timedelta(days=1)
  else:
    dt1 = date.fromisoformat(dt).replace(day=1)
    dt2 = date.fromisoformat(dt)
    
  dt_list = dict.fromkeys([dt1.strftime("%Y%m"), dt2.strftime("%Y%m")])
  dtmo = ",".join(dt_list)
    
  return dt1.strftime("%Y-%m-%d"),dt2.strftime("%Y-%m-%d"),dtmo

# COMMAND ----------

def fn_country_filter(df,country):
  
  if var_country.strip() != '' and var_country.upper() != 'ALL':
    df = df.filter(upper(col("country")) == var_country.upper())
    
  return df

# COMMAND ----------

def execute_sql(sql):
  return spark.sql(sql).rdd.first()

# COMMAND ----------

print("Inventory Prep Source:")

validateInputDate()
print("Country: " + var_country)
pdtfrom,pdtto,pdtmo = getDataRange(inputDate)
print("Process date range: {0} - {1}".format(pdtfrom,pdtto))

invPrep = config['ia_inventory_prep']
print("soPrep : {0}".format(invPrep['target_path']))
inv_prep = spark.read.format(invPrep['file_format']).load(invPrep['target_path']).filter(col("date").between(lit(pdtfrom),lit(pdtto)))
inv_prep = fn_country_filter(inv_prep ,var_country)
inv_prep.createOrReplaceTempView("inv_prep")

soPrep = config['ia_sellout_prep']
print("soPrep : {0}".format(soPrep['target_path']))
sell_prep = spark.read.format(soPrep['file_format']).load(soPrep['target_path']).filter(col("date").between(lit(pdtfrom),lit(pdtto)))
sell_prep = fn_country_filter(sell_prep ,var_country)
sell_prep.createOrReplaceTempView("sell_prep")

# COMMAND ----------

inv_groupBy = spark.sql("SELECT CAST('{0}' AS TIMESTAMP) as keymonth \
                                ,country, distributor, distributor_branch, sub_sector, case_gtin \
                                ,min(product_name) as product_name \
                                ,SUM(uom_value) as uom_value \
                                ,SUM(niv) as niv \
                                ,SUM(su) as su \
                                ,SUM(cs) as cs \
                                ,SUM(it) as it \
                                ,from_utc_timestamp(from_unixtime(unix_timestamp()), \"UTC+8\") as process_date \
                                ,SUM(uom_value_usd) as uom_value_usd \
                                ,SUM(niv_usd) as niv_usd \
                                ,REPLACE(SUBSTRING('{0}',0,7),'-','') as month \
                                 FROM inv_prep mt \
                            WHERE case_gtin IS NOT NULL \
                                 GROUP BY country, distributor, distributor_branch, sub_sector, case_gtin".format(pdtfrom))

inv_groupBy.createOrReplaceTempView("inv_groupBy")


# COMMAND ----------

so_groupBy = spark.sql("SELECT CAST('{0}' AS TIMESTAMP) as keymonth \
                                ,country, distributor, distributor_branch, sub_sector, case_gtin \
                                ,min(product_name) as product_name \
                                ,SUM(uom_value) as uom_value \
                                ,SUM(niv) as niv \
                                ,SUM(su) as su \
                                ,SUM(cs) as cs \
                                ,SUM(it) as it \
                                ,SUM(giv) as giv \
                                ,from_utc_timestamp(from_unixtime(unix_timestamp()), \"UTC+8\") as process_date \
                                ,SUM(uom_value_usd) as uom_value_usd \
                                ,SUM(niv_usd) as niv_usd \
                                ,REPLACE(SUBSTRING('{0}',0,7),'-','') as month \
                                 FROM sell_prep mt \
                            WHERE case_gtin IS NOT NULL \
                                 GROUP BY country, distributor, distributor_branch, sub_sector, case_gtin".format(pdtfrom))

so_groupBy.createOrReplaceTempView("so_groupBy")


# COMMAND ----------

if var_dryRun == 'Y':
  print("DRY RUN only")
  
  print("Displaying data of inv_groupBy dataframe")
  display(inv_groupBy)
  
  print("Displaying data of so_groupBy dataframe")
  display(so_groupBy)

else:
  
  inv_country_list = execute_sql('select concat_ws(",", collect_list(distinct concat("\'",country,"\'"))) from {0}'.format('inv_groupBy'))[0]
  invOutConf = config['ia_abc_monthly_data_inventory']
  inv_partition_fields = invOutConf['partition_col'].split(",")
  print("Partition fields to be used: {0}".format(inv_partition_fields))
  invOpt = {"replaceWhere":"{0} = '{1}' AND {2} IN ({3})".format(inv_partition_fields[0],pdtto[0:7].replace('-',''),inv_partition_fields[1],inv_country_list)}
  print(invOpt)
  print("Writing data to {0}.. \n".format(invOutConf['target_path']))
  writeDataFrame(inv_groupBy, invOutConf['file_format'], "overwrite", partitions=inv_partition_fields, path=invOutConf['target_path'],**invOpt)
  
  so_country_list = execute_sql('select concat_ws(",", collect_list(distinct concat("\'",country,"\'"))) from {0}'.format('so_groupBy'))[0]
  soOutConf = config['ia_abc_monthly_data_sellout']
  so_partition_fields = soOutConf['partition_col'].split(",")
  print("Partition fields to be used: {0}".format(so_partition_fields))
  soOpt = {"replaceWhere":"{0} = '{1}' AND {2} IN ({3})".format(so_partition_fields[0],pdtto[0:7].replace('-',''),so_partition_fields[1],so_country_list)}
  print(soOpt)
  print("Writing data to {0}.. \n".format(soOutConf['target_path']))
  writeDataFrame(so_groupBy, soOutConf['file_format'], "overwrite", partitions=so_partition_fields, path=soOutConf['target_path'],**soOpt)

print("PROCESS COMPLETED!")

