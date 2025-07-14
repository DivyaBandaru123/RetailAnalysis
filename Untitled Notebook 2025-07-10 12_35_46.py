# Databricks notebook source
df = spark.read.csv("/FileStore/tables/store_csv.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

dbutils.widgets.dropdown("time_period", 'Weekly', ['Weekly','Monthly'])

# COMMAND ----------

# from datetime import date,timedelta,datetime
# from pyspark.sql.functions import * 

# time_period= dbutils.widgets.get("time_period")
# print(time_period)
# today = date.today()

# if time_period=='Weekly':
#     start_date=today-timedelta(days=today.weekday(),weeks=1)-timedelta(days=1)
#     end_date=start_date+timedelta(days=6)

# else:
#     first=today.replace(day=1)
#     end_date=first-timedelta(days=1)
#     start_date=first-timedelta(days=end_date.day)

# print(start_date,end_date)

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample

# COMMAND ----------

# DBTITLE 1,Total no.of Customers
# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT Customer_id) FROM sample

# COMMAND ----------

# display(spark.sql(f"""SELECT COUNT(DISTINCT Customer_id) FROM sample where order_date between '{start_date}' and '{end_date}'"""))

#Here answer displayed as 0 beacuse we have taken the last year's dataset. Other wise this code is best to run weekly,monthly based data.

# COMMAND ----------

# DBTITLE 1,Total customers has done the order last week/month based on the dropdown
# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT Customer_id) FROM sample where order_date between '2024-06-01' and '2024-06-31'

# COMMAND ----------

# DBTITLE 1,Total number of orders
# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT Order_id) FROM sample

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT Order_id) FROM sample where order_date between '2024-06-01' and '2024-06-31'
# MAGIC
# MAGIC -- #This is manual above spark code with start_date and end_date is automated one where stackholders can see the weekly ,monthly basis based on their requirements.

# COMMAND ----------

# DBTITLE 1,Total Sales and Proft
# MAGIC %sql
# MAGIC SELECT sum(sales), sum(Profit) from sample
# MAGIC

# COMMAND ----------

# DBTITLE 1,Top sales by country
# MAGIC %sql
# MAGIC SELECT sum(sales), country from sample
# MAGIC GROUP BY 2

# COMMAND ----------

# DBTITLE 1,Most profitable  region and country
# MAGIC %sql
# MAGIC SELECT sum(sales), country,region from sample
# MAGIC GROUP BY 2,3

# COMMAND ----------

# DBTITLE 1,Top sales category products
# MAGIC %sql
# MAGIC SELECT sum(sales), category from sample
# MAGIC GROUP BY 2
# MAGIC Order by 1 DESC

# COMMAND ----------

# DBTITLE 1,Top 10  sales sub category products
# MAGIC %sql
# MAGIC SELECT sum(sales),sub_category from sample
# MAGIC GROUP BY 2
# MAGIC Order by 1 DESC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Most ordered quantity product
# MAGIC %sql
# MAGIC SELECT sum(quantity),product_name FROM sample
# MAGIC GROUP BY 2
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

# DBTITLE 1,Top customers based on sales and city
# MAGIC %sql
# MAGIC SELECT sum(sales),customer_name,city FROM sample
# MAGIC GROUP BY 2,3
# MAGIC ORDER BY 1 DESC