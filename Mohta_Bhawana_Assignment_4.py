from __future__ import print_function 
import findspark
findspark.init()
# create a SparkSession object
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("white-house-log").getOrCreate()    
import sys
#  CREATE a new RDD[String]
input_path = sys.argv[1]
# df is a DataFrame representing your input
df = spark.read.option("header","true")\
.option("delimiter","\t")\
.option("inferSchema","true")\
.option("sep", ",")\
.csv("whitehouse_waves-2016_12.csv")
# Lower case columns in dataframe
for col in df.columns:
    df = df.withColumn(col, F.lower(F.col(col)))
df1=df.select("NAMELAST", "NAMEFIRST","visitee_namelast", "visitee_namefirst")
df4=df1.count()
# Drop records if visitee_namelast or NAMELAST is null/empty
df2=df1.na.drop(subset=["NAMELAST","visitee_namelast"])
#count dropped records
df3=df2.count()
print("The Number of records dropped",df4-df3)
# count frequency of top 10 visitor
df5=df2.groupby("NAMELAST", "NAMEFIRST").count()
df6=df5.groupby ('NAMELAST','NAMEFIRST').sum()
df7=df6.orderBy(desc("sum(count)"))
print("The 10 most frequent visitors to the White House")
df7.show(10, truncate=False)
# count frequency of top 10 visited people
df8=df2.groupby("visitee_namelast", "visitee_namefirst").count()
df9=df8.groupby ("visitee_namelast", "visitee_namefirst").sum()
df10=df9.orderBy(desc("sum(count)"))
print("The 10 most frequently visited people in the White House.")
df10.show(10, truncate=False)
# count frequency of top 10 visitor-visitee combinations
df11=df2.groupby("NAMELAST", "NAMEFIRST","visitee_namelast", "visitee_namefirst").count()
df12=df11.groupby ("NAMELAST", "NAMEFIRST","visitee_namelast", "visitee_namefirst").sum()
df13=df12.orderBy(desc("sum(count)"))
print("The 10 most frequent visitor-visitee combinations.")
df13.show(10, truncate=False)



