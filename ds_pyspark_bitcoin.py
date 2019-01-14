# -*- coding:UTF-8 -*-
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from graphframes import *
import curses
import sys
import datetime

print("############################Start############################")
print("Data Loading....")
spark = SparkSession.builder.appName('ds_bitcoin').config("spark.jars.packages", "graphframes:graphframes:0.6.0-spark2.3-s_2.11").getOrCreate()
df = spark.read.load(["gs://mining-table/MiningTable.csv"],format="csv", delimiter=",", header=True) #讀取要讀入的CSV
df.show() #顯示CSV匯入的資料
df.createOrReplaceTempView("table1")

df2 = spark.read.load(["gs://ds_bitcoin/ds-transactions-*.csv"],format="csv", delimiter=",", header=True)
df2.show()
#df2.count()
df2.createOrReplaceTempView("transactiontable_origin")

print("Data Range: 2017-10-01 to 2018-09-10")
keyin_1 = input('Key in start-date(YYYY-MM-DD):')
keyin_2 = input("Key in end-date(YYYY-MM-DD):")
print("Processing....")

df2_1 = spark.sql("SELECT * FROM transactiontable_origin WHERE DTime >= " + "'" + keyin_1 + "00:00:00'" + " AND DTime <= " + "'" + keyin_2 + " 23:59:59'")
#df2_1 = df2.select("DTime", "output_key", "input_key").filter("DTime >= '" + keyin_1 + " 00:00:00' AND DTime <= '" + keyin_2 + " 23:59:59'").sort(asc("DTime"))
df2_1.show()
df2_1.count()
df2_1.createOrReplaceTempView("transactiontable")

#df3 = spark.sql("SELECT output_key FROM table1 GROUP BY output_key")
df3 = spark.sql("SELECT output_key FROM (SELECT * FROM table1 WHERE DAT >= '" + keyin_1 + "' AND DAT <= '" + keyin_2 + "') GROUP BY output_key")
df3.show()
df3.count() #Max:3220
df3.createOrReplaceTempView("vertextable")
df3_1 = df3.selectExpr("output_key as id") #set output_key label to id which graphfame required
#df3 = spark.read.load(["gs://ds_bitcoin/ds-transactions-*.csv"],format="csv", delimiter=",", header=True)
#df3.show()
#df3.createOrReplaceTempView("transactiontable")
df4 = spark.sql("SELECT input_key, output_key, COUNT(input_key) AS RelationCount FROM (SELECT * FROM ( SELECT T1.DTime AS DTime, T1.input_key AS input_key, T1.output_key AS output_key FROM (SELECT T1.DTime AS DTime, T1.input_key AS input_key, T1.output_key AS output_key FROM transactiontable AS T1 JOIN vertextable AS T2 ON T1.input_key = T2.output_key) AS T1 JOIN vertextable AS T2 ON T1.output_key = T2.output_key) WHERE input_key != output_key) GROUP BY input_key, output_key ORDER BY RelationCount DESC")
#df4.collect()
df4.show()
df4.count()
df4.createOrReplaceTempView("edgetable")
df4_1 = df4.selectExpr("input_key as src", "output_key as dst", "RelationCount as relationship")

g = GraphFrame(df3_1, df4_1) # df3:vertex, df4:edge
g.inDegrees.show()
g.inDegrees.orderBy(g.inDegrees.inDegree.desc()).show()
#g.inDegrees.orderBy(g.inDegrees.inDegree.desc()).collect()
inDegreeCollect = g.inDegrees.orderBy(g.inDegrees.inDegree.desc()).collect()
#g.edges.filter("RelationCount = '1410'").count()
#g.vertices.groupBy().max("RelationCount").show()

#outDegree
g.outDegrees.show()
g.outDegrees.orderBy(g.outDegrees.outDegree.desc()).show()
outDegreeCollect = g.outDegrees.orderBy(g.outDegrees.outDegree.desc()).collect()

while True:
    print("Keyin Pagerank Parameter:")
    print("resetProbability & tol Between 0 - 1")
    print("Keyin 'Q' to Quit!")
    while True:
        pr_keyin_1 = input("Key in reserProbability:")
        if pr_keyin_1 == 'Q':
            break
        pr_keyin_2 = input("Key in tol:")
        if pr_keyin_2 == 'Q':
            break
        if (float(pr_keyin_1) < 0 or float(pr_keyin_1) > 1 or  float(pr_keyin_2) < 0 or float(pr_keyin_2) > 1):
            print("Out of parameter Range")
        else:
            print("resetProbability: " + pr_keyin_1)
            print("tol: " + pr_keyin_2)
            print("Processing...")
            break
    if pr_keyin_1 == 'Q' or pr_keyin_2 == 'Q':
        print("Quit!")
        break
    else:
        results_2 = g.pageRank(resetProbability= float(pr_keyin_1), tol= float(pr_keyin_2))
        results_2.vertices.select("id","pagerank").show()
        qq_2 = results_2.vertices.select("id","pagerank")
        qq_2.orderBy(desc("pagerank")).show()
