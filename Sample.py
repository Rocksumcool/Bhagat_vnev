from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("HousePriceSolution").master("local[*]").getOrCreate()
    SalesReport=[("zF","Shirt","2019-11-10 00:00:00",200,8),("zF","Tshirt","2019-05-09 00:00:00",300,1),("zF","Shirt","2019-06-08 00:00:00",100,7),("zC","Tshirt","2019-07-07 00:00:00",400,1),("zP","ShortShirt","2018-08-06 00:00:00",600,2),("zP","ShortShirt","2018-09-06 00:00:00",300,2),("zP","ShortShirt","2018-10-06 00:00:00",200,2),("zU","ShortShirt","2018-09-05 00:00:00",800,10),("zF","Tshirt","2018-10-04 00:00:00",600,9)]
    salesReportDF=spark.createDataFrame(SalesReport,("store_id","item_id","date","price","Quantntity"))
    print("The Sales Table :")
    salesReportDF.show()
    lookup=[("Shirt",300),("Tshirt",600),("ShortShirt",200)]
    lookupDF=spark.createDataFrame(lookup,("item_id","costprice"))
    print("The lookup Table :")
    lookupDF.show()
    lookup.select(lookup.soreid, F.count('store'))
    IntDF=salesReportDF.join(broadcast(lookupDF), salesReportDF.item_id == lookupDF.item_id).select(salesReportDF.store_id,salesReportDF.item_id,F.date_format(salesReportDF.date,'yyyy-MM').alias("date"),salesReportDF.price,salesReportDF.Quantntity,lookupDF.costprice)
    TotalSPCP=IntDF.withColumn("SellingPrice",col('price')*col('Quantntity')).withColumn("CostPrice",col('costprice')*col('Quantntity'))
    DFInsz=TotalSPCP.select(TotalSPCP.store_id,TotalSPCP.item_id,TotalSPCP.CostPrice,TotalSPCP.SellingPrice,TotalSPCP.date)
    ProfitDF=DFInsz.withColumn("ProfitPercentage",F.bround((((col('SellingPrice')-col('CostPrice'))/col('CostPrice'))*100),2)).orderBy(DFInsz.date)
    ProfitMetrixWindow =Window.partitionBy('store_id','item_id').orderBy(ProfitDF['date'])
    Sales_Report=ProfitDF.select(ProfitDF.store_id,ProfitDF.item_id,ProfitDF.date,ProfitDF.ProfitPercentage).withColumn("%ProfitGrowth",F.when(F.min(ProfitDF.date).over(ProfitMetrixWindow)==ProfitDF.date,0).otherwise(ProfitDF.ProfitPercentage - F.lag(ProfitDF.ProfitPercentage,1,0).over(ProfitMetrixWindow)))
    print("The Final Reporting Table : ")
    Sales_Report.show()