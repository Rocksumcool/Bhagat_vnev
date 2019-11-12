from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HousePriceSolution").master("local[*]").getOrCreate()

@udf("double")
def doubleDefault(col):
    if (col == -9.99999999999999E14):
        return None
    else:
        return col

@udf("integer")
def intDefault(col):
    if (col == -2147483648):
        return None
    else:
        return col

Fac = spark.sql("SELECT Facpk, Facname, Facfname, Faclname, Facadd1, Facadd2, Faccity, Facdirections, Facstate, Faccntry, Faczip , Facemail, Facphn1, Facphn2, Facfax, Facurl, Facreqpersonnel,Facnote, Facinternalind, Facreqappr,Facapprfk, Faclstupd, Facusrname, Faclck, Factimestamp, Factimezonefk , Facbuilding,Faccountryfk, Facregioncd, Facshippingadd1, Facshippingadd2, Facshippingcity, Facshippingstate, Facshippingzip from mylearning_1720.Fac")
InszFac=Fac.select(Fac.Facpk, Fac.Facname, Fac.Facfname, Fac.Faclname, Fac.Facadd1, Fac.Facadd2, Fac.Faccity, Fac.Facdirections, Fac.Facstate, Fac.Faccntry, Fac.Faczip , Fac.Facemail, Fac.Facphn1, Fac.Facphn2, Fac.Facfax, Fac.Facurl, Fac.Facreqpersonnel,intDefault(Fac.Facreqappr).alias("Facreqappr"),Fac.Facinternalind,intDefault(Fac.Facapprfk).alias("Facapprfk"), Fac.Faclstupd, Fac.Facusrname, Fac.Faclck,Fac.Factimestamp,intDefault(Fac.Factimezonefk).alias("Factimezonefk"),Fac.Facbuilding,intDefault(Fac.Faccountryfk).alias("Faccountryfk"),Fac.Facregioncd, Fac.Facshippingadd1, Fac.Facshippingadd2, Fac.Facshippingcity, Fac.Facshippingstate, Fac.Facshippingzip,F.regexp_replace(Fac.Facnote, '[\r\n]','').alias("facnote"))

#Registration Track
windowRegistrationTrack = Window.partitionBy("regtrack_pk").orderBy("LstUpd")


RegTr = spark.sql("SELECT ,Row_number() OVER(partition BY t1.regtrack_pk ORDER BY t1.LstUpd DESC) AS row_num FROM myLearning_1720.TBL_TMX_RegistrationTrack t1")

RegTr  = spark.sql("RegTrack_PK,RegFK,TrackFK,EnrollerFK,RegXmlFK,Status,RegDt,CnclDt,CnfmNo,Note,ChrgdOrgFK,PayTermFK,ReimbInd,PaidDt,BilledDt,TrvlCost,OthrCost,ActualCost,ReimbAmt,CurrencyFK,WLNotifDt,LstUpd,SWLReserveExpiryDt,UsrName,Lck,WLOverrideDt from myLearning_1720.TBL_TMX_RegistrationTrack")

