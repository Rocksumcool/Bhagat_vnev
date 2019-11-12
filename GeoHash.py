import pygeohash as pg

a=pg.encode(latitude="",longitude="",precision=12)

DF.withColumn("GeohashColumn", pg.encode(latitude="DF.ColumnNameLAt",longitude="DF.ColumnNameLONG",precision=12))

F.when (