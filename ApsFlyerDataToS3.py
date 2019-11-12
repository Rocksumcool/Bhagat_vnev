#pip install requests
from requests import get  # to make GET request
from pyspark.sql import SparkSession
spark =SparkSession.builder.appName("Get Data from Apps Flyer").getOrCreate()
url ='https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2018-financial-year-provisional/Download-data/annual-enterprise-survey-2018-financial-year-provisional-csv.csv'
tempFile = "./temp.txt"
def download(url, tempFile):
    # open in binary mode
    with open(tempFile, "wb") as file:
        # get request
        response = get(url)
        # write to file
        file.write(response.content)
download(url,tempFile)

Dataframe=spark.read.text(tempFile)
Dataframe.write.mode("overwrite").format('parquet').save("AWS PATH")




