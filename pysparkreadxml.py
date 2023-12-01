from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

#if __name__ == "__main__":
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Test") \
    .config("spark.jars", "C:\py\spark-xml_2.12-0.17.0.jar") \
    .getOrCreate() \

print("Spark Session Created")

def unpackDf(df: DataFrame,colNameOne,fieldNameXMLOne,colNameTwo,fieldNameXMLTwo):
    print("hello")
    df = df \
    .withColumn(
    colNameOne,
    F.explode_outer(F.col(fieldNameXMLOne)),
    ) \
    .withColumn(
    colNameTwo,
    F.explode_outer(F.col(fieldNameXMLTwo)),
    )
    return df

df = spark.read.format("com.databricks.spark.xml") \
.options(rowTag="customer") \
.load("C:\py\sample.xml")
df.printSchema()
df = unpackDf(df,"ColNameState","address.state","ColNamePinCode","address.zip")

df.show(50,truncate=False)
