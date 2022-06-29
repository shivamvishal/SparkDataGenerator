import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("SimpleApp").master("local[4]").getOrCreate()
    column_count = 10
    data_rows = 1000 * 1000

    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']

    df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                partitions=4)
               .withIdOutput()
               .withColumn("month", StringType(), values=months, random=True)
               .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
               .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
               .withColumn("code3", StringType(), values=['a', 'b', 'c'])
               .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
               .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[7, 2, 1])
               .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                           numColumns=column_count)
               )

    df = df_spec.build()
    df.show()
    num_rows = df.count()
    df.write.partitionBy("month").parquet("./testfile")

    spark.stop()
